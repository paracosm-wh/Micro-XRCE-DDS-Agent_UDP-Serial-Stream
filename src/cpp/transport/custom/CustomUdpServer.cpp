#include <uxr/agent/transport/custom/CustomUdpServer.hpp>
#include <uxr/agent/utils/Conversion.hpp>
#include <uxr/agent/logger/Logger.hpp>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <cerrno>

namespace eprosima {
namespace uxr {

CustomUdpServer::CustomUdpServer(
        uint16_t port,
        Middleware::Kind middleware_kind)
    : Server<CustomEndPoint>{middleware_kind}
    , port_{port}
    , poll_fd_{-1, 0, 0}
    , buffer_{0}
    , framing_io_(
          0x00, // Agent 在串口协议层的逻辑地址，通常为 0
          std::bind(&CustomUdpServer::write_data, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
          std::bind(&CustomUdpServer::read_data, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4))
    , input_buffer_pos_(0)
{
    // [优化] 预分配内存，避免 vector 频繁扩容带来的性能损耗
    input_buffer_.reserve(SERVER_BUFFER_SIZE);
    tx_buffer_.reserve(SERVER_BUFFER_SIZE);
}

CustomUdpServer::~CustomUdpServer()
{
    try
    {
        stop();
    }
    catch (std::exception& e)
    {
        UXR_AGENT_LOG_CRITICAL(
            UXR_DECORATE_RED("error stopping server"),
            "exception: {}",
            e.what());
    }
}

bool CustomUdpServer::init()
{
    bool rv = false;
    poll_fd_.fd = socket(PF_INET, SOCK_DGRAM, 0);

    if (-1 != poll_fd_.fd)
    {
        struct sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port_);
        address.sin_addr.s_addr = INADDR_ANY;
        memset(address.sin_zero, '\0', sizeof(address.sin_zero));

        if (-1 != bind(poll_fd_.fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)))
        {
            poll_fd_.events = POLLIN;
            rv = true;
            UXR_AGENT_LOG_INFO(
                UXR_DECORATE_GREEN("running Custom UDP..."),
                "port: {}",
                port_);
        }
        else
        {
            UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("bind error"), "port: {}, errno: {}", port_, errno);
        }
    }
    else
    {
        UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("socket error"), "port: {}, errno: {}", port_, errno);
    }
    return rv;
}

bool CustomUdpServer::fini()
{
    if (-1 == poll_fd_.fd) return true;

    bool rv = false;
    if (0 == ::close(poll_fd_.fd))
    {
        poll_fd_.fd = -1;
        rv = true;
        UXR_AGENT_LOG_INFO(UXR_DECORATE_GREEN("server stopped"), "port: {}", port_);
    }
    else
    {
        UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("socket error"), "port: {}, errno: {}", port_, errno);
    }
    return rv;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int timeout,
        TransportRc& transport_rc)
{
    bool rv = false;
    uint8_t framing_src_addr = 0x00; // 对方在 Serial 协议层中的逻辑地址
    ssize_t bytes_read = 0;

    // [注释] framing_io_.read_framed_msg 会循环调用我们实现的 read_data
    // 直到解包出一个完整的 Serial 帧或者超时
    do
    {
        bytes_read = framing_io_.read_framed_msg(
            buffer_, SERVER_BUFFER_SIZE, framing_src_addr, timeout, transport_rc);
    }
    while ((0 == bytes_read) && (0 < timeout));

    if (0 < bytes_read)
    {
        input_packet.message.reset(new InputMessage(buffer_, static_cast<size_t>(bytes_read)));
        
        // [注释] 构建 CustomEndPoint
        // 我们组合了 UDP 层的物理地址 (IP/Port) 和 Serial 层的逻辑地址 (framing_addr)
        CustomEndPoint custom_endpoint;
        custom_endpoint.add_member<std::string>("address");
        custom_endpoint.add_member<uint16_t>("port");
        custom_endpoint.add_member<uint8_t>("framing_addr");

        custom_endpoint.set_member_value("address", std::string(inet_ntoa(source_to_map_.sin_addr)));
        custom_endpoint.set_member_value("port", static_cast<uint16_t>(ntohs(source_to_map_.sin_port)));
        custom_endpoint.set_member_value("framing_addr", framing_src_addr);

        input_packet.source = custom_endpoint;
        rv = true;

        uint32_t raw_client_key;
        if (get_client_key(input_packet.source, raw_client_key))
        {
            UXR_AGENT_LOG_MESSAGE(
                UXR_DECORATE_YELLOW("[==>> C-UDP <<==]"),
                raw_client_key,
                input_packet.message->get_buf(),
                input_packet.message->get_len());
        }
    }
    return rv;
}

bool CustomUdpServer::send_message(
        OutputPacket<CustomEndPoint> output_packet,
        TransportRc& transport_rc)
{
    bool rv = false;
    
    // [优化] 1. 准备目标 UDP 地址结构体
    struct sockaddr_in dest_addr{};
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    
    // 从 CustomEndPoint 获取 IP 和 Port
    dest_addr.sin_port = htons(output_packet.destination.get_member<uint16_t>("port"));
    std::string ip_str = output_packet.destination.get_member<std::string>("address");
    if (inet_aton(ip_str.c_str(), &dest_addr.sin_addr) == 0)
    {
        // IP 解析失败
        transport_rc = TransportRc::server_error;
        return false;
    }

    uint8_t dest_framing_addr = output_packet.destination.get_member<uint8_t>("framing_addr");

    // [优化] 2. 清空发送缓冲区，准备聚合数据
    tx_buffer_.clear();

    // [优化] 3. 调用 FramingIO
    // 此时 write_framed_msg 会多次调用下方的 write_data，
    // 但 write_data 现在只负责将数据 append 到 tx_buffer_，不进行实际发送。
    ssize_t bytes_framing_generated = framing_io_.write_framed_msg(
                output_packet.message->get_buf(),
                output_packet.message->get_len(),
                dest_framing_addr,
                transport_rc);

    // [优化] 4. 真正执行 UDP 发送
    // 只有当 framing 生成成功且 buffer 有数据时才发送
    if ((bytes_framing_generated > 0) && !tx_buffer_.empty())
    {
        ssize_t bytes_sent = sendto(
            poll_fd_.fd,
            tx_buffer_.data(),
            tx_buffer_.size(),
            0,
            reinterpret_cast<struct sockaddr*>(&dest_addr),
            sizeof(dest_addr));

        if (bytes_sent > 0 && static_cast<size_t>(bytes_sent) == tx_buffer_.size())
        {
            rv = true; // 发送成功
            
            // Log 逻辑保持不变
            uint32_t raw_client_key;
            if (get_client_key(output_packet.destination, raw_client_key))
            {
                UXR_AGENT_LOG_MESSAGE(
                    UXR_DECORATE_YELLOW("[** <<C-UDP>> **]"),
                    raw_client_key,
                    output_packet.message->get_buf(),
                    output_packet.message->get_len());
            }
        }
        else
        {
            transport_rc = TransportRc::server_error;
        }
    }
    else if (bytes_framing_generated <= 0)
    {
        // Framing 生成失败，transport_rc 已经在 write_framed_msg 中被设置
    }

    return rv;
}

bool CustomUdpServer::handle_error(TransportRc)
{
    return fini() && init();
}

// [修改] 只负责数据聚合，不负责发送
ssize_t CustomUdpServer::write_data(
        uint8_t* buf,
        size_t len,
        TransportRc& /*transport_rc*/)
{
    // 将数据追加到 tx_buffer_ 末尾
    tx_buffer_.insert(tx_buffer_.end(), buf, buf + len);
    return len;
}

ssize_t CustomUdpServer::read_data(
        uint8_t* buf,
        size_t len,
        int timeout,
        TransportRc& transport_rc)
{
    // 1. 如果输入缓冲区有遗留数据，直接返回遗留数据给 FramingIO
    if (input_buffer_pos_ < input_buffer_.size())
    {
        size_t available = input_buffer_.size() - input_buffer_pos_;
        size_t to_copy = (available < len) ? available : len;
        memcpy(buf, input_buffer_.data() + input_buffer_pos_, to_copy);
        input_buffer_pos_ += to_copy;
        return to_copy;
    }

    // 2. 缓冲区为空，尝试从 UDP 读取新的包
    input_buffer_.clear();
    input_buffer_pos_ = 0;

    int poll_rv = poll(&poll_fd_, 1, timeout);
    if(poll_fd_.revents & (POLLERR+POLLHUP))
    {
        transport_rc = TransportRc::server_error;
        return -1;
    }
    else if (0 < poll_rv)
    {
        // 临时 buffer 用于接收 UDP 包
        uint8_t temp_buf[SERVER_BUFFER_SIZE];
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        ssize_t bytes_recvd = recvfrom(
            poll_fd_.fd,
            temp_buf,
            sizeof(temp_buf),
            0,
            reinterpret_cast<struct sockaddr*>(&client_addr),
            &addr_len);

        if (bytes_recvd > 0)
        {
            // 将接收到的数据存入成员变量 input_buffer_
            input_buffer_.assign(temp_buf, temp_buf + bytes_recvd);
            
            // [重要] 更新源地址，供 recv_message 创建 Endpoint 使用
            source_to_map_ = client_addr; 
            
            // 递归调用自己，复用步骤1的逻辑来拷贝数据
            return read_data(buf, len, 0, transport_rc);
        }
        else if (bytes_recvd < 0)
        {
            transport_rc = TransportRc::server_error;
            return -1;
        }
    }
    else
    {
        // poll 返回 0，超时
        transport_rc = (poll_rv == 0) ? TransportRc::timeout_error : TransportRc::server_error;
    }
    return 0;
}

} // namespace uxr
} // namespace eprosima
