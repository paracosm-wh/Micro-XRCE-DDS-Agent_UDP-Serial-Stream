#include <uxr/agent/transport/custom/CustomUdpServer.hpp>
#include <uxr/agent/utils/Conversion.hpp>
#include <uxr/agent/logger/Logger.hpp>

#include <asio.hpp>
#include <poll.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <array>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>

// [修复 1] 添加 iomanip 头文件以支持 setw 和 setfill
#include <iomanip> 
#include <sstream>

namespace eprosima {
namespace uxr {

// --- 配置常量 --------------------------------
static constexpr uint8_t HDLC_FLAG = 0x7E; 
static constexpr size_t MAX_CLIENT_BUFFER_SIZE = 16 * 1024; 
static constexpr size_t KEEP_TAIL_ON_RESYNC = 32; 
// ---------------------------------------------------------

// 辅助函数：打印 buffer 十六进制 (用于调试)
// [修复 1] 这里的 setw 和 setfill 现在可以正常工作了
static std::string hex_str(const uint8_t* data, size_t len) {
    std::stringstream ss;
    ss << std::hex;
    for(size_t i=0; i<len && i<16; ++i) {
        ss << std::setw(2) << std::setfill('0') << (int)data[i] << " ";
    }
    return ss.str();
}

namespace {
struct PendingFrame
{
    std::unique_ptr<InputMessage> msg;
    std::string address;
    uint16_t port;
    uint8_t framing_addr;
};
static std::deque<PendingFrame> pending_frames_;
static std::mutex pending_frames_mutex_;
} // anonymous

CustomUdpServer::CustomUdpServer(
        uint16_t port,
        Middleware::Kind middleware_kind)
    : Server<CustomEndPoint>(middleware_kind)
    , port_(port)
    , socket_(io_service_)
{
}

CustomUdpServer::~CustomUdpServer()
{
    if (socket_.is_open())
    {
        fini();
    }
}

bool CustomUdpServer::init()
{
    bool rv = false;
    try
    {
        asio::ip::udp::endpoint endpoint(asio::ip::udp::v4(), port_);
        socket_.open(endpoint.protocol());
        // 增大内核 Socket 缓冲区
        socket_.set_option(asio::socket_base::receive_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.set_option(asio::socket_base::send_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.bind(endpoint);
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("CustomUDP server started"),
            "port: {}",
            port_);
        rv = true;
    }
    catch (const std::exception& e)
    {
        UXR_AGENT_LOG_ERROR(
            UXR_DECORATE_RED("CustomUDP server error"),
            "port: {}, what: {}",
            port_,
            e.what());
    }
    return rv;
}

bool CustomUdpServer::fini()
{
    if (socket_.is_open())
    {
        socket_.close();
    }
    UXR_AGENT_LOG_INFO(
        UXR_DECORATE_GREEN("CustomUDP server stopped"),
        "port: {}",
        port_);
    return true;
}

/**
 * process_client_buffer (修复版)
 * 使用 vector 代替 array 以支持大包 (XML Topic定义)
 */
bool CustomUdpServer::process_client_buffer(
    ClientIO& client_io,
    const asio::ip::udp::endpoint& endpoint,
    InputPacket<CustomEndPoint>& input_packet,
    TransportRc& transport_rc)
{
    if (client_io.recv_buffer.empty()) {
        return false;
    }

    // 限制单客户端 buffer 大小
    if (client_io.recv_buffer.size() > MAX_CLIENT_BUFFER_SIZE) {
        size_t drop = client_io.recv_buffer.size() - MAX_CLIENT_BUFFER_SIZE;
        client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.begin() + drop);
        UXR_AGENT_LOG_WARN(UXR_DECORATE_YELLOW("Buffer Overflow"), "Dropped {} bytes", drop);
    }

    std::vector<std::unique_ptr<InputMessage>> parsed_messages;
    std::vector<uint8_t> parsed_framing_addrs;

    // 关键修改：使用足够大的动态 Buffer 来进行解帧，防止 XML 被截断
    std::vector<uint8_t> temp_msg_buffer(MAX_CLIENT_BUFFER_SIZE); 

    while (true)
    {
        client_io.read_pos = 0;
        
        auto read_lam = [&](uint8_t* buf, size_t len, int, TransportRc&) -> ssize_t
        {
            if (client_io.read_pos >= client_io.recv_buffer.size()) { return 0; }
            size_t bytes_to_copy = std::min(len, client_io.recv_buffer.size() - client_io.read_pos);
            std::memcpy(buf, client_io.recv_buffer.data() + client_io.read_pos, bytes_to_copy);
            client_io.read_pos += bytes_to_copy;
            return static_cast<ssize_t>(bytes_to_copy);
        };

        FramingIO framing_io(0x00, [](uint8_t*, size_t, TransportRc&){ return 0; }, read_lam);

        uint8_t remote_addr = 0;
        int framing_timeout = 1;

        ssize_t bytes_decoded = framing_io.read_framed_msg(
            temp_msg_buffer.data(), 
            temp_msg_buffer.size(), 
            remote_addr, 
            framing_timeout, 
            transport_rc);

        if (bytes_decoded > 0)
        {
            // 成功解析一帧
            auto imsg = std::unique_ptr<InputMessage>(new InputMessage(temp_msg_buffer.data(), static_cast<size_t>(bytes_decoded)));
            parsed_messages.push_back(std::move(imsg));
            parsed_framing_addrs.push_back(remote_addr);

            // 从 buffer 删除已解析数据
            client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.begin() + client_io.read_pos);

            if (client_io.recv_buffer.empty()) break;
        }
        else
        {
            // 解析失败：可能是数据不全，或者格式错误
            if (client_io.read_pos > 0) {
                // 读了部分数据但不够一帧，保留等待下次
                break; 
            }
            
            // read_pos == 0，说明无法识别头部，尝试 Resync
            auto it = std::find(client_io.recv_buffer.begin(), client_io.recv_buffer.end(), HDLC_FLAG);
            if (it != client_io.recv_buffer.end())
            {
                if (it != client_io.recv_buffer.begin()) {
                    size_t garbage = std::distance(client_io.recv_buffer.begin(), it);
                    client_io.recv_buffer.erase(client_io.recv_buffer.begin(), it);
                    // 找到了新头，重新尝试
                    continue; 
                }
                // 开头就是 FLAG 但没解出来，说明数据不够，跳出等待
                break;
            }
            else
            {
                // 无 FLAG，保留尾部
                if (client_io.recv_buffer.size() > KEEP_TAIL_ON_RESYNC) {
                    size_t keep = KEEP_TAIL_ON_RESYNC;
                    client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.end() - keep);
                }
                break;
            }
        }
    }

    if (parsed_messages.empty()) return false;

    // 取出第一帧返回
    auto &first_msg = parsed_messages.front();
    input_packet.message.reset(new InputMessage(first_msg->get_buf(), first_msg->get_len()));

    CustomEndPoint custom_endpoint;
    custom_endpoint.add_member<std::string>("address");
    custom_endpoint.add_member<uint16_t>("port");
    custom_endpoint.add_member<uint8_t>("framing_addr");
    
    custom_endpoint.set_member_value("address", endpoint.address().to_string());
    custom_endpoint.set_member_value("port", (uint16_t)endpoint.port());
    custom_endpoint.set_member_value("framing_addr", parsed_framing_addrs.front());
    
    input_packet.source = custom_endpoint;

    // 将剩余帧放入 Pending 队列
    if (parsed_messages.size() > 1) {
        std::lock_guard<std::mutex> lock(pending_frames_mutex_);
        for (size_t i = 1; i < parsed_messages.size(); ++i) {
            PendingFrame pf;
            pf.msg = std::move(parsed_messages[i]);
            pf.address = endpoint.address().to_string();
            pf.port = endpoint.port();
            pf.framing_addr = parsed_framing_addrs[i];
            pending_frames_.push_back(std::move(pf));
        }
    }

    uint32_t client_key = 0;
    if(get_client_key(input_packet.source, client_key)) {
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("Packet Ready"), 
            "ClientKey: 0x{:08X}, Len: {}", 
            client_key, input_packet.message->get_len());
    } else {
        // [修复 2] 宏调用修复：添加 "{}" 作为格式化字符串，把具体信息作为参数传入
        // 这样可以避免变参宏 __VA_ARGS__ 为空导致的编译错误
        UXR_AGENT_LOG_WARN(
            UXR_DECORATE_RED("Unknown Client"), 
            "{}", 
            "Could not get client key for packet"
        );
    }

    return true;
}

// ... recv_message, send_message 等函数的其余部分保持不变 ...

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int /*timeout*/,
        TransportRc& transport_rc)
{
    // 检查 Pending 队列
    {
        std::lock_guard<std::mutex> lock(pending_frames_mutex_);
        if (!pending_frames_.empty())
        {
            PendingFrame pf = std::move(pending_frames_.front());
            pending_frames_.pop_front();

            input_packet.message.reset(new InputMessage(pf.msg->get_buf(), pf.msg->get_len()));

            CustomEndPoint custom_endpoint;
            custom_endpoint.add_member<std::string>("address");
            custom_endpoint.add_member<uint16_t>("port");
            custom_endpoint.add_member<uint8_t>("framing_addr");
            custom_endpoint.set_member_value("address", pf.address);
            custom_endpoint.set_member_value("port", pf.port);
            custom_endpoint.set_member_value("framing_addr", pf.framing_addr);
            input_packet.source = custom_endpoint;

            transport_rc = TransportRc::ok;
            return true;
        }
    }

    while (true)
    {
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            for (auto& it : client_io_map_)
            {
                if (it.second->recv_buffer.empty()) continue;
                if (process_client_buffer(*it.second, it.first, input_packet, transport_rc))
                {
                    return true;
                }
            }
        }

        pollfd pfd{socket_.native_handle(), POLLIN, 0};
        int poll_rv = poll(&pfd, 1, 1000); 

        if (poll_rv > 0)
        {
            if (pfd.revents & POLLIN)
            {
                try
                {
                    std::array<uint8_t, SERVER_BUFFER_SIZE> udp_payload_buffer;
                    asio::ip::udp::endpoint remote_endpoint;
                    asio::error_code ec;
                    size_t bytes_recvd = socket_.receive_from(asio::buffer(udp_payload_buffer), remote_endpoint, 0, ec);

                    if (!ec && bytes_recvd > 0)
                    {
                        std::lock_guard<std::mutex> lock(clients_mutex_);
                        auto it = client_io_map_.find(remote_endpoint);
                        if (it == client_io_map_.end())
                        {
                            it = client_io_map_.emplace(remote_endpoint, std::unique_ptr<ClientIO>(new ClientIO())).first;
                        }
                        
                        it->second->recv_buffer.insert(
                            it->second->recv_buffer.end(),
                            udp_payload_buffer.begin(),
                            udp_payload_buffer.begin() + bytes_recvd);
                    }
                }
                catch (const std::exception& e)
                {
                    UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("recv_message error"), "what: {}", e.what());
                }
            }
        }
    }
    return false;
}

bool CustomUdpServer::send_message(
        OutputPacket<CustomEndPoint> output_packet,
        TransportRc& transport_rc)
{
    transport_rc = TransportRc::ok;
    try
    {
        asio::ip::udp::endpoint destination_endpoint(
            asio::ip::address::from_string(output_packet.destination.get_member<std::string>("address")),
            output_packet.destination.get_member<uint16_t>("port"));

        std::vector<uint8_t> framed_buffer;
        auto write_lam = [&](const uint8_t* buf, size_t len, TransportRc& rc) -> ssize_t
        {
            framed_buffer.insert(framed_buffer.end(), buf, buf + len);
            rc = TransportRc::ok;
            return ssize_t(len);
        };

        FramingIO framing_io(0x00, write_lam, [](uint8_t*, size_t, int, TransportRc&){ return 0; });
        uint8_t remote_framing_addr = output_packet.destination.get_member<uint8_t>("framing_addr");

        ssize_t bytes_written = framing_io.write_framed_msg(
            output_packet.message->get_buf(),
            output_packet.message->get_len(),
            remote_framing_addr,
            transport_rc);

        if (bytes_written > 0)
        {
            asio::error_code ec;
            socket_.send_to(asio::buffer(framed_buffer), destination_endpoint, 0, ec);
            if (ec) {
                 UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("send_message error"), "ec: {}", ec.message());
                 transport_rc = TransportRc::server_error;
                 return false;
            }
            return true;
        }
    }
    catch(const std::exception& e)
    {
        UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("send_message exception"), "what: {}", e.what());
        transport_rc = TransportRc::server_error;
    }
    return false;
}

bool CustomUdpServer::handle_error(TransportRc)
{
    return true;
}

} // namespace uxr
} // namespace eprosima
