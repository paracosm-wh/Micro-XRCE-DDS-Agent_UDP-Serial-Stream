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
#include <iomanip>
#include <sstream>

namespace eprosima {
namespace uxr {

// --- 配置常量 --------------------------------
static constexpr size_t MAX_CLIENT_BUFFER_SIZE = 16 * 1024; 
// ---------------------------------------------------------

// 内部 pending 队列
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
        // 增大内核缓冲区，防止高吞吐丢包
        socket_.set_option(asio::socket_base::receive_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.set_option(asio::socket_base::send_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.bind(endpoint);
        
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("CustomUDP (Raw Pass-through) server started"),
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
 * 核心接收函数 (纯透传模式 Raw Pass-through)
 * 不做任何 7E/Length/CRC 解析，直接把 UDP Payload 交给 Agent
 */
bool CustomUdpServer::process_client_buffer(
    ClientIO& client_io,
    const asio::ip::udp::endpoint& endpoint,
    InputPacket<CustomEndPoint>& input_packet,
    TransportRc& transport_rc)
{
    // 1. 空检查
    if (client_io.recv_buffer.empty()) {
        return false;
    }

    size_t len = client_io.recv_buffer.size();
    uint8_t* buf = client_io.recv_buffer.data();

    // 2. 直接构造 InputMessage (透传)
    // 假设 UDP 包边界就是消息边界 (UDP preserves message boundaries)
    input_packet.message.reset(new InputMessage(buf, len));

    // 3. 设置源端点信息
    CustomEndPoint custom_endpoint;
    custom_endpoint.add_member<std::string>("address");
    custom_endpoint.add_member<uint16_t>("port");
    custom_endpoint.add_member<uint8_t>("framing_addr"); // 保留成员以防上层需要，但设为固定值
    
    custom_endpoint.set_member_value("address", endpoint.address().to_string());
    custom_endpoint.set_member_value("port", (uint16_t)endpoint.port());
    
    // [注意]：由于我们不再解析协议头，无法获取 SADD/RADD。
    // 我们必须假定 framing_addr。
    // 如果是点对点连接，0x01 或 0x00 通常都可以。
    // 如果你有多个 Client，这里可能需要根据 IP 来区分，或者 Client 必须在 UDP Payload 里带 session ID。
    // 既然之前日志显示 ClientID 是 0x01，我们这里默认设为 0x01。
    uint8_t dummy_framing_addr = 0x01; 
    custom_endpoint.set_member_value("framing_addr", dummy_framing_addr);
    
    input_packet.source = custom_endpoint;

    // 4. 清空 buffer (表示全部消费)
    client_io.recv_buffer.clear();

    // 5. 日志
    uint32_t client_key = 0;
    if(get_client_key(input_packet.source, client_key)) {
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("Raw Recv"), 
            "Key: 0x{:08X}, Len: {}, IP: {}", 
            client_key, len, endpoint.address().to_string());
    } else {
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_YELLOW("Raw Recv (New)"), 
            "Len: {}, IP: {}", 
            len, endpoint.address().to_string());
    }

    return true;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int /*timeout*/,
        TransportRc& transport_rc)
{
    // Pending 处理保持不变
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
                    // 直接接收，不做分包处理
                    std::vector<uint8_t> udp_payload_buffer(MAX_CLIENT_BUFFER_SIZE);
                    asio::ip::udp::endpoint remote_endpoint;
                    asio::error_code ec;
                    
                    size_t bytes_recvd = socket_.receive_from(asio::buffer(udp_payload_buffer), remote_endpoint, 0, ec);

                    if (!ec && bytes_recvd > 0)
                    {
                        udp_payload_buffer.resize(bytes_recvd);

                        std::lock_guard<std::mutex> lock(clients_mutex_);
                        auto it = client_io_map_.find(remote_endpoint);
                        if (it == client_io_map_.end())
                        {
                            it = client_io_map_.emplace(remote_endpoint, std::unique_ptr<ClientIO>(new ClientIO())).first;
                        }
                        
                        // 直接覆盖，因为是 Raw UDP 模式，假设一包一消息
                        // 如果之前的数据没处理完，这里选择追加还是覆盖取决于应用场景
                        // 为了避免粘包混乱，对于 Raw UDP 建议直接覆盖或确保 process_client_buffer 消耗得足够快
                        if (!it->second->recv_buffer.empty()) {
                             // 如果 buffer 不为空，追加数据
                             it->second->recv_buffer.insert(
                                 it->second->recv_buffer.end(),
                                 udp_payload_buffer.begin(),
                                 udp_payload_buffer.end()
                             );
                        } else {
                             it->second->recv_buffer = std::move(udp_payload_buffer);
                        }
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

/**
 * 发送函数 (纯透传模式 Raw Pass-through)
 * 不添加任何 7E 头，直接发送 Payload
 */
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

        size_t payload_len = output_packet.message->get_len();
        const uint8_t* msg_buf = output_packet.message->get_buf();

        // 直接发送，不加头
        asio::error_code ec;
        size_t sent = socket_.send_to(asio::buffer(msg_buf, payload_len), destination_endpoint, 0, ec);

        if (ec)
        {
            UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("Socket Send Error"), "ec: {}", ec.message());
            transport_rc = TransportRc::server_error;
            return false;
        }
        
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_YELLOW("[** >> Raw Sent >> **]"), 
            "Len: {}", sent);

        return true;
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
