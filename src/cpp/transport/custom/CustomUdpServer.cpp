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
static constexpr uint8_t HDLC_FLAG = 0x7E;
static constexpr size_t MAX_CLIENT_BUFFER_SIZE = 16 * 1024; 
static constexpr size_t HEADER_SIZE = 5; // 7E + SADD + RADD + LEN_L + LEN_H
static constexpr size_t OVERHEAD_SIZE = 7; // Header(5) + CRC(2)
// ---------------------------------------------------------

// 辅助工具：打印 HEX 日志
// 作用：将 buffer 的前 len 个字节转换成字符串，方便在日志中查看协议头
static std::string hex_debug_str(const uint8_t* data, size_t len) {
    std::stringstream ss;
    ss << "[ ";
    ss << std::hex << std::uppercase;
    // 只打印前 16 个字节，防止日志爆炸
    size_t limit = (len > 16) ? 16 : len;
    for(size_t i=0; i<limit; ++i) {
        ss << std::setw(2) << std::setfill('0') << (int)data[i] << " ";
    }
    if (len > limit) ss << "... ";
    ss << "]";
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
 * 核心接收函数 (重写增强版)
 * 手动解析包含 RADD 的自定义协议，并输出详细的调试日志
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

    size_t recv_len = client_io.recv_buffer.size();
    uint8_t* buf = client_io.recv_buffer.data();

    // 3. 协议检查
    bool valid_frame = false;
    uint16_t payload_len = 0;
    uint8_t client_addr = 0; // 这将存储 Client 的 ID (SADD)

    do {
        // [Check 1] 长度是否足够最小帧？
        if (recv_len < OVERHEAD_SIZE) {
            UXR_AGENT_LOG_WARN(UXR_DECORATE_RED("Parse Fail"), "Frame too short: {} bytes", recv_len);
            break;
        }

        // [Check 2] 帧头 Flag 是否为 7E?
        if (buf[0] != HDLC_FLAG) {
            UXR_AGENT_LOG_WARN(UXR_DECORATE_RED("Parse Fail"), "Invalid Flag: 0x{:02X} (Expected 0x7E)", buf[0]);
            break;
        }

        // 协议结构: [0:7E] [1:SADD] [2:RADD] [3:LEN_L] [4:LEN_H] ...
        // === 关键修改 ===
        // 获取发送者的地址 (SADD)，以便我们回复给它
        client_addr = buf[1]; 
        
        // buf[2] 是 RADD (应该是指向 Agent 的，比如 0x00)，我们这里不需要处理它，或者可以校验它是否为 0x00

        // 提取长度
        payload_len = (uint16_t)buf[3] | ((uint16_t)buf[4] << 8);

        // [Check 3] 长度是否匹配?
        size_t expected_total_len = payload_len + OVERHEAD_SIZE;
        if (recv_len != expected_total_len) {
            UXR_AGENT_LOG_WARN(
                UXR_DECORATE_RED("Parse Fail"), 
                "Length Mismatch! UDP says: {}, Protocol says: {} (Payload: {})", 
                recv_len, expected_total_len, payload_len);
            UXR_AGENT_LOG_WARN(UXR_DECORATE_RED("Debug Dump"), "{}", hex_debug_str(buf, recv_len));
            break;
        }

        valid_frame = true;

    } while(0);

    if (!valid_frame) {
        client_io.recv_buffer.clear();
        return false;
    }

    // 4. 提取 Payload
    input_packet.message.reset(new InputMessage(&buf[HEADER_SIZE], payload_len));

    CustomEndPoint custom_endpoint;
    custom_endpoint.add_member<std::string>("address");
    custom_endpoint.add_member<uint16_t>("port");
    custom_endpoint.add_member<uint8_t>("framing_addr");
    
    custom_endpoint.set_member_value("address", endpoint.address().to_string());
    custom_endpoint.set_member_value("port", (uint16_t)endpoint.port());
    
    // === 这里设置回信地址 ===
    // 将提取到的 client_addr (原 SADD) 存入 framing_addr
    // 之后 send_message 会将其作为 RADD 发回给 Client
    custom_endpoint.set_member_value("framing_addr", client_addr); 
    
    input_packet.source = custom_endpoint;

    // 清空 buffer
    client_io.recv_buffer.clear();

    // 5. 成功日志
    uint32_t client_key = 0;
    if(get_client_key(input_packet.source, client_key)) {
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("Packet OK"), 
            "Key: 0x{:08X}, Payload: {}, ClientID: 0x{:02X}", 
            client_key, payload_len, client_addr);
    } else {
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_YELLOW("New Session?"), 
            "Payload: {}, ClientID: 0x{:02X} (No Key yet)", 
            payload_len, client_addr);
    }

    return true;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int /*timeout*/,
        TransportRc& transport_rc)
{
    // Pending Queue 处理保持不变
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
                    // 使用 Vector 动态接收，避免大包截断
                    std::vector<uint8_t> udp_payload_buffer(MAX_CLIENT_BUFFER_SIZE);
                    asio::ip::udp::endpoint remote_endpoint;
                    asio::error_code ec;
                    
                    size_t bytes_recvd = socket_.receive_from(asio::buffer(udp_payload_buffer), remote_endpoint, 0, ec);

                    if (!ec && bytes_recvd > 0)
                    {
                        // 缩小 vector 到实际接收大小
                        udp_payload_buffer.resize(bytes_recvd);

                        std::lock_guard<std::mutex> lock(clients_mutex_);
                        auto it = client_io_map_.find(remote_endpoint);
                        if (it == client_io_map_.end())
                        {
                            it = client_io_map_.emplace(remote_endpoint, std::unique_ptr<ClientIO>(new ClientIO())).first;
                        }
                        
                        // 将收到的数据追加到客户端 buffer
                        // 注意：对于 UDP 自定义协议，通常一包就是一帧
                        // 如果之前 buffer 里有垃圾数据，这里追加可能会导致问题
                        // 建议：如果你确信 UDP 不分片，这里可以直接覆盖 buffer
                        if (!it->second->recv_buffer.empty()) {
                             UXR_AGENT_LOG_WARN(UXR_DECORATE_YELLOW("Buffer Overwrite"), "Discarding {} old bytes", it->second->recv_buffer.size());
                             it->second->recv_buffer.clear();
                        }
                        
                        it->second->recv_buffer = std::move(udp_payload_buffer);
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
 * 发送函数 (重写增强版)
 * 手动构建包含 RADD 的帧
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

        // 获取目标 framing_addr (作为 RADD 发送)
        uint8_t target_radd = output_packet.destination.get_member<uint8_t>("framing_addr");
        size_t payload_len = output_packet.message->get_len();

        // 1. 手动构建帧
        // 结构: [7E] [SADD] [RADD] [LEN_L] [LEN_H] [PAYLOAD...] [CRC_L] [CRC_H]
        std::vector<uint8_t> frame;
        frame.reserve(payload_len + OVERHEAD_SIZE);

        frame.push_back(HDLC_FLAG); // 0x7E
        frame.push_back(0x00);      // SADD (Agent ID, usually 0x00)
        frame.push_back(target_radd); // RADD
        
        // LEN (Little Endian)
        frame.push_back(static_cast<uint8_t>(payload_len & 0xFF));
        frame.push_back(static_cast<uint8_t>((payload_len >> 8) & 0xFF));

        // Payload
        const uint8_t* msg_buf = output_packet.message->get_buf();
        frame.insert(frame.end(), msg_buf, msg_buf + payload_len);

        // CRC (Placeholder 0x0000)
        frame.push_back(0x00);
        frame.push_back(0x00);

        // 2. 发送日志
        /*
        UXR_AGENT_LOG_DEBUG(
            UXR_DECORATE_WHITE("Sending Frame"),
            "Payload: {}, RADD: 0x{:02X}, Header: {}",
            payload_len, target_radd, hex_debug_str(frame.data(), 5)
        );
        */

        // 3. 执行发送
        asio::error_code ec;
        size_t sent = socket_.send_to(asio::buffer(frame), destination_endpoint, 0, ec);

        if (ec)
        {
            UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("Socket Send Error"), "ec: {}", ec.message());
            transport_rc = TransportRc::server_error;
            return false;
        }
        
        // 成功发送日志 (精简版)
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_YELLOW("[** >> Sent >> **]"), 
            "Len: {} (Pay: {}), To: 0x{:02X}", 
            sent, payload_len, target_radd);

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
