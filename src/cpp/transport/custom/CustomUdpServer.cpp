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

namespace eprosima {
namespace uxr {

// --- 配置常量 --------------------------------
static constexpr size_t MAX_CLIENT_BUFFER_SIZE = 16 * 1024; 
// ---------------------------------------------------------

// 内部 pending 队列 (用于存储未处理的消息)
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
        // 增大内核缓冲区，防止高吞吐时丢包
        socket_.set_option(asio::socket_base::receive_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.set_option(asio::socket_base::send_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.bind(endpoint);
        
        // [修复编译错误] 增加格式化参数 "{}"
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("CustomUDP server started (Raw Pass-through)"),
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
    // [修复编译错误] 增加格式化参数 "{}"
    UXR_AGENT_LOG_INFO(
        UXR_DECORATE_GREEN("CustomUDP server stopped"),
        "port: {}",
        port_);
    return true;
}

/**
 * 核心接收函数 (纯透传)
 * 逻辑：直接将 client_io.recv_buffer 的内容视为一条完整的 DDS 消息
 */
bool CustomUdpServer::process_client_buffer(
    ClientIO& client_io,
    const asio::ip::udp::endpoint& endpoint,
    InputPacket<CustomEndPoint>& input_packet,
    TransportRc& /*transport_rc*/)
{
    // 如果没有数据，直接返回
    if (client_io.recv_buffer.empty())
    {
        return false;
    }

    // 1. 获取数据指针和长度
    size_t len = client_io.recv_buffer.size();
    uint8_t* buf = client_io.recv_buffer.data();

    // 2. 直接构建消息 (不解析 7E, 不解析长度，不校验 CRC)
    input_packet.message.reset(new InputMessage(buf, len));

    // 3. 构建源端点信息
    CustomEndPoint custom_endpoint;
    custom_endpoint.add_member<std::string>("address");
    custom_endpoint.add_member<uint16_t>("port");
    custom_endpoint.add_member<uint8_t>("framing_addr"); // 保留字段

    custom_endpoint.set_member_value("address", endpoint.address().to_string());
    custom_endpoint.set_member_value("port", (uint16_t)endpoint.port());
    
    // 在纯透传模式下，没有协议头来告诉我们 Session ID (framing_addr)。
    // 我们必须指定一个默认值（例如 0x00），或者依靠 ClientKey (Agent 会自动处理)。
    // 注意：如果 ESP32 端期望收到的回复包里 RADD 是特定值（如 0x01），你需要在这里硬编码。
    uint8_t dummy_framing_addr = 0x00; 
    custom_endpoint.set_member_value("framing_addr", dummy_framing_addr);

    input_packet.source = custom_endpoint;

    // 4. 打印调试日志
    uint32_t client_key = 0;
    get_client_key(input_packet.source, client_key);
    
    if (client_key != 0) {
        UXR_AGENT_LOG_MESSAGE(
            UXR_DECORATE_YELLOW("[==>> RAW UDP RECV <<==]"),
            client_key,
            input_packet.message->get_buf(),
            input_packet.message->get_len());
    } else {
        // 尚未建立 Session 时的日志
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_WHITE("Raw UDP Recv (Unknown Session)"),
            "Len: {}, IP: {}",
            len, endpoint.address().to_string());
    }

    // 5. 消费完毕，清空缓冲区
    client_io.recv_buffer.clear();
    client_io.read_pos = 0;

    return true;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int /*timeout*/,
        TransportRc& transport_rc)
{
    // 优先处理 Pending 队列
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
        // 处理现有缓冲区数据
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

        // 轮询 Socket
        pollfd pfd{socket_.native_handle(), POLLIN, 0};
        int poll_rv = poll(&pfd, 1, 1000); 

        if (poll_rv > 0)
        {
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
            {
                // [修复编译错误] 增加格式化参数 "{}"
                UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("UDP Socket Error"), "{}", "poll() error events");
                transport_rc = TransportRc::server_error;
                return false;
            }

            if (pfd.revents & POLLIN)
            {
                try
                {
                    // 接收 UDP 数据
                    std::vector<uint8_t> buffer(SERVER_BUFFER_SIZE); 
                    asio::ip::udp::endpoint remote_endpoint;
                    asio::error_code ec;
                    
                    size_t bytes_recvd = socket_.receive_from(asio::buffer(buffer), remote_endpoint, 0, ec);

                    if (!ec && bytes_recvd > 0)
                    {
                        buffer.resize(bytes_recvd);

                        std::lock_guard<std::mutex> lock(clients_mutex_);
                        auto it = client_io_map_.find(remote_endpoint);
                        if (it == client_io_map_.end())
                        {
                            it = client_io_map_.emplace(remote_endpoint, std::unique_ptr<ClientIO>(new ClientIO())).first;
                        }
                        
                        // 在透传模式下，直接覆盖旧数据（假设 UDP 不粘包）
                        // 这样保证处理的是最新的完整包
                        it->second->recv_buffer = std::move(buffer);
                    }
                }
                catch (const std::exception& e)
                {
                    UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("recv_message exception"), "what: {}", e.what());
                }
            }
        }
        else if (poll_rv < 0)
        {
            transport_rc = TransportRc::server_error;
            return false;
        }
    }
    return false;
}

/**
 * 发送函数 (纯透传)
 * 逻辑：直接发送 Payload，不加 7E 头，不加 CRC
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

        // Raw Send: 直接发 buffer
        asio::error_code ec;
        size_t sent = socket_.send_to(
            asio::buffer(output_packet.message->get_buf(), output_packet.message->get_len()), 
            destination_endpoint, 
            0, 
            ec);
        
        // [修复未使用变量警告] 显式忽略 sent
        (void)sent;

        if (ec)
        {
            UXR_AGENT_LOG_ERROR(
                UXR_DECORATE_RED("send_message error"),
                "endpoint: {}:{}, what: {}",
                destination_endpoint.address().to_string(),
                destination_endpoint.port(),
                ec.message());
            transport_rc = TransportRc::server_error;
            return false;
        }

        uint32_t client_key = 0;
        get_client_key(output_packet.destination, client_key);
        UXR_AGENT_LOG_MESSAGE(
            UXR_DECORATE_YELLOW("[** >> RAW UDP SENT >> **]"),
            client_key,
            output_packet.message->get_buf(),
            output_packet.message->get_len());
            
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
