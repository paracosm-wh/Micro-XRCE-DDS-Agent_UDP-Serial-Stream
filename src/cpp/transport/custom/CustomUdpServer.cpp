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

// --- 配置常量，可按需调整 --------------------------------
static constexpr uint8_t HDLC_FLAG = 0x7E; // 帧头标志（若不是 0x7E，请调整）
static constexpr size_t MAX_CLIENT_BUFFER_SIZE = 16 * 1024; // 单客户端缓冲上限
static constexpr size_t KEEP_TAIL_ON_RESYNC = 32; // 找不到 FLAG 时保留尾部字节数（用于半帧拼接）
// ---------------------------------------------------------

// 内部 pending 队列，用于存放 parse 出来的但尚未被上层消费的帧
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
        socket_.set_option(asio::socket_base::receive_buffer_size(SERVER_BUFFER_SIZE * 5));
        socket_.set_option(asio::socket_base::send_buffer_size(SERVER_BUFFER_SIZE * 5));
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
 * process_client_buffer
 *
 * - 支持一次解析出多个连续完整帧；
 * - 将第一个帧放入 input_packet 返回；
 * - 将解析出的后续帧存入内部 pending_frames_，由 recv_message 优先返回；
 * - 若解析失败但读过部分字节（read_pos > 0），会保留这些尾部字节（用于下次拼接）；
 * - 若 buffer 无 FLAG，则保留尾部 KEEP_TAIL_ON_RESYNC 字节以便拼接；
 */
bool CustomUdpServer::process_client_buffer(
    ClientIO& client_io,
    const asio::ip::udp::endpoint& endpoint,
    InputPacket<CustomEndPoint>& input_packet,
    TransportRc& transport_rc)
{
    UXR_AGENT_LOG_DEBUG(
        UXR_DECORATE_WHITE("Processing buffer"),
        "Buffer size before parse: {}",
        client_io.recv_buffer.size());

    // 限制单客户端 buffer 大小，防止无限增长
    if (client_io.recv_buffer.size() > MAX_CLIENT_BUFFER_SIZE)
    {
        // 保留最新的数据
        size_t drop = client_io.recv_buffer.size() - MAX_CLIENT_BUFFER_SIZE;
        client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.begin() + drop);
        UXR_AGENT_LOG_WARN(
            UXR_DECORATE_YELLOW("Client buffer truncated to max size"),
            "Dropped {} bytes, new size {}",
            drop,
            client_io.recv_buffer.size());
    }

    // 如果 buffer 为空，直接返回
    if (client_io.recv_buffer.empty())
    {
        return false;
    }

    std::vector<std::unique_ptr<InputMessage>> parsed_messages;
    std::vector<uint8_t> parsed_framing_addrs; // 与 parsed_messages 对应的 framing addr

    // 反复尝试解析 buffer，直到没有完整帧为止
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
        std::array<uint8_t, SERVER_BUFFER_SIZE> message_buffer;
        int framing_timeout = 1;
        ssize_t bytes_read = framing_io.read_framed_msg(
            message_buffer.data(), message_buffer.size(), remote_addr, framing_timeout, transport_rc);

        UXR_AGENT_LOG_DEBUG(
            UXR_DECORATE_WHITE("Parse attempt complete (multi-loop)"),
            "Parsed message length: {}, Raw bytes consumed: {}",
            bytes_read,
            client_io.read_pos);

        if (bytes_read > 0)
        {
            // 成功解析出一帧：拷贝消息并记录 framing addr
            auto imsg = std::unique_ptr<InputMessage>(new InputMessage(message_buffer.data(), static_cast<size_t>(bytes_read)));
            parsed_messages.push_back(std::move(imsg));
            parsed_framing_addrs.push_back(remote_addr);

            // 从 recv_buffer 中删除已消费的字节（read_pos）
            client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.begin() + client_io.read_pos);

            // 继续循环尝试解析 buffer 中的下一个完整帧（如果存在）
            if (client_io.recv_buffer.empty())
            {
                break;
            }
            else
            {
                // 有剩余数据，继续下一轮解析
                continue;
            }
        }
        else
        {
            // 未解析到完整帧
            if (client_io.read_pos > 0)
            {
                // read_pos > 0：说明已经读过一部分（半帧），但无法解析完整帧
                // 保留这部分数据（作为半帧），等待下次拼接
                // 我们不删除这段尾部，以便下次追加与解析
                UXR_AGENT_LOG_DEBUG(
                    UXR_DECORATE_WHITE("Partial consumption, preserving tail for next read"),
                    "Preserving {} bytes tail for client", client_io.recv_buffer.size());
                // 在前面循环中 read_pos 只用作读取位置，我们已经没有删除任何数据（因为在失败分支没有 erase）
                // 直接退出解析循环
                break;
            }
            else
            {
                // read_pos == 0：表示解析器一开始就无法识别当前 buffer（可能因为前导垃圾导致未对齐）
                // 尝试寻找第一个 HDLC_FLAG 进行 resync
                auto it = std::find(client_io.recv_buffer.begin(), client_io.recv_buffer.end(), HDLC_FLAG);
                if (it != client_io.recv_buffer.end())
                {
                    // 找到了帧头，将帧头之前的垃圾数据丢弃，但保留从帧头开始的半帧
                    if (it != client_io.recv_buffer.begin())
                    {
                        size_t dropped = static_cast<size_t>(std::distance(client_io.recv_buffer.begin(), it));
                        client_io.recv_buffer.erase(client_io.recv_buffer.begin(), it);
                        UXR_AGENT_LOG_WARN(
                            UXR_DECORATE_YELLOW("Resync: discarded leading garbage"),
                            "Dropped {} bytes before HDLC flag, new buffer size {}",
                            dropped,
                            client_io.recv_buffer.size());
                    }
                    // 等待更多数据到来以完成帧
                    break;
                }
                else
                {
                    // 整体 buffer 中没有找到 HDLC_FLAG：保守策略，保留尾部 KEEP_TAIL_ON_RESYNC 字节
                    if (client_io.recv_buffer.size() > KEEP_TAIL_ON_RESYNC)
                    {
                        size_t keep = KEEP_TAIL_ON_RESYNC;
                        size_t drop = client_io.recv_buffer.size() - keep;
                        client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.begin() + drop);
                        UXR_AGENT_LOG_WARN(
                            UXR_DECORATE_YELLOW("No HDLC flag found"),
                            "Buffer contained no HDLC flag, truncating to last {} bytes (dropped {})",
                            keep, drop);
                    }
                    else
                    {
                        // buffer 很短且无 flag，直接保留，等待更多数据
                    }
                    break;
                }
            }
        }
    } // end while parse-loop

    // 如果没有解析到任何完整帧，则返回 false
    if (parsed_messages.empty())
    {
        return false;
    }

    // parsed_messages 至少有一帧：把第一帧放到 input_packet 返回
    {
        auto &first_msg = parsed_messages.front();
        input_packet.message.reset(new InputMessage(first_msg->get_buf(), first_msg->get_len()));

        CustomEndPoint custom_endpoint;
        custom_endpoint.add_member<std::string>("address");
        custom_endpoint.add_member<uint16_t>("port");
        custom_endpoint.add_member<uint8_t>("framing_addr");
        custom_endpoint.set_member_value("address", endpoint.address().to_string());
        custom_endpoint.set_member_value("port", endpoint.port());
        custom_endpoint.set_member_value("framing_addr", uint8_t(parsed_framing_addrs.front()));
        input_packet.source = custom_endpoint;

        // 日志与上报
        uint32_t client_key = 0;
        get_client_key(input_packet.source, client_key);
        UXR_AGENT_LOG_MESSAGE(
            UXR_DECORATE_YELLOW("[==>> CustomUDP (hdlc) <<==]"),
            client_key,
            input_packet.message->get_buf(),
            input_packet.message->get_len());
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("CustomUDP message parsed successfully!"),
            "client_key: {}",
            client_key);
    }

    // 若 parsed_messages.size() > 1，将后续消息存入 pending_frames_ 以便下次 recv_message 调用先返回它们
    if (parsed_messages.size() > 1)
    {
        std::lock_guard<std::mutex> lock(pending_frames_mutex_);
        for (size_t i = 1; i < parsed_messages.size(); ++i)
        {
            PendingFrame pf;
            pf.msg = std::move(parsed_messages[i]);
            pf.address = endpoint.address().to_string();
            pf.port = endpoint.port();
            pf.framing_addr = parsed_framing_addrs[i];
            pending_frames_.push_back(std::move(pf));
        }
        UXR_AGENT_LOG_DEBUG(
            UXR_DECORATE_WHITE("Multiple frames parsed"),
            "Parsed {} frames (1 delivered, {} queued)", parsed_messages.size(), parsed_messages.size() - 1);
    }

    // 返回 true 表示已产生一个可交付的 input_packet
    return true;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int /*timeout*/,
        TransportRc& transport_rc)
{
    // 优先检查内部 pending_frames_ 队列（这是本次增强的关键点）
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

            uint32_t client_key = 0;
            get_client_key(input_packet.source, client_key);
            UXR_AGENT_LOG_MESSAGE(
                UXR_DECORATE_YELLOW("[==>> CustomUDP (hdlc) (pending) <<==]"),
                client_key,
                input_packet.message->get_buf(),
                input_packet.message->get_len());
            transport_rc = TransportRc::ok;
            return true;
        }
    }

    // This function will now wait indefinitely for a message.
    while (true)
    {
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            for (auto& it : client_io_map_)
            {
                if (it.second->recv_buffer.empty()) continue;
                if (process_client_buffer(*it.second, it.first, input_packet, transport_rc))
                {
                    return true; // Success, a message was processed.
                }
            }
        }

        // Poll for new data with a fixed, reasonable timeout (e.g., 1000ms).
        // This prevents busy-waiting while still being responsive.
        pollfd pfd{socket_.native_handle(), POLLIN, 0};
        int poll_rv = poll(&pfd, 1, 1000); // Poll for 1 second

        if (poll_rv > 0)
        {
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
            {
                UXR_AGENT_LOG_ERROR(
                    UXR_DECORATE_RED("UDP Socket Error"),
                    "poll() returned error event: {}", pfd.revents);
                transport_rc = TransportRc::server_error;
                return false; // Hard error, return immediately.
            }

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
                        // append 新收到的数据
                        it->second->recv_buffer.insert(
                            it->second->recv_buffer.end(),
                            udp_payload_buffer.begin(),
                            udp_payload_buffer.begin() + bytes_recvd);

                        // 防止单个插入导致超大，立刻裁剪（双重保险）
                        if (it->second->recv_buffer.size() > MAX_CLIENT_BUFFER_SIZE)
                        {
                            it->second->recv_buffer.erase(
                                it->second->recv_buffer.begin(),
                                it->second->recv_buffer.begin() + (it->second->recv_buffer.size() - MAX_CLIENT_BUFFER_SIZE));
                            UXR_AGENT_LOG_WARN(
                                UXR_DECORATE_YELLOW("Client buffer truncated after receive"),
                                "client buffer trimmed to {} bytes",
                                it->second->recv_buffer.size());
                        }
                    }
                }
                catch (const std::exception& e)
                {
                    UXR_AGENT_LOG_ERROR(
                        UXR_DECORATE_RED("recv_message error"),
                        "what: {}", e.what());
                }
            }
        }
        else if (poll_rv < 0)
        {
            // Error from poll()
            transport_rc = TransportRc::server_error;
            return false; // Hard error, return immediately.
        }
        // If poll_rv == 0 (timeout), the loop will just continue, and we will poll again.
        // This achieves the "wait forever" behavior without a fatal timeout.
    }
    // The function should never reach here.
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

        // Assemble the complete frame in a local buffer to ensure integrity.
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
            // Send the assembled frame immediately.
            asio::error_code ec;
            socket_.send_to(asio::buffer(framed_buffer), destination_endpoint, 0, ec);

            if (ec)
            {
                transport_rc = TransportRc::server_error;
                UXR_AGENT_LOG_ERROR(
                    UXR_DECORATE_RED("send_message error"),
                    "endpoint: {}:{}, what: {}",
                    destination_endpoint.address().to_string(),
                    destination_endpoint.port(),
                    ec.message());
                return false;
            }

            uint32_t client_key = 0;
            get_client_key(output_packet.destination, client_key);
            UXR_AGENT_LOG_MESSAGE(
                UXR_DECORATE_YELLOW("[** >> CustomUDP (hdlc) >> **]"),
                client_key,
                output_packet.message->get_buf(),
                output_packet.message->get_len());
            return true;
        }
    }
    catch(const std::exception& e)
    {
        UXR_AGENT_LOG_ERROR(
            UXR_DECORATE_RED("send_message error"),
            "what: {}", e.what());
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

