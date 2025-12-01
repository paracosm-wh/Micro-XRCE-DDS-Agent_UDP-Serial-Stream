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

// Internal pending frames queue (kept for compatibility with Server architecture)
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
        // Increase buffer sizes to handle high throughput or large topics
        socket_.set_option(asio::socket_base::receive_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.set_option(asio::socket_base::send_buffer_size(SERVER_BUFFER_SIZE * 10));
        socket_.bind(endpoint);
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("CustomUDP server started (Raw Mode)"),
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
 * process_client_buffer (Raw Mode)
 * Directly takes the UDP payload and passes it to the Agent as a message.
 * No HDLC deframing is performed.
 */
bool CustomUdpServer::process_client_buffer(
    ClientIO& client_io,
    const asio::ip::udp::endpoint& endpoint,
    InputPacket<CustomEndPoint>& input_packet,
    TransportRc& /*transport_rc*/)
{
    // If buffer is empty, nothing to do
    if (client_io.recv_buffer.empty())
    {
        return false;
    }

    // 1. Create InputMessage directly from the received buffer
    // We assume the UDP packet contains exactly one valid XRCE-DDS message (or fragment).
    input_packet.message.reset(new InputMessage(client_io.recv_buffer.data(), client_io.recv_buffer.size()));

    // 2. Set up the source endpoint identity
    CustomEndPoint custom_endpoint;
    custom_endpoint.add_member<std::string>("address");
    custom_endpoint.add_member<uint16_t>("port");
    custom_endpoint.add_member<uint8_t>("framing_addr"); // Kept for compatibility

    custom_endpoint.set_member_value("address", endpoint.address().to_string());
    custom_endpoint.set_member_value("port", (uint16_t)endpoint.port());
    // In raw UDP, we don't have a framing address inside the packet.
    // We can default it to 0 or derive it from the IP if needed.
    // Here we use 0x00 as a default "Stream ID".
    custom_endpoint.set_member_value("framing_addr", (uint8_t)0x00);

    input_packet.source = custom_endpoint;

    // 3. Logging
    uint32_t client_key = 0;
    get_client_key(input_packet.source, client_key);
    
    // Optional: Only log if we successfully identified a client, to reduce noise
    if(client_key != 0) {
        UXR_AGENT_LOG_MESSAGE(
            UXR_DECORATE_YELLOW("[==>> CustomUDP (Raw) <<==]"),
            client_key,
            input_packet.message->get_buf(),
            input_packet.message->get_len());
    }

    // 4. Clear the buffer immediately since we consumed all of it
    client_io.recv_buffer.clear();
    client_io.read_pos = 0;

    return true;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int /*timeout*/,
        TransportRc& transport_rc)
{
    // Check pending frames queue first (unlikely used in Raw mode but good practice)
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
        // Process existing data in buffers
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

        // Poll socket
        pollfd pfd{socket_.native_handle(), POLLIN, 0};
        int poll_rv = poll(&pfd, 1, 1000); // 1000ms timeout

        if (poll_rv > 0)
        {
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
            {
                UXR_AGENT_LOG_ERROR(UXR_DECORATE_RED("UDP Socket Error"), "poll() error events");
                transport_rc = TransportRc::server_error;
                return false;
            }

            if (pfd.revents & POLLIN)
            {
                try
                {
                    // Use a vector to receive data to handle variable sizes up to max MTU
                    std::vector<uint8_t> buffer(SERVER_BUFFER_SIZE); 
                    asio::ip::udp::endpoint remote_endpoint;
                    asio::error_code ec;
                    
                    size_t bytes_recvd = socket_.receive_from(asio::buffer(buffer), remote_endpoint, 0, ec);

                    if (!ec && bytes_recvd > 0)
                    {
                        buffer.resize(bytes_recvd); // Shrink to fit

                        std::lock_guard<std::mutex> lock(clients_mutex_);
                        auto it = client_io_map_.find(remote_endpoint);
                        if (it == client_io_map_.end())
                        {
                            it = client_io_map_.emplace(remote_endpoint, std::unique_ptr<ClientIO>(new ClientIO())).first;
                        }
                        
                        // In Raw mode, we assume packet boundaries matter. 
                        // Overwrite buffer with new packet (assuming previous was processed or dropped)
                        // If you need to support fragmentation/streams, use insert() instead.
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

        // Raw send: directly send the message buffer without adding framing
        asio::error_code ec;
        size_t sent = socket_.send_to(
            asio::buffer(output_packet.message->get_buf(), output_packet.message->get_len()), 
            destination_endpoint, 
            0, 
            ec);

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
            UXR_DECORATE_YELLOW("[** >> CustomUDP (Raw) >> **]"),
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
