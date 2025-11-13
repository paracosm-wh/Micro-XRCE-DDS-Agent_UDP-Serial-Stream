#include <uxr/agent/transport/custom/CustomUdpServer.hpp>
#include <uxr/agent/utils/Conversion.hpp>
#include <uxr/agent/logger/Logger.hpp>

#include <asio.hpp>
#include <poll.h>
#include <unistd.h>

namespace eprosima {
namespace uxr {

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

bool CustomUdpServer::process_client_buffer(
    ClientIO& client_io,
    const asio::ip::udp::endpoint& endpoint,
    InputPacket<CustomEndPoint>& input_packet,
    TransportRc& transport_rc)
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

    uint8_t remote_addr;
    std::array<uint8_t, SERVER_BUFFER_SIZE> message_buffer;
    int framing_timeout = 1;
    ssize_t bytes_read = framing_io.read_framed_msg(
        message_buffer.data(), message_buffer.size(), remote_addr, framing_timeout, transport_rc);

    if (bytes_read > 0)
    {
        input_packet.message.reset(new InputMessage(message_buffer.data(), static_cast<size_t>(bytes_read)));
        client_io.recv_buffer.erase(client_io.recv_buffer.begin(), client_io.recv_buffer.begin() + client_io.read_pos);

        CustomEndPoint custom_endpoint;
        custom_endpoint.add_member<std::string>("address");
        custom_endpoint.add_member<uint16_t>("port");
        custom_endpoint.add_member<uint8_t>("framing_addr");
        custom_endpoint.set_member_value("address", endpoint.address().to_string());
        custom_endpoint.set_member_value("port", endpoint.port());
        custom_endpoint.set_member_value("framing_addr", uint8_t(remote_addr));
        input_packet.source = custom_endpoint;

        uint32_t client_key = 0;
        get_client_key(input_packet.source, client_key);
        UXR_AGENT_LOG_MESSAGE(
            UXR_DECORATE_YELLOW("[==>> CustomUDP (hdlc) <<==]"),
            client_key,
            input_packet.message->get_buf(),
            input_packet.message->get_len());
        return true;
    }
    return false;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int timeout,
        TransportRc& transport_rc)
{

    auto end_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);

    do
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

        auto time_left = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - std::chrono::steady_clock::now()).count();
        if (time_left <= 0)
        {
            break;
        }

        pollfd pfd{socket_.native_handle(), POLLIN, 0};
        int poll_rv = poll(&pfd, 1, static_cast<int>(time_left));

        if (poll_rv > 0)
        {
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
            {
                UXR_AGENT_LOG_ERROR(
                    UXR_DECORATE_RED("UDP Socket Error"),
                    "poll() returned error event: {}", pfd.revents);
                transport_rc = TransportRc::server_error;
                return false;
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
                        it->second->recv_buffer.insert(it->second->recv_buffer.end(), udp_payload_buffer.begin(), udp_payload_buffer.begin() + bytes_recvd);
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
            // Error
            transport_rc = TransportRc::server_error;
            return false;
        }
    } while (std::chrono::steady_clock::now() < end_time);

    transport_rc = TransportRc::timeout_error;
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
