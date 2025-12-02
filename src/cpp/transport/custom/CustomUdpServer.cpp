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
          0x00, // Default Agent Address
          std::bind(&CustomUdpServer::write_data, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
          std::bind(&CustomUdpServer::read_data, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4))
    , input_buffer_pos_(0)
{
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

            UXR_AGENT_LOG_DEBUG(
                UXR_DECORATE_GREEN("Custom UDP port opened"),
                "port: {}",
                port_);

            UXR_AGENT_LOG_INFO(
                UXR_DECORATE_GREEN("running..."),
                "port: {}",
                port_);
        }
        else
        {
            UXR_AGENT_LOG_ERROR(
                UXR_DECORATE_RED("bind error"),
                "port: {}, errno: {}",
                port_, errno);
        }
    }
    else
    {
        UXR_AGENT_LOG_ERROR(
            UXR_DECORATE_RED("socket error"),
            "port: {}, errno: {}",
            port_, errno);
    }

    return rv;
}

bool CustomUdpServer::fini()
{
    if (-1 == poll_fd_.fd)
    {
        return true;
    }

    bool rv = false;
    if (0 == ::close(poll_fd_.fd))
    {
        poll_fd_.fd = -1;
        rv = true;
        UXR_AGENT_LOG_INFO(
            UXR_DECORATE_GREEN("server stopped"),
            "port: {}",
            port_);
    }
    else
    {
        UXR_AGENT_LOG_ERROR(
            UXR_DECORATE_RED("socket error"),
            "port: {}, errno: {}",
            port_, errno);
    }
    return rv;
}

bool CustomUdpServer::recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int timeout,
        TransportRc& transport_rc)
{
    bool rv = false;
    uint8_t remote_addr = 0x00;
    ssize_t bytes_read = 0;

    // This call drives read_data, which fetches UDP packets
    do
    {
        bytes_read = framing_io_.read_framed_msg(
            buffer_, SERVER_BUFFER_SIZE, remote_addr, timeout, transport_rc);
    }
    while ((0 == bytes_read) && (0 < timeout));

    if (0 < bytes_read)
    {
        input_packet.message.reset(new InputMessage(buffer_, static_cast<size_t>(bytes_read)));
        
        // Construct CustomEndPoint
        // We use the address from the UDP packet (source_to_map_) 
        // AND the framing address from the message (remote_addr)
        CustomEndPoint custom_endpoint;
        custom_endpoint.add_member<std::string>("address");
        custom_endpoint.add_member<uint16_t>("port");
        custom_endpoint.add_member<uint8_t>("framing_addr");

        custom_endpoint.set_member_value("address", std::string(inet_ntoa(source_to_map_.sin_addr)));
        // Cast to uint16_t explicitly to ensure correct storage type in CustomEndPoint
        custom_endpoint.set_member_value("port", static_cast<uint16_t>(ntohs(source_to_map_.sin_port)));
        custom_endpoint.set_member_value("framing_addr", remote_addr);

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

    // Prepare destination for write_data
    memset(&dest_to_send_, 0, sizeof(dest_to_send_));
    dest_to_send_.sin_family = AF_INET;
    dest_to_send_.sin_port = htons(output_packet.destination.get_member<uint16_t>("port"));
    std::string ip_str = output_packet.destination.get_member<std::string>("address");
    inet_aton(ip_str.c_str(), &dest_to_send_.sin_addr);

    uint8_t dest_framing_addr = output_packet.destination.get_member<uint8_t>("framing_addr");

    ssize_t bytes_written =
            framing_io_.write_framed_msg(
                output_packet.message->get_buf(),
                output_packet.message->get_len(),
                dest_framing_addr,
                transport_rc);

    if ((0 < bytes_written) && (
         static_cast<size_t>(bytes_written) == output_packet.message->get_len()))
    {
        rv = true;

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
    return rv;
}

bool CustomUdpServer::handle_error(
        TransportRc /*transport_rc*/)
{
    return fini() && init();
}

ssize_t CustomUdpServer::write_data(
        uint8_t* buf,
        size_t len,
        TransportRc& transport_rc)
{
    size_t rv = 0;
    ssize_t bytes_written = sendto(
        poll_fd_.fd,
        buf,
        len,
        0,
        reinterpret_cast<struct sockaddr*>(&dest_to_send_),
        sizeof(dest_to_send_));

    if (0 < bytes_written)
    {
        rv = size_t(bytes_written);
    }
    else
    {
        transport_rc = TransportRc::server_error;
    }
    return rv;
}

ssize_t CustomUdpServer::read_data(
        uint8_t* buf,
        size_t len,
        int timeout,
        TransportRc& transport_rc)
{
    // 1. Serve from internal buffer if available
    if (input_buffer_pos_ < input_buffer_.size())
    {
        size_t available = input_buffer_.size() - input_buffer_pos_;
        size_t to_copy = (available < len) ? available : len;
        memcpy(buf, input_buffer_.data() + input_buffer_pos_, to_copy);
        input_buffer_pos_ += to_copy;
        return to_copy;
    }

    // 2. If empty, fetch new packet
    input_buffer_.clear();
    input_buffer_pos_ = 0;

    int poll_rv = poll(&poll_fd_, 1, timeout);
    if(poll_fd_.revents & (POLLERR+POLLHUP))
    {
        transport_rc = TransportRc::server_error;
    }
    else if (0 < poll_rv)
    {
        // We have a packet waiting
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
            input_buffer_.assign(temp_buf, temp_buf + bytes_recvd);
            source_to_map_ = client_addr; // Capture source of this stream chunk
            
            size_t to_copy = (size_t(bytes_recvd) < len) ? size_t(bytes_recvd) : len;
            memcpy(buf, input_buffer_.data(), to_copy);
            input_buffer_pos_ = to_copy;
            return to_copy;
        }
        else if (0 > bytes_recvd)
        {
            transport_rc = TransportRc::server_error;
        }
    }
    else
    {
        transport_rc = (poll_rv == 0) ? TransportRc::timeout_error : TransportRc::server_error;
    }
    return 0;
}

} // namespace uxr
} // namespace eprosima
