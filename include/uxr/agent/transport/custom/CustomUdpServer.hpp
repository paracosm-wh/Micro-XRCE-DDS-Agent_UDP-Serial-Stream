#ifndef UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_
#define UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_

#include <uxr/agent/transport/Server.hpp>
#include <uxr/agent/transport/endpoint/CustomEndPoint.hpp>
#include <uxr/agent/transport/stream_framing/StreamFramingProtocol.hpp>

#include <vector>
#include <sys/poll.h>
#include <netinet/in.h>

namespace eprosima {
namespace uxr {

class CustomUdpServer : public Server<CustomEndPoint>
{
public:
    CustomUdpServer(
        uint16_t port,
        Middleware::Kind middleware_kind);

    ~CustomUdpServer() override;

    bool has_discovery() final { return false; }
    bool has_p2p() final { return false; }

private:
    bool init() override;
    bool fini() override;

    bool recv_message(
        InputPacket<CustomEndPoint>& input_packet,
        int timeout,
        TransportRc& transport_rc) override;

    bool send_message(
        OutputPacket<CustomEndPoint> output_packet,
        TransportRc& transport_rc) override;

    bool handle_error(
        TransportRc transport_rc) override;

    ssize_t write_data(
        uint8_t* buf,
        size_t len,
        TransportRc& transport_rc);

    ssize_t read_data(
        uint8_t* buf,
        size_t len,
        int timeout,
        TransportRc& transport_rc);

private:
    uint16_t port_;
    struct pollfd poll_fd_;
    uint8_t buffer_[SERVER_BUFFER_SIZE];
    FramingIO framing_io_;
    
    // UDP buffering and addressing
    std::vector<uint8_t> input_buffer_;
    size_t input_buffer_pos_;
    struct sockaddr_in source_to_map_; // Last received address from UDP
    struct sockaddr_in dest_to_send_;  // Destination for the current send operation
};

} // namespace uxr
} // namespace eprosima

#endif // UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_
