#ifndef UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_
#define UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_

#include <uxr/agent/transport/Server.hpp>
#include <uxr/agent/transport/endpoint/CustomEndPoint.hpp>
#include <uxr/agent/transport/stream_framing/StreamFramingProtocol.hpp>

#include <asio.hpp>
#include <map>
#include <mutex>
#include <vector>
#include <chrono>
#include <memory>

namespace eprosima {
namespace uxr {

class CustomUdpServer : public Server<CustomEndPoint>
{
public:
    CustomUdpServer(
        uint16_t port,
        Middleware::Kind middleware_kind);

    ~CustomUdpServer() override;

    /**
     * @brief This function is not supported in this custom transport.
     */
    bool has_discovery() final { return false; }

    /**
     * @brief This function is not supported in this custom transport.
     */
    bool has_p2p() final { return false; }

private:
    struct ClientIO
    {
        // recv buffering
        std::vector<uint8_t> recv_buffer;
        size_t read_pos = 0;
    };

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


    bool process_client_buffer(
        ClientIO& client_io,
        const asio::ip::udp::endpoint& endpoint,
        InputPacket<CustomEndPoint>& input_packet,
        TransportRc& transport_rc);

private:
    uint16_t port_;
    asio::io_service io_service_;
    asio::ip::udp::socket socket_;
    std::map<asio::ip::udp::endpoint, std::unique_ptr<ClientIO>> client_io_map_;
    std::mutex clients_mutex_;
};

} // namespace uxr
} // namespace eprosima

#endif // UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_