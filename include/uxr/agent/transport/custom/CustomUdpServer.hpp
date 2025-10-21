#pragma once

#include <uxr/agent/transport/Server.hpp>
#include <uxr/agent/transport/endpoint/CustomEndPoint.hpp>
#include <uxr/agent/transport/stream_framing/StreamFramingProtocol.hpp>

#include <asio.hpp>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <map>
#include <vector>
#include <queue>
#include <memory>

namespace eprosima {
namespace uxr {

class CustomUdpServer : public Server<CustomEndPoint>
{
public:
    UXR_AGENT_EXPORT CustomUdpServer(
            uint16_t port,
            Middleware::Kind middleware_kind);

    UXR_AGENT_EXPORT ~CustomUdpServer() final;

#ifdef UAGENT_DISCOVERY_PROFILE
    bool has_discovery() final { return false; }
#endif
#ifdef UAGENT_P2P_PROFILE
    bool has_p2p() final { return false; }
#endif

private:
    bool init() final;
    bool fini() final;

    bool recv_message(
            InputPacket<CustomEndPoint>& input_packet,
            int timeout,
            TransportRc& transport_rc) final;

    bool send_message(
            OutputPacket<CustomEndPoint> output_packet,
            TransportRc& transport_rc) final;

    bool handle_error(TransportRc transport_rc) final;

    void receiver_thread_entry();

private:
    // Framing state per client
    struct ClientIO
    {
        std::vector<uint8_t> buffer; // Raw byte buffer from UDP
        size_t read_pos = 0;
        std::unique_ptr<FramingIO> framer;
    };

    uint16_t port_;
    std::thread receiver_thread_;
    std::atomic<bool> running_;
    asio::io_service io_service_;
    asio::ip::udp::socket socket_;



    std::map<asio::ip::udp::endpoint, ClientIO> client_io_map_;
    std::mutex clients_mutex_;

    std::queue<InputPacket<CustomEndPoint>> message_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
};

} // namespace uxr
} // namespace eprosima
