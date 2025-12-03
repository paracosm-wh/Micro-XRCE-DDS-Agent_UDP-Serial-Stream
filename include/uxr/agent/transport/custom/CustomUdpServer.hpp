#ifndef UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_
#define UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_

#include <uxr/agent/transport/Server.hpp>
#include <uxr/agent/transport/endpoint/CustomEndPoint.hpp>
#include <uxr/agent/transport/stream_framing/StreamFramingProtocol.hpp>

#include <vector>
#include <chrono>
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
    // 上次发送结束的时间点
    std::chrono::steady_clock::time_point last_send_time_;
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

    // FramingIO 回调函数
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
    
    // [注释] framing_io_ 解码时使用的临时 buffer，必须足够大以容纳最大帧
    uint8_t buffer_[SERVER_BUFFER_SIZE]; 
    FramingIO framing_io_;
    
    // [注释] 接收缓冲：用于暂存从 UDP socket 读取的原始数据流
    std::vector<uint8_t> input_buffer_;
    size_t input_buffer_pos_;
    
    // [注释] 发送缓冲：用于聚合 FramingIO 生成的 Header+Payload+CRC，确保单次 UDP 发送
    std::vector<uint8_t> tx_buffer_;

    // 地址映射辅助变量
    struct sockaddr_in source_to_map_; // 最近一次接收到的 UDP 来源地址
};

} // namespace uxr
} // namespace eprosima

#endif // UXR_AGENT_TRANSPORT_CUSTOM_CUSTOM_UDP_SERVER_HPP_
