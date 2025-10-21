# Micro XRCE-DDS Agent (添加UDP串口通信版)

本项目是 [eProsima Micro XRCE-DDS Agent](https://github.com/eProsima/Micro-XRCE-DDS-Agent) 的一个功能添加版本。在原有Agent的基础上，我们新增并集成了一个功能强大的自定义UDP传输模块——`CustomUdpServer`。

## 核心特性: `CustomUdpServer` 自定义UDP传输

`CustomUdpServer` 是一个专为满足特定硬件（如运行DroneBridge固件的ESP32）或网络环境需求而设计的自定义传输层。它允许Micro XRCE-DDS Agent通过UDP进行高效、可靠的通信。

该模块的核心优势在于其强大的数据链路层设计，主要包含以下关键特性：

1.  **基于UDP的HDLC帧协议**
    *   将标准的XRCE消息封装在HDLC（高级数据链路控制）风格的帧中进行传输。
    *   每一帧都包含起始/结束标志、地址信息和CRC-16校验和，确保了消息的完整性和正确性。
    *   每个HDLC帧作为一个独立的UDP数据包进行发送，兼顾了UDP的低延迟特性和HDLC的可靠性。

2.  **UDP分片重组机制**
    *   这是此模块最关键的功能之一。它内置了一个重组缓冲区，能够缓存来自同一客户端（由IP和端口标识）的多个UDP数据包。
    *   Agent能够将一个被分割在多个UDP包中的大型HDLC帧正确地重组为一条完整的消息。
    *   此特性使得传输大数据（如复杂的Topic定义或大容量数据样本）成为可能，有效避免了因分片问题导致的会话超时和通信失败。

3.  **面向嵌入式设备的优化**
    *   为嵌入式设备和无线链路（如Wi-Fi）的通信场景量身打造，在保证可靠性的同时，提供了比标准TCP更低的延迟。

## 如何使用

### 编译

编译过程与标准的eProsima Agent相同。

```bash
# 创建构建目录
mkdir build
cd build

# 运行CMake并编译
cmake ..
make
```

### 运行Agent

使用 `custom_udp` 关键字来启动这个自定义传输模块。

```bash
./MicroXRCEAgent custom_udp -p <端口号> -v <日志级别>
```

**参数说明:**
*   `-p <端口号>`: 指定Agent监听的UDP端口号 (例如: `-p 14552`)。
*   `-v <日志级别>`: 指定日志的详细程度 (例如: `-v 4` 对应Info级别, `-v 6` 对应Debug级别)。

**运行示例:**
```bash
./MicroXRCEAgent custom_udp -p 14552 -v 4
```

## 客户端配置要求

为了与本Agent正确通信，您的客户端（如ESP32）需要遵循以下配置：

1.  **HDLC帧封装**: 客户端必须使用与eProsima客户端库中 `StreamFramingProtocol` 兼容的HDLC帧封装协议。
2.  **地址配置**:
    *   客户端发送给Agent的HDLC帧中，**目标地址**必须为 `0x00`。
    *   客户端自身需要使用一个唯一的**源地址**（例如 `0x01`）。
3.  **分片 (可选)**: 如果客户端需要发送大于网络MTU的消息，它可以将一个HDLC帧分割到多个UDP数据包中。本Agent能够正确处理这种分片并进行重组。

## 原始项目与许可证

本项目基于 eProsima Micro XRCE-DDS Agent。
*   **原始仓库**: [https://github.com/eProsima/Micro-XRCE-DDS-Agent](https://github.com/eProsima/Micro-XRCE-DDS-Agent)
*   **许可证**: Apache License 2.0
