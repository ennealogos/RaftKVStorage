#pragma once

#include <cstdint>
#include <string>

#include "google/protobuf/message.h"
#include "google/protobuf/descriptor.h"

class Buffer; // NetLib 中的 Buffer 类

namespace rpc {

// rpc_header.proto 生成的类，头文件在 .cc 再 include
struct RpcRequestHeader; 
struct RpcResponseHeader;

enum FrameState {
    kOk,
    kExpectMore,
    kCorrupt,
};

// 对节点请求编码
/*
请求帧（frame_len 包含它自己的 4 字节）：
+----------------+----------------+----------------------+-----------------+
| frame_len (4B) | header_len (4B)| RPCHeader (protobuf) | args (protobuf) |
+----------------+----------------+----------------------+-----------------+
frame_len = 4 + 4 + header_len + args_len
*/
std::string encodeRequest(const google::protobuf::MethodDescriptor* method,
                        const google::protobuf::Message& request, const int64_t& requestId);

// 对节点响应编码
/*
响应帧（frame_len 包含它自己的 4 字节）：
+----------------+----------------+----------------------+---------------------------------+
| frame_len (4B) | rsp_header_len (4B)| RPCResponseHeader (protobuf) | rsp_body (protobuf) |
+----------------+----------------+----------------------+---------------------------------+
frame_len = 4 + 4 + rsp_header_len + response_len
*/
std::string encodeResponse(const google::protobuf::Message& response, const int32_t errorCode,
                        const std::string& errorText, const int64_t& requestId);

// 从 Buffer 中读取收到的数据帧（完整帧，包含开头的 4 字节 frame_len），存入 frame 中
FrameState tryTakeFrame(Buffer* buffer, std::string* frame);

// 对请求解码
FrameState decodeRequest(const std::string& frame, RpcRequestHeader* header, std::string* args);

// 对响应解码
FrameState decodeResponse(const std::string& frame, RpcResponseHeader* header, google::protobuf::Message* response);

} // namespace rpc