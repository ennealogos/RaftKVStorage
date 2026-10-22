#pragma once

#include <cstdint>
#include <string>

#include "google/protobuf/message.h"
#include "google/protobuf/descriptor.h"

class Buffer; // NetLib 中的 Buffer 类

namespace rpc {

struct RpcHeader; // rpc_header.proto 生成的类，头文件在 .cc 再 include

// 对节点请求编码
/*
请求帧：
+----------------+----------------+----------------------+-------------------+
| total_len (4B) | header_len (4B)| RpcHeader (protobuf) | args (protobuf)   |
+----------------+----------------+----------------------+-------------------+
*/
std::string encodeRequest(const google::protobuf::MethodDescriptor* method, const google::protobuf::Message& request);

// 对节点响应编码
/*
响应帧：
+----------------+---------------------------+
| total_len (4B) | response (protobuf bytes) |
+----------------+---------------------------+
*/
std::string encodeResponse(const google::protobuf::Message& response);

// 从 Buffer 中读取收到的数据帧，存入 frame 中
bool tryTakeFrame(Buffer* buffer, std::string* frame);

// 对请求解码
bool decodeRequest(const std::string& frame, RpcHeader* header, std::string* args);

// 对响应解码
bool decodeResponse(const std::string& packet, google::protobuf::Message* response);

} // namespace rpc