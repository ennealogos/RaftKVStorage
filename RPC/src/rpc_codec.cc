#include "rpc_codec.h"

#include <arpa/inet.h>
#include <cstring>
#include "Buffer.h"
#include "rpc_header.pb.h"

namespace {

// 向指定字符串中追加 4字节的数据
void appendU32(std::string* out, uint32_t v) {
    uint32_t be = htonl(v); // 主机字节序（小端）转网络字节序（大端）
    out->append(reinterpret_cast<const char*>(&be), sizeof(be)); // 追加到 out 中
}

// 从指定指针处读取 4 字节的32位无符号整数
uint32_t readU32(const char* p) {
    uint32_t be = 0;
    std::memcpy(&be, p, sizeof(be));
    return ntohl(be);
}
} // namespace

namespace rpc {

// 对节点请求进行编码，返回用于传输的数据帧
std::string encodeRequest(const google::protobuf::MethodDescriptor* method, const google::protobuf::Message& request) {
    std::string args; // 要传输的业务数据（投票消息，日志同步消息等）
    request.SerializeToString(&args); // 对要传输的请求进行序列化，并存储 args 中

    RpcHeader header;
    header.set_service_name(method->service()->name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(args.size())); // 业务数据的长度

    std::string header_str;
    header.SerializeToString(&header_str);

    std::string frame; // 最终发送的数据帧

    // frame_len 表示数据帧的总长度：frame_len 字节数（4B） + header_len字节数（4B）+ header.size() + args.size()
    const uint32_t frame_len = 4 + 4 + header_str.size() + args.size();

    appendU32(&frame, frame_len);
    // header 长度
    appendU32(&frame, static_cast<uint32_t>(header_str.size()));

    // 再接上 header 和业务数据
    frame += header_str;
    frame += args;
    return frame;
}

// 对响应进行编码，返回用于传输的数据帧
std::string encodeResponse(const google::protobuf::Message& response) {
    // 数据帧组成：总长度 + 要传输的响应
    std::string body;
    response.SerializeToString(&body);

    std::string frame;
    const int32_t frame_len = 4 + body.size();
    // 计算总长度，并放在数据帧的开头 4 字节
    appendU32(&frame, frame_len);
    frame += body;

    return frame;
}

// 判断缓冲区中是否有一个可读取的帧，如果小于 4B，那么这个数据帧是损坏的，如果数据帧大小不是帧头指示的大小，说明数据帧没有接收完。
bool tryTakeFrame(Buffer* buffer, std::string* frame) {
    // 如果 buffer 中可读取的数据小于 4B，那么放弃
    if (buffer->readableBytes() < 4) return false;
    uint32_t frame_len = readU32(buffer->peek()); // 读取数据帧头的4字节总长度
    // 至少要能容纳 frame_len(4B) + header_len(4B)
    if (frame_len < 8) return false;
    
    // 检查数据帧是否完整
    if (buffer->readableBytes() < frame_len) return false;

    *frame = buffer->retrieveAsString(frame_len); // 取剩余部分
    return true;
}

// 对数据帧解码
bool decodeRequest(const std::string& frame, RpcHeader* header, std::string* args) {
    
}

}
