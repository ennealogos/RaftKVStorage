#include "RpcCodec.h"

#include <arpa/inet.h>
#include <cstring>
#include "Buffer.h"
#include "RpcHeader.pb.h"

namespace {

// 单个帧的最大长度（64MB）：防止损坏/恶意的长度字段导致无限等待，也保证后续 int 转换安全
constexpr uint32_t kMaxFrameLen = 64 * 1024 * 1024;

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
std::string encodeRequest(const google::protobuf::MethodDescriptor* method, 
                        const google::protobuf::Message& request, const int64_t& requestId) {
    std::string args; // 要传输的业务数据（投票消息，日志同步消息等）
    request.SerializeToString(&args); // 对要传输的请求进行序列化，并存储 args 中

    RpcRequestHeader header;
    header.set_service_name(method->service()->name());
    header.set_method_name(method->name());
    header.set_request_id(requestId); // 设置本次请求的 id
    header.set_args_size(static_cast<uint32_t>(args.size())); // 业务数据的长度

    std::string header_str;
    header.SerializeToString(&header_str);

    std::string frame; // 最终发送的数据帧

    // frame_len 表示数据帧的总长度：
    //   frame_len 字段(4B) + header_len 字段(4B) + header + args
    const size_t frame_len_size = 4 + 4 + header_str.size() + args.size();
    if (frame_len_size > kMaxFrameLen) return {}; // 空串表示编码失败
    const uint32_t frame_len = static_cast<uint32_t>(frame_len_size);

    appendU32(&frame, frame_len);
    // header 长度
    appendU32(&frame, static_cast<uint32_t>(header_str.size()));

    // 再接上 header 和业务数据
    frame += header_str;
    frame += args;
    return frame;
}

// 对响应进行编码，返回用于传输的数据帧
std::string encodeResponse(const google::protobuf::Message& response, const int32_t errorCode,
                         const std::string& errorText, const int64_t& requestId) {
    // 数据帧组成：总长度 + 要传输的响应
    std::string body;
    response.SerializeToString(&body);

    RpcResponseHeader rspHeader;
    rspHeader.set_request_id(requestId);
    rspHeader.set_error_code(errorCode);
    rspHeader.set_error_text(errorText);
    // 序列化为字符串
    std::string header_str;
    rspHeader.SerializeToString(&header_str);

    const size_t frame_len_size = 4 + 4 + header_str.size() + body.size();
    if (frame_len_size > kMaxFrameLen) return {}; // 空串表示编码失败
    const uint32_t frame_len = static_cast<uint32_t>(frame_len_size);

    std::string frame;

    appendU32(&frame, frame_len); // 填入总长度
    appendU32(&frame, static_cast<uint32_t>(header_str.size())); // 填入头部长度

    frame += header_str; // 填入头部
    frame += body; // 填入响应体

    return frame;
}

// 尝试从 buffer 中取出一个完整帧（包含开头的 4 字节 frame_len）。
// 返回 false 表示数据还不完整（buffer 保持不变），不代表数据损坏。
FrameState tryTakeFrame(Buffer* buffer, std::string* frame) {
    // 连长度字段都没收全
    if (buffer->readableBytes() < 4) {
        return FrameState::kExpectMore;
    }

    uint32_t frame_len = readU32(buffer->peek()); // 读取数据帧头的 4 字节总长度

    // 最小合法帧是 4 字节（空响应体）；更小说明长度字段损坏
    // 防御超大帧：避免无限等待，也保证后续 int 转换安全
    if (frame_len < 4 || frame_len > kMaxFrameLen) {
        // 长度非法
        return FrameState::kCorrupt;
    }

    // 检查整个帧是否已经收全
    if (buffer->readableBytes() < frame_len) {
        return FrameState::kExpectMore;
    }

    *frame = buffer->retrieveAsString(frame_len); // 取出完整帧（含长度字段）
    return FrameState::kOk;
}

// 对数据帧解码
FrameState decodeRequest(const std::string& frame, RpcRequestHeader* header, std::string* args) {
    if (frame.size() < 8) { // frame_len + header_len 两个字段
        return FrameState::kExpectMore;
    }

    // 校验帧头长度与实际帧长一致（frame 来自 tryTakeFrame 时应恒成立）
    uint32_t frame_len = readU32(frame.data());
    if (frame_len != frame.size()) return FrameState::kCorrupt;

    uint32_t header_len = readU32(frame.data() + 4); // frame 中包含开头的 4 字节 frame_len
    // 用 size_t 比较，避免 4 + 4 + header_len 在 uint32_t 下回绕
    if (header_len > frame.size() - 8) return FrameState::kCorrupt; // 帧内数据不足以容纳 header

    if (!header->ParseFromArray(frame.data() + 8, static_cast<int>(header_len))) return FrameState::kCorrupt;

    size_t args_len = frame.size() - 8 - header_len;
    if (header->args_size() != args_len) return FrameState::kCorrupt; // 交叉验证
    args->assign(frame.data() + 8 + header_len, args_len);
    return FrameState::kOk;
}

// 对响应解码
FrameState decodeResponse(const std::string& frame, RpcResponseHeader* header, google::protobuf::Message* response) {
    if (frame.size() < 8) return FrameState::kExpectMore;

    // 校验帧头长度与实际帧长一致
    uint32_t frame_len = readU32(frame.data());
    if (frame_len != frame.size()) return FrameState::kCorrupt;

    uint32_t header_len = readU32(frame.data() + 4); // frame 中包含开头的 4 字节 frame_len
    // 用 size_t 比较，避免 4 + 4 + header_len 在 uint32_t 下回绕
    if (header_len > frame.size() - 8) return FrameState::kCorrupt; // 帧内数据不足以容纳 header

    if (!header->ParseFromArray(frame.data() + 8, static_cast<int>(header_len))) return FrameState::kCorrupt;

    if (!response->ParseFromArray(frame.data() + 4, static_cast<int>(frame.size() - 4))) {
        return FrameState::kCorrupt;
    } else {
        return FrameState::kOk;
    }
}

} // namespace rpc
