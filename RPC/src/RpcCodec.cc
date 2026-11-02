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
                        const google::protobuf::Message& request, const uint64_t requestId) {
    if (method == nullptr || method->service() == nullptr) {
        return {};
    }

    std::string args; // 要传输的业务数据（投票消息，日志同步消息等）
    if (!request.SerializeToString(&args)) {
        return {};
    }

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
                         const std::string& errorText, const uint64_t requestId) {
    // 只有成功响应才携带业务 body；错误响应 body 为空
    std::string body;
    if (errorCode == 0 && !response.SerializeToString(&body)) {
        return {};
    }

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
// kExpectMore：数据还不完整，buffer 保持不变，等下一次 read；
// kCorrupt：长度字段非法，数据已不可恢复；
// kOk：成功取出一帧。
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

// 校验 frame，校验后的 frame 各字段存储在 view 中
FrameState parseFrame(const std::string& frame, FrameView* view) {
    // 校验帧大小是否符合要求，保证后续 int 转换的类型安全
    if (frame.size() > kMaxFrameLen) {
        return FrameState::kCorrupt;
    }

    // 帧大小小于两个头部字段的大小，帧出现了问题
    if (frame.size() < 8) {
        return FrameState::kCorrupt;
    }

    // 检查帧长是否符合头部中记录的帧长度
    uint32_t frameLen = readU32(frame.data());
    if (frameLen != frame.size()) {
        return FrameState::kCorrupt;
    }

    // 检查头部字段是否完整
    uint32_t headerLen = readU32(frame.data() + 4);
    // headerLen > frame.size() - 8 帧内数据不足以容纳 header
    if (headerLen == 0 || headerLen > frame.size() - 8) {
        return FrameState::kCorrupt;
    }

    // 将帧中各字段填入 view 中
    view->header = frame.data() + 8;
    view->header_len = headerLen;
    view->body = frame.data() + 8 + headerLen;
    view->body_len = frame.size() - 8 - headerLen;
    return FrameState::kOk;
}

// 对数据帧解码
FrameState decodeRequest(const std::string& frame, RpcRequestHeader* header, std::string* args) {
    // 如果传入的接收头部和参数的指针为空，出错
    if (header == nullptr || args == nullptr) {
        return FrameState::kCorrupt;
    }

    FrameView view; // 解析出的头部和body都存放在 view 中
    if (parseFrame(frame, &view) != FrameState::kOk) {
        return FrameState::kCorrupt;
    }

    // 接收头部
    if (!header->ParseFromArray(view.header, static_cast<int>(view.header_len))) {
        return FrameState::kCorrupt;
    }

    // 0 是无效的请求 id
    if (header->request_id() == 0) {
        return FrameState::kCorrupt;
    }

    // 再次验证参数体大小是否正确
    if (header->args_size() != view.body_len) {
        return FrameState::kCorrupt;
    }

    // 接收参数体
    args->assign(view.body, view.body_len);
    return FrameState::kOk;
}

// 并发客户端在 IO 线程中并不知道具体的响应类型，不能直接 ParseFromArray，需要先将 body 原样取出，等待调用线程来解析
FrameState decodeResponse(const std::string& frame, RpcResponseHeader* header, std::string* body) {
    if (header == nullptr || body == nullptr) return FrameState::kCorrupt;

    FrameView view;
    if (parseFrame(frame, &view) != FrameState::kOk) {
        return FrameState::kCorrupt;
    }

    if (!header->ParseFromArray(view.header, static_cast<int>(view.header_len))) {
        return FrameState::kCorrupt;
    }

    if (header->request_id() == 0) {
        return FrameState::kCorrupt;
    }

    if (header->error_code() != 0) {
        body->clear();
        return FrameState::kOk;
    }

    body->assign(view.body, view.body_len);
    return FrameState::kOk;
}

// 对响应解码
FrameState decodeResponse(const std::string& frame, RpcResponseHeader* header, google::protobuf::Message* response) {
    if (response == nullptr) return FrameState::kCorrupt;

    std::string body;
    auto state = decodeResponse(frame, header, &body);
    if (state != FrameState::kOk) return state;

    if (header->error_code() != 0) {
        response->Clear();
        return FrameState::kOk;
    }

    return response->ParseFromString(body) ? FrameState::kOk : FrameState::kCorrupt;
}

std::string encodeErrorResponse(int32_t errorCode, const std::string& errorText, uint64_t requestId) {
    RpcResponseHeader header;
    header.set_request_id(requestId);
    header.set_error_code(errorCode);
    header.set_error_text(errorText);

    std::string headerStr;
    if (!header.SerializeToString(&headerStr)) {
        return {};
    }

    const size_t frameLenSize = 4 + 4 + headerStr.size();
    if (frameLenSize > kMaxFrameLen) {
        return {};
    }

    std::string frame;
    appendU32(&frame, static_cast<uint32_t>(frameLenSize));
    appendU32(&frame, static_cast<uint32_t>(headerStr.size()));
    frame += headerStr;
    return frame;
}

} // namespace rpc
