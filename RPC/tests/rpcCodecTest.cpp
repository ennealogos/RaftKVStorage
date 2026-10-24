// RPC 编解码测试
// 覆盖：
//   1. 请求/响应往返
//   2. 空响应体（frame_len == 4）
//   3. 半包 / 粘包 / 部分帧
//   4. 损坏的 frame_len / header_len / args_size
//   5. 1MB 大消息
#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

#include "Buffer.h"
#include "raft.pb.h"
#include "RpcCodec.h"
#include "RpcHeader.pb.h"

namespace {

int g_checks = 0;
int g_failures = 0;

#define CHECK(cond)                                                     \
    do {                                                                \
        ++g_checks;                                                     \
        if (!(cond)) {                                                  \
            std::cerr << "FAILED: " #cond " at " << __FILE__ << ":"     \
                      << __LINE__ << std::endl;                         \
            ++g_failures;                                               \
        }                                                               \
    } while (0)

void appendU32(std::string* out, uint32_t v) {
    uint32_t be = htonl(v);
    out->append(reinterpret_cast<const char*>(&be), sizeof(be));
}

uint32_t readU32(const char* p) {
    uint32_t be = 0;
    std::memcpy(&be, p, sizeof(be));
    return ntohl(be);
}

void writeU32(std::string* s, size_t offset, uint32_t v) {
    uint32_t be = htonl(v);
    std::memcpy(&(*s)[offset], &be, sizeof(be));
}

const google::protobuf::MethodDescriptor* raftMethod(const std::string& name) {
    return raftrpc::RaftService::descriptor()->FindMethodByName(name);
}

raftrpc::RequestVoteArgs makeRequestVoteArgs() {
    raftrpc::RequestVoteArgs args;
    args.set_term(3);
    args.set_candidate_id(2);
    args.set_last_log_index(10);
    args.set_last_log_term(2);
    return args;
}

// ------------------------------------------------------------------
// 1. 请求往返
// ------------------------------------------------------------------
void testRequestRoundTrip() {
    const google::protobuf::MethodDescriptor* method = raftMethod("RequestVote");
    CHECK(method != nullptr);

    std::string frame = rpc::encodeRequest(method, makeRequestVoteArgs());
    CHECK(!frame.empty());
    CHECK(frame.size() >= 8);
    CHECK(readU32(frame.data()) == frame.size());  // frame_len 含自身
    CHECK(readU32(frame.data() + 4) <= frame.size() - 8);

    Buffer buffer;
    buffer.append(frame);

    std::string taken;
    CHECK(rpc::tryTakeFrame(&buffer, &taken));
    CHECK(taken == frame);
    CHECK(buffer.readableBytes() == 0);

    rpc::RpcRequestHeader header;
    std::string args;
    CHECK(rpc::decodeRequest(taken, &header, &args));
    CHECK(header.service_name() == "RaftService");
    CHECK(header.method_name() == "RequestVote");
    CHECK(header.args_size() == args.size());

    raftrpc::RequestVoteArgs decoded;
    CHECK(decoded.ParseFromString(args));
    CHECK(decoded.term() == 3);
    CHECK(decoded.candidate_id() == 2);
    CHECK(decoded.last_log_index() == 10);
    CHECK(decoded.last_log_term() == 2);
}

// ------------------------------------------------------------------
// 2. 响应往返
// ------------------------------------------------------------------
void testResponseRoundTrip() {
    raftrpc::RequestVoteReply reply;
    reply.set_term(5);
    reply.set_vote_granted(true);

    std::string frame = rpc::encodeResponse(reply);
    CHECK(frame.size() > 4);
    CHECK(readU32(frame.data()) == frame.size());

    Buffer buffer;
    buffer.append(frame);

    std::string taken;
    CHECK(rpc::tryTakeFrame(&buffer, &taken));
    CHECK(taken == frame);

    raftrpc::RequestVoteReply decoded;
    CHECK(rpc::decodeResponse(taken, &decoded));
    CHECK(decoded.term() == 5);
    CHECK(decoded.vote_granted());
}

// ------------------------------------------------------------------
// 3. 空响应体（proto3 默认值不序列化，body 为 0 字节）
// ------------------------------------------------------------------
void testEmptyResponse() {
    raftrpc::RequestVoteReply reply;  // term=0, vote_granted=false
    std::string body;
    CHECK(reply.SerializeToString(&body));
    CHECK(body.empty());

    std::string frame = rpc::encodeResponse(reply);
    CHECK(frame.size() == 4);
    CHECK(readU32(frame.data()) == 4);

    Buffer buffer;
    buffer.append(frame);

    std::string taken;
    CHECK(rpc::tryTakeFrame(&buffer, &taken));
    CHECK(taken.size() == 4);

    raftrpc::RequestVoteReply decoded;
    CHECK(rpc::decodeResponse(taken, &decoded));
    CHECK(decoded.term() == 0);
    CHECK(!decoded.vote_granted());
}

// ------------------------------------------------------------------
// 4. 半包：每次只 append 1 字节
// ------------------------------------------------------------------
void testHalfPacket() {
    std::string frame = rpc::encodeRequest(raftMethod("RequestVote"), makeRequestVoteArgs());
    Buffer buffer;

    int takenCount = 0;
    for (size_t i = 0; i < frame.size(); ++i) {
        buffer.append(frame.data() + i, 1);
        std::string taken;
        if (rpc::tryTakeFrame(&buffer, &taken)) {
            ++takenCount;
            CHECK(taken == frame);
        }
    }
    CHECK(takenCount == 1);
    CHECK(buffer.readableBytes() == 0);
}

// ------------------------------------------------------------------
// 5. 粘包：一次 append 两个完整帧
// ------------------------------------------------------------------
void testStickyPacket() {
    std::string f1 = rpc::encodeRequest(raftMethod("RequestVote"), makeRequestVoteArgs());
    std::string f2 = rpc::encodeRequest(raftMethod("AppendEntries"), raftrpc::AppendEntriesArgs());

    Buffer buffer;
    buffer.append(f1);
    buffer.append(f2);

    std::string t1;
    std::string t2;
    std::string t3;
    CHECK(rpc::tryTakeFrame(&buffer, &t1));
    CHECK(rpc::tryTakeFrame(&buffer, &t2));
    CHECK(!rpc::tryTakeFrame(&buffer, &t3));
    CHECK(t1 == f1);
    CHECK(t2 == f2);
    CHECK(buffer.readableBytes() == 0);
}

// ------------------------------------------------------------------
// 6. 一个完整帧 + 下一个帧的一部分
// ------------------------------------------------------------------
void testPartialTail() {
    std::string f1 = rpc::encodeRequest(raftMethod("RequestVote"), makeRequestVoteArgs());
    std::string f2 = rpc::encodeRequest(raftMethod("AppendEntries"), raftrpc::AppendEntriesArgs());

    Buffer buffer;
    buffer.append(f1);
    buffer.append(f2.data(), 5);

    std::string taken;
    CHECK(rpc::tryTakeFrame(&buffer, &taken));
    CHECK(taken == f1);
    CHECK(!rpc::tryTakeFrame(&buffer, &taken));  // f2 还没收全
    CHECK(buffer.readableBytes() == 5);

    buffer.append(f2.data() + 5, f2.size() - 5);
    CHECK(rpc::tryTakeFrame(&buffer, &taken));
    CHECK(taken == f2);
    CHECK(buffer.readableBytes() == 0);
}

// ------------------------------------------------------------------
// 7. 损坏的 frame_len：小于 4
// ------------------------------------------------------------------
void testCorruptSmallFrameLen() {
    std::string bad;
    appendU32(&bad, 3);
    bad += "abc";

    Buffer buffer;
    buffer.append(bad);

    std::string taken;
    CHECK(!rpc::tryTakeFrame(&buffer, &taken));
    CHECK(buffer.readableBytes() == bad.size());  // 失败时不消费
}

// ------------------------------------------------------------------
// 8. 超大 frame_len：超过 64MB 上限
// ------------------------------------------------------------------
void testOversizedFrameLen() {
    std::string bad;
    appendU32(&bad, 64u * 1024 * 1024 + 1);

    Buffer buffer;
    buffer.append(bad);

    std::string taken;
    CHECK(!rpc::tryTakeFrame(&buffer, &taken));
    CHECK(buffer.readableBytes() == 4);
}

// ------------------------------------------------------------------
// 9. header_len 超过帧内剩余长度
// ------------------------------------------------------------------
void testHeaderLenExceedsFrame() {
    std::string frame = rpc::encodeRequest(raftMethod("RequestVote"), makeRequestVoteArgs());
    std::string bad = frame;
    writeU32(&bad, 4, static_cast<uint32_t>(bad.size()));  // header_len 过大

    rpc::RpcRequestHeader header;
    std::string args;
    CHECK(!rpc::decodeRequest(bad, &header, &args));
}

// ------------------------------------------------------------------
// 10. 帧头 frame_len 与实际帧大小不一致
// ------------------------------------------------------------------
void testFrameLenMismatch() {
    std::string frame = rpc::encodeRequest(raftMethod("RequestVote"), makeRequestVoteArgs());
    std::string bad = frame;
    writeU32(&bad, 0, static_cast<uint32_t>(bad.size() + 4));

    rpc::RpcRequestHeader header;
    std::string args;
    CHECK(!rpc::decodeRequest(bad, &header, &args));

    std::string responseFrame = rpc::encodeResponse(raftrpc::RequestVoteReply());
    std::string badResponse = responseFrame;
    writeU32(&badResponse, 0, static_cast<uint32_t>(badResponse.size() + 4));

    raftrpc::RequestVoteReply reply;
    CHECK(!rpc::decodeResponse(badResponse, &reply));
}

// ------------------------------------------------------------------
// 11. RpcRequestHeader.args_size 与实际 args 长度不一致
// ------------------------------------------------------------------
void testArgsSizeMismatch() {
    rpc::RpcRequestHeader header;
    header.set_service_name("RaftService");
    header.set_method_name("RequestVote");
    header.set_args_size(999);  // 故意不一致

    std::string headerStr;
    CHECK(header.SerializeToString(&headerStr));

    std::string body = "short";
    std::string frame;
    appendU32(&frame, static_cast<uint32_t>(4 + 4 + headerStr.size() + body.size()));
    appendU32(&frame, static_cast<uint32_t>(headerStr.size()));
    frame += headerStr;
    frame += body;

    rpc::RpcRequestHeader outHeader;
    std::string outArgs;
    CHECK(!rpc::decodeRequest(frame, &outHeader, &outArgs));
}

// ------------------------------------------------------------------
// 12. 1MB 大消息
// ------------------------------------------------------------------
void testLargeRequest() {
    raftrpc::AppendEntriesArgs request;
    request.set_term(9);
    request.set_leader_id(1);

    std::string big(1024 * 1024, 'x');
    raftrpc::LogEntry* entry = request.add_entries();
    entry->set_term(9);
    entry->set_index(1);
    entry->set_command(big);

    std::string frame = rpc::encodeRequest(raftMethod("AppendEntries"), request);
    CHECK(!frame.empty());
    CHECK(frame.size() > big.size());

    Buffer buffer;
    buffer.append(frame);

    std::string taken;
    CHECK(rpc::tryTakeFrame(&buffer, &taken));

    rpc::RpcRequestHeader header;
    std::string args;
    CHECK(rpc::decodeRequest(taken, &header, &args));
    CHECK(header.service_name() == "RaftService");
    CHECK(header.method_name() == "AppendEntries");

    raftrpc::AppendEntriesArgs decoded;
    CHECK(decoded.ParseFromString(args));
    CHECK(decoded.entries_size() == 1);
    CHECK(decoded.entries(0).command() == big);
}

// ------------------------------------------------------------------
// 13. 响应帧太短
// ------------------------------------------------------------------
void testDecodeResponseTooShort() {
    std::string bad = "abc";
    raftrpc::RequestVoteReply reply;
    CHECK(!rpc::decodeResponse(bad, &reply));
}

}  // namespace

int main() {
    testRequestRoundTrip();
    testResponseRoundTrip();
    testEmptyResponse();
    testHalfPacket();
    testStickyPacket();
    testPartialTail();
    testCorruptSmallFrameLen();
    testOversizedFrameLen();
    testHeaderLenExceedsFrame();
    testFrameLenMismatch();
    testArgsSizeMismatch();
    testLargeRequest();
    testDecodeResponseTooShort();

    if (g_failures == 0) {
        std::cout << "ALL TESTS PASSED (" << g_checks << " checks)" << std::endl;
        return 0;
    }

    std::cerr << g_failures << " / " << g_checks << " checks FAILED" << std::endl;
    return 1;
}
