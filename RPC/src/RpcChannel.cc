#include "RpcChannel.h"

#include "Buffer.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Logger.h"
#include "TcpConnection.h"
#include "RpcCodec.h"
#include "RpcHeader.pb.h"

#include <chrono>

RpcChannel::RpcChannel(EventLoop* loop, const std::string& ip, uint16_t port, int timeoutMs)
: loop_(loop), timeoutMs_(timeoutMs) {
    // 创建 TCP 客户端
    client_ = std::make_shared<TcpClient>(loop_, InetAddress(port, ip), "rpc-channel");
    // 设置回调函数
    client_->setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { onConnection(conn); }
    ); // 设置为 RpcChannel 中定义的回调函数
    client_->setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            onMessage(conn, buf, ts);
        }
    );

    // 构造时就发起连接，TcpClient 内部会自动重连
    client_->connect();
}

RpcChannel::~RpcChannel() {
    // 先让 client 停下来
    if (client_) {
        client_->stop();
    }
    failAllPending("Channel destroyed!");
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure*) {
    // 失败时执行的函数
    auto fail = [controller](const std::string& reason) {
        if (controller != nullptr) {
            controller->SetFailed(reason);
        }
    };

    if (method == nullptr || request == nullptr || response == nullptr) {
        fail("invalid argument");
        return;
    }

    // 1. 等待连接
    std::string connErr;
    if (!ensureConnected(timeoutMs_, &connErr)) {
        // 连接断开或重连失败
        fail(connErr);
        return;
    }

    // 2. 分配 request_id 并记录到 pending_ 中
    uint64_t requestId = nextRequestId_.fetch_add(1);
    while (requestId == 0) {  // 0 保留给错误/无效请求
        requestId = nextRequestId_.fetch_add(1);
    }
    // 创建对应的等待调用
    auto call = std::make_shared<PendingCall>();
    {
        // 局部加锁，将请求 id -> 等待的调用 的映射记录到映射表
        std::lock_guard<std::mutex> lock(pendingMtx_);
        pending_[requestId] = call;
    }

    // 3. 编码并发送
    std::string frame = rpc::encodeRequest(method, *request, requestId);
    if (frame.empty()) {
        // 编码失败，将映射表中对应的映射清除
        {
            std::lock_guard<std::mutex> lock(pendingMtx_);
            pending_.erase(requestId);
        }
        fail("encode request failed!");
        return;
    }

    // 获取对应连接
    TcpConnectionPtr conn = client_->getConnection();
    if (!conn) {
        // 获取连接失败，清除对应请求
        {
            std::lock_guard<std::mutex> lock(pendingMtx_);
            pending_.erase(requestId);
        }
        fail("no connection!");
        return;
    }

    // 在 IO 线程中发送
    conn->getLoop()->runInLoop([conn, frame]() { conn->send(frame); });

    // 4. 等待该请求的响应
    std::unique_lock<std::mutex> lock(call->mtx); // 加锁（对应的等待调用的锁）
    if (!call->cv.wait_for(lock, std::chrono::milliseconds(timeoutMs_),
                            [&call]() { return call->finished; })) {
        // 如果等待超时，call->finished 仍然没有变为 true，就说明响应丢失或者迟到
        lock.unlock();
        // 删除本请求的映射，如果是响应迟到，会因为找不到对应的映射而被丢弃
        {
            std::lock_guard<std::mutex> plock(pendingMtx_);
            pending_.erase(requestId);
        }

        fail("rpc timeout!");
        return;
    }

    // 发生了错误
    if (call->errorCode != 0) {
        fail(call->errorText);
        return;
    }

    if (!response->ParseFromString(call->body)) {
        fail("decode response failed!");
        return;
    }

    if (controller != nullptr) {
        controller->Reset();
    }
}

// 检查连接是否成功，尝试重连，出错返回错误信息
bool RpcChannel::ensureConnected(int timeoutMs, std::string* error) {
    std::unique_lock<std::mutex> lock(stateMtx_);

    if (connected_) return true;

    // 忽略上一次断开留下的错误，等待本次重连
    connectError_.clear();

    // 在指定时间内，等待 connected 为 true（说明连接重连成功了），或者出现了 connect error.
    // 否则说明连接超时
    if (!stateCv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this]() { return connected_ || !connectError_.empty(); })) {
        if (error) *error = "connect timeout!";
        return false;
    }

    // 如果连接仍未成功，说明出现了连接错误
    if (!connected_) {
        if (error) *error = connectError_.empty() ? "connect failed!" : connectError_;
        return false;
    }
    return true;
}

void RpcChannel::onConnection(const TcpConnectionPtr& conn) {
    {
        std::lock_guard<std::mutex> lock(stateMtx_);
        if (conn->connected()) { // 连接成功
            connected_ = true;
            connectError_.clear();
        } else { // 连接断开
            connected_ = false;
            connectError_ = "connection closed!";
        }
    }

    stateCv_.notify_all(); // 唤醒等待建连的线程

    if (!conn->connected()) {
        // 连接断开：唤醒所有在途请求
        failAllPending("connection closed!");
    }
}

void RpcChannel::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp) {
    while (true) {
        std::string frame;
        auto state = rpc::tryTakeFrame(buf, &frame);

        // 半包， 等待下一次 read
        if (state == rpc::FrameState::kExpectMore) {
            return;
        }
        // 出错的帧，关闭连接
        if (state == rpc::FrameState::kCorrupt) {
            LOG_ERROR << "RpcChannel: corrupt frame, closing connection!";
            conn->forceClose();
            return;
        }

        // 解码
        rpc::RpcResponseHeader header;
        std::string body;
        if (rpc::decodeResponse(frame, &header, &body) != rpc::FrameState::kOk) {
            LOG_ERROR << "RpcChannel: decode response failed, drop frame!";
            continue;
        }

        // 移除响应表中的 pending，保证一个响应只会被投递一次
        std::shared_ptr<PendingCall> call;
        {
            std::lock_guard<std::mutex> lock(pendingMtx_);
            // 在映射表中查找到达响应对应的请求
            auto it = pending_.find(header.request_id());
            if (it == pending_.end()) {
                // 在映射表中查找不到，说明是：超时迟到的响应 或 重复的响应，直接丢弃
                continue;
            }
            call = it->second;
            pending_.erase(it);
        }

        {
            // 将响应的信息转移到 call 中
            std::lock_guard<std::mutex> lock(call->mtx);
            call->finished = true;
            call->body = std::move(body);
            call->errorCode = header.error_code();
            call->errorText = header.error_text();
        }
        call->cv.notify_one(); // 唤醒被阻塞的请求
    }
}

void RpcChannel::failAllPending(const std::string& reason) {
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> calls;

    {
        std::lock_guard<std::mutex> lock(pendingMtx_);
        calls.swap(pending_);
    }

    // 注意：不要持有 pendingMtx_ 再去锁 call->mtx，避免锁顺序问题
    for (auto& item : calls) {
        const auto& call = item.second;
        {
            std::lock_guard<std::mutex> clock(call->mtx);
            if (call->finished) {
                continue;
            }
            call->finished = true;
            call->errorCode = -1;
            call->errorText = reason;
        }
        call->cv.notify_one();
    }
}