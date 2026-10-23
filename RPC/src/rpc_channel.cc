#include "rpc_channel.h"

#include "Buffer.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Logger.h"
#include "TcpConnection.h"
#include "rpc_codec.h"

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
}

RpcChannel::~RpcChannel() {
    // 先让 client 停下来
    if (client_) {
        client_->stop();
    }
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure*) {
    auto fail = [controller](const std::string& reason) {
        if (controller != nullptr) {
            controller->SetFailed(reason);
        }
    };

    std::unique_lock<std::mutex> lock(mtx_);

    // 1. 确保连接已建立
    if (!connected_) {
        // 如果上一次超时/断线的连接还在关闭过程中，先等他彻底关闭
        if (client_->getConnection()) {
            cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs_),
                        [this]() { return client_->getConnection() == nullptr; });
        }

        err_.clear(); // 情况错误信息

        // client 没有连接了，重新建立连接
        if (!client_->getConnection()) {
            // connect() 函数内部会 runInLoop，先释放锁，避免同线程回调陷入思索
            lock.unlock();
            client_->connect();
            lock.lock();
        }

        // 等待连接建立成功，超时调用 fail() 函数
        if (!cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs_),
                        [this]() { return connected_ || !err_.empty(); })) {
            fail("connect timeout!");
            return ;
        }

        // 再次检查连接是否建立成功
        if (!connected_) {
            fail(err_.empty() ? "connnect failed!" : err_);
            return;
        }
    }

    // 2. 编码
    std::string frame = rpc::encodeRequest(method, *request);

    // 编码失败
    if (frame.empty()) {
        fail("encode request failed!");
        return;
    }

    // 初始化响应载体
    hasResponse_ = false;
    responseFrame_.clear();
    err_.clear();

    TcpConnectionPtr conn = client_->getConnection();
    // 检查连接是否还在
    if (!conn) {
        connected_ = false;
        fail("no connection!");
        return;
    }

    // 3. 发送
    // 通过 runInLoop 在连接所属的 IO 线程中发送
    conn->getLoop()->runInLoop([conn, frame]() { conn->send(frame); });

    // 4. 等待响应
    if (!cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs_),
                    [this]() { return hasResponse_ || !err_.empty(); })) {
        connected_ = false;
        lock.unlock();

        // 超时后断开连接，丢弃可能已经迟到的响应，避免污染下一次调用
        client_->disconnect();
        fail("rpc timeout!");
        return;
    }

    if (!err_.empty()) {
        fail(err_);
        return;
    }

    // 5. 解码
    if (!rpc::decodeResponse(responseFrame_, response)) {
        fail("decode response failed!");
        return;
    }

    if (controller != nullptr) {
        controller->Reset();
    }
}

void RpcChannel::onConnection(const TcpConnectionPtr& conn) {
    std::lock_guard<std::mutex> lock(mtx_);

    if (conn->connected()) {
        connected_ = true;
        err_.clear();
    } else {
        connected_ = false;
        err_ = "connection closed!";
    }

    cv_.notify_all(); // 唤醒阻塞的所有线程
}

void RpcChannel::onMessage(const TcpConnectionPtr& , Buffer* buf, Timestamp) {
    std::string frame;
    while (rpc::tryTakeFrame(buf, &frame)) {
        std::lock_guard<std::mutex> lock(mtx_);

        // 同步模型：同一时刻只有一个在途请求；多余响应直接忽略
        if (!hasResponse_) {
            responseFrame_ = frame;
            hasResponse_ = true;
            cv_.notify_all();
        } else {
            LOG_ERROR << "RpcChannel: unexpected extra response, dropped";
        }
    }
}