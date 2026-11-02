#include "RpcProvider.h"

#include "Buffer.h"
#include "Logger.h"
#include "TcpConnection.h"
#include "RpcCodec.h"
#include "RpcHeader.pb.h"

#include <google/protobuf/stubs/callback.h>

#include <atomic>
#include <exception>
#include <memory>

// ------------------------------------------------------------------
// ResponseClosure：业务方法调用 done->Run() 时执行
// 由 shared_ptr 管理生命周期（不再 delete this），
// 避免 handler 忘记调用/重复调用导致的泄漏或 double free。
// 约定：handler 必须同步调用 done->Run()（或返回前不调用，由 provider 回错误）。
// ------------------------------------------------------------------
class RpcProvider::ResponseClosure : public google::protobuf::Closure {
public:
    ResponseClosure(RpcProvider* provider,
                    TcpConnectionPtr conn,
                    uint64_t requestId,
                    std::shared_ptr<google::protobuf::Message> response)
        : provider_(provider),
          conn_(std::move(conn)),
          requestId_(requestId),
          response_(std::move(response)) {}

    void Run() override {
        if (finished_.exchange(true)) {
            return;  // 重复调用直接忽略
        }
        provider_->sendResponse(conn_, requestId_, response_.get());
    }

    bool finished() const { return finished_.load(); }

private:
    RpcProvider* provider_;
    TcpConnectionPtr conn_;
    uint64_t requestId_;
    std::shared_ptr<google::protobuf::Message> response_;
    std::atomic<bool> finished_{false};
};

RpcProvider::RpcProvider(EventLoop* loop,
                        const InetAddress& listenAddr,
                        const std::string& name,
                        size_t workerThreads)
: server_(loop, listenAddr, name), pool_(workerThreads) {
    server_.setThreadNum(2);  // IO 线程数：拆帧/解析请求头可以并行

    server_.setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { this->onConnection(conn); }
    );

    server_.setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            this->onMessage(conn, buf, ts);
        }
    );
}

RpcProvider::~RpcProvider() {
    // 先等待所有在途 handler 完成，再销毁 server_
    pool_.stop();
}

// 根据给定的 service，建立 service_name -> ServiceInfo 的映射表
void RpcProvider::notifyService(google::protobuf::Service* service) {
    if (service == nullptr) {
        return;
    }

    if (started_.load()) {
        LOG_ERROR << "RpcProvider: notifyService must be called before start()";
        return;
    }

    const google::protobuf::ServiceDescriptor* sd = service->GetDescriptor();

    if (sd == nullptr) {
        return;
    }

    ServiceInfo info;
    info.service = service;

    // 将 service 中支持的 method 都存储在 info 内对应的映射表中
    for (int i = 0; i < sd->method_count(); ++i) {
        const google::protobuf::MethodDescriptor* md = sd->method(i);
        info.methods.emplace(md->name(), md);
    }
    services_[sd->name()] = std::move(info);
    LOG_INFO << "RpcProvider: registered service [" << sd->name() << "] with "
             << services_[sd->name()].methods.size() << " method(s)";
}

void RpcProvider::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) {
        LOG_ERROR << "RpcProvider: start() called more than once";
        return;
    }
    server_.start();
}

void RpcProvider::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        LOG_INFO << "RpcProvider: connection UP [" << conn->name() << "]";
    } else {
        LOG_INFO << "RpcProvider: connection DOWN [" << conn->name() << "]";
    }
}

void RpcProvider::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp) {
    // 消息到来，可能存在半包，一个完整帧或多个数据帧的情况，循环读取，直到 buffer 中不再有完整帧
    // 读取一个帧后就分发消息
    while (true) {
        std::string frame;
        auto state = rpc::tryTakeFrame(buf, &frame);

        switch (state) {
            case rpc::FrameState::kOk:
                // tryTakeFrame 已经把这一帧从 buffer 中取走
                dispatch(conn, frame);
                continue;   // 继续尝试取下一帧

            case rpc::FrameState::kExpectMore:
                // 半包：数据留在 buffer 里，等下一次 read 事件再拼
                return;

            case rpc::FrameState::kCorrupt:
                // 长度字段非法（<4 或 >max），协议已不可恢复
                LOG_ERROR << "RpcProvider: corrupt frame, closing connection";
                // 丢弃脏数据
                buf->retrieveAll();
                // 关闭连接
                conn->forceClose();
                return;
        }
    }
}

void RpcProvider::dispatch(const TcpConnectionPtr& conn, const std::string& frame) {
    rpc::RpcRequestHeader header;
    std::string args;

    // 尝试解码请求
    if (rpc::decodeRequest(frame, &header, &args) != rpc::FrameState::kOk) {
        // 连 request_id 都无法确定，无法回送错误包；协议已损坏，直接彻底关闭
        LOG_ERROR << "RpcProvider: decodeRequest failed, closing connection";
        conn->forceClose();
        return;
    }

    const uint64_t requestId = header.request_id();

    // 根据请求帧的头部中存储的 service_name 分发给对应的服务
    auto serviceIt = services_.find(header.service_name());
    if (serviceIt == services_.end()) {
        // 没找到对应的服务
        sendError(conn, requestId, 1, "unknown service");
        return;
    }

    auto methodIt = serviceIt->second.methods.find(header.method_name());
    if (methodIt == serviceIt->second.methods.end()) {
        sendError(conn, requestId, 2, "unknown method");
        return;
    }
    // 找到对应的 service 和 method
    google::protobuf::Service* service = serviceIt->second.service;
    const google::protobuf::MethodDescriptor* method = methodIt->second;

    // 根据 proto_type 创建正确的消息类型，然后从字符串 args 中反序列化出正确的请求对象
    auto request = std::shared_ptr<google::protobuf::Message>(
        service->GetRequestPrototype(method).New());
    if (!request->ParseFromString(args)) {
        sendError(conn, requestId, 3, "parse request failed");
        return;
    }

    // 创建正确消息类型的响应
    auto response = std::shared_ptr<google::protobuf::Message>(
        service->GetResponsePrototype(method).New());

    // done 由 shared_ptr 管理：handler 忘记调用不会泄漏，重复调用会被忽略
    auto done = std::make_shared<ResponseClosure>(this, conn, requestId, response);

    // 投递到线程池：handler 并发执行，响应可以乱序返回
    bool ok = pool_.enqueue(
        [this, service, method, request, response, done, conn, requestId]() mutable {
            bool handlerOk = true;
            try {
                // 约定：handler 必须同步调用 done->Run()，或在返回前不调用
                service->CallMethod(method, nullptr, request.get(), response.get(), done.get());
            } catch (const std::exception& e) {
                handlerOk = false;
                LOG_ERROR << "RpcProvider: handler exception: " << e.what();
            } catch (...) {
                handlerOk = false;
                LOG_ERROR << "RpcProvider: handler unknown exception";
            }

            // 兜底：handler 既没抛异常也没调用 done->Run()，给客户端回错误，避免空等超时
            if (!done->finished()) {
                sendError(conn, requestId, 5,
                          handlerOk ? "handler produced no response" : "handler exception");
            }
        });

    if (!ok) {
        sendError(conn, requestId, 4, "server overloaded");
    }
}

void RpcProvider::sendResponse(TcpConnectionPtr conn,
                            uint64_t requestId,
                            google::protobuf::Message* response) {
    // 编码响应包
    std::string frame = rpc::encodeResponse(*response, 0, "", requestId);

    if (frame.empty()) {
        LOG_ERROR << "RpcProvider::sendResponse - encodeResponse failed";
        return;
    }

    // 可能在线程池线程中调用：把发送投递回连接所属 IO 线程
    conn->getLoop()->runInLoop([conn, frame = std::move(frame)]() mutable {
        if (conn->connected()) {
            conn->send(frame);
        }
    });
}

void RpcProvider::sendError(const TcpConnectionPtr& conn,
                            uint64_t requestId,
                            int32_t errorCode,
                            const std::string& errorText) {
    // 编码错误响应包
    std::string frame = rpc::encodeErrorResponse(errorCode, errorText, requestId);
    if (frame.empty()) {
        LOG_ERROR << "RpcProvider::sendError - encodeErrorResponse failed";
        return;
    }

    conn->getLoop()->runInLoop([conn, frame = std::move(frame)]() mutable {
        if (conn->connected()) {
            conn->send(frame);
        }
    });
}