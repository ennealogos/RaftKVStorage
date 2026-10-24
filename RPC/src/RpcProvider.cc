#include "RpcProvider.h"

#include "Buffer.h"
#include "Logger.h"
#include "TcpConnection.h"
#include "RpcCodec.h"
#include "rpc_header.pb.h"

#include <google/protobuf/stubs/callback.h>

#include <memory.h>

RpcProvider::RpcProvider(EventLoop* loop, const InetAddress& listenAddr, const std::string& name)
: server_(loop, listenAddr, name) {
    server_.setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { this->onConnetion(conn); }
    );

    server_.setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp ts) {
            this->onMessage(conn, buf, ts);
        }
    );
}

// 根据给定的 service，建立 service_name -> ServiceInfo 的映射表
void RpcProvider::notifyService(google::protobuf::Service* service) {
    if (service == nullptr) {
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
    server_.start();
}

void RpcProvider::onConnetion(const TcpConnectionPtr& conn) {
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
    rpc::RpcHeader header;
    std::string args;

    // 尝试解码请求
    if (!rpc::decodeRequest(frame, &header, &args)) {
        // 解码失败，丢弃该帧
        LOG_ERROR << "RpcProvider: decodeRequest failed, drop frame.";
        return;
    }

    // 根据请求帧的头部中存储的 service_name 分发给对应的服务
    auto serviceIt = services_.find(header.service_name());
    if (serviceIt == services_.end()) {
        // 没找到对应的服务
        LOG_ERROR << "RpcProvider: unknown service [" << header.service_name() << "]";
        return;
    }

    auto methodIt = serviceIt->second.methods.find(header.method_name());
    if (methodIt == serviceIt->second.methods.end()) {
        LOG_ERROR << "RpcProvider: unknown method [" << header.service_name()
                  << "." << header.method_name() << "]";
        return;
    }
    // 找到对应的 service 和 method
    google::protobuf::Service* service = serviceIt->second.service;
    const google::protobuf::MethodDescriptor* method = methodIt->second;

    // 根据 proto_type 创建正确的消息类型，然后从字符串 args 中反序列化出正确的请求对象
    std::unique_ptr<google::protobuf::Message> request(service->GetRequestPrototype(method).New());
    if (!request->ParseFromString(args)) {
        LOG_ERROR << "RpcProvider: parse request failed";
        return;
    }

    // response 由 sendResponse 释放
    // 创建正确消息类型的响应
    google::protobuf::Message* response = service->GetResponsePrototype(method).New();

    google::protobuf::Closure* done = google::protobuf::NewCallback(this, &RpcProvider::sendResponse, conn, response);
    service->CallMethod(method, nullptr, request.get(), response, done);
}

void RpcProvider::sendResponse(TcpConnectionPtr conn, google::protobuf::Message* response) {
    std::unique_ptr<google::protobuf::Message> guard(response);

    std::string frame = rpc::encodeResponse(*response);
    if (frame.empty()) {
        LOG_ERROR << "RpcProvider::SendResponse - encodeResponse failed";
        return;
    }

    // onMessage 在 IO 线程中执行，这里直接 send 是线程安全的
    conn->send(frame);
}