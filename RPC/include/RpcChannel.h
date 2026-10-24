#pragma once

#include "TcpClient.h"
#include <google/protobuf/service.h>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

// RPC 客户端
/*
底层使用 TcpClient 维护长连接，自动重试；
同步调用：同一时刻只有一个在途请求；
*/
class RpcChannel : public google::protobuf::RpcChannel {
public:
    // 初始化参数，创建 TcpClient，将 TcpClient 中的连接回调和消息回调设置为 RpcChannel 中定义的两个回调
    RpcChannel(EventLoop* loop, const std::string& ip, uint16_t port, int timeoutMs = 3000);
    ~RpcChannel() override;
    
    // 序列化请求 → 通过网络发送 → 等待响应。
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;
    
private:
    // 检查连接是否建立，并唤醒阻塞的线程
    void onConnection(const TcpConnectionPtr& conn);
    // 从 buffer 中提取响应，只会提取一个，多余的忽略
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp receiveTime);

    EventLoop* loop_;
    std::shared_ptr<TcpClient> client_;
    int timeoutMs_;

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    bool connected_ = false;
    bool hasResponse_ = false;
    std::string responseFrame_;
    uint64_t nextId_; // 下一个请求的 request_id
    std::unordered_map<uint64_t, std::string> pending_; // request_id -> response 的映射
    std::string err_;
};