#pragma once

#include "TcpClient.h"

#include <google/protobuf/service.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// RPC 客户端
/*
底层使用 TcpClient 维护长连接，自动重试；
支持同一 channel 上多线程并发调用：request_id -> PendingCall 映射区分响应。
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
    // 记录一个发出的请求的状态，锁，响应，错误码与错误信息。
    struct PendingCall {
        std::mutex mtx;
        std::condition_variable cv;
        bool finished = false;
        std::string body; // 响应 protobuf 字节
        int32_t errorCode = 0; // 0 == OK; 负数 == 本地错误
        std::string errorText;
    }; // 每个请求拥有单独的锁

    // 检查连接是否建立，并唤醒阻塞的线程
    void onConnection(const TcpConnectionPtr& conn);
    // 从 buffer 中提取所有完整响应帧，逐个匹配 request_id 并唤醒对应调用
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp receiveTime);

    bool ensureConnected(int timeMS, std::string* error);
    // 清空映射表，将等待调用映射表中的所有 pending 状态设置为 finished = true, 设置错误码为 -1，错误信息为传入的 reason
    void failAllPending(const std::string& reason);

    EventLoop* loop_;
    std::shared_ptr<TcpClient> client_;
    int timeoutMs_;
    // 请求 id：每个 channel 独立递增，0 保留
    std::atomic<uint64_t> nextRequestId_{1};

    // request_id -> 等待中的调用
    std::mutex pendingMtx_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending_;

    // 连接状态
    mutable std::mutex stateMtx_;
    std::condition_variable stateCv_;
    bool connected_ = false;
    std::string connectError_;
};