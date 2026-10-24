#pragma once

#include "TcpServer.h"
#include <google/protobuf/service.h>
#include <string>
#include <unordered_map>

// RPC 服务端
// 维护 service_name -> ServiceInfo 的映射
// 其中 ServiceInfo 维护了 具体的 service以及method_name -> MethodDescripter 的映射

// 用 TcpServer 接收连接
// 从字节流中按帧解析请求 -> 查表 -> protobuf 反射调用 -> 回包

class RpcProvider {
public:
    // 初始化 TcpServer，设置连接和消息回调
    RpcProvider(EventLoop* loop, const InetAddress& listenAddr, const std::string& name);
    ~RpcProvider(){}

    // 注册服务，service 对象必须比 RpcProvider 活得更久
    // 根据给定的 service，建立 service_name -> ServiceInfo 的映射表
    void notifyService(google::protobuf::Service* service);

    // 启动监听，内部调用 TcpServer::start
    void start();

private:
    struct ServiceInfo {
        google::protobuf::Service* service = nullptr;
        // method_name -> MethodDescriptor 的映射
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> methods;
    };

    // 回调函数
    // 连接事件到来时，根据连接状态，打印状态信息（连接建立，连接断开）
    void onConnetion(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp receiveTime);
    // 分发消息给对应的服务
    void dispatch(const TcpConnectionPtr& conn, const std::string& frame);

    // 对响应进行编码并发送
    void sendResponse(TcpConnectionPtr conn, google::protobuf::Message* response);

    TcpServer server_;
    std::unordered_map<std::string, ServiceInfo> services_; // service_name -> 服务具体信息 的映射
};