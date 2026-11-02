#pragma once

#include "RpcChannel.h"
#include "RpcController.h"
#include "rpc_echo.pb.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

extern char** environ;

namespace rpctestutil {

// 让系统分配一个空闲端口，避免写死端口导致并行测试冲突。
inline uint16_t findFreePort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return 0;
    }

    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        ::close(fd);
        return 0;
    }

    ::close(fd);
    return ntohs(addr.sin_port);
}

// 等待 server 端口可连接，避免测试一开始就产生一次 Connection refused 错误日志。
inline bool waitForPort(uint16_t port, int timeoutMs) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);

    while (std::chrono::steady_clock::now() < deadline) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd >= 0) {
            sockaddr_in addr {};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);

            int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            ::close(fd);
            if (rc == 0) {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

// 用 posix_spawn 启动独立的 Echo server 进程。
// 父进程此时可能已经有 EventLoop 线程，使用 posix_spawn 比 fork 更安全。
class ServerProcess {
public:
    ServerProcess(const std::string& executable, uint16_t port) {
        std::string portStr = std::to_string(port);
        char* argv[] = {
            const_cast<char*>(executable.c_str()),
            const_cast<char*>(portStr.c_str()),
            nullptr,
        };

        pid_t pid = -1;
        int rc = ::posix_spawn(&pid, executable.c_str(), nullptr, nullptr, argv, environ);
        if (rc != 0) {
            std::cerr << "posix_spawn failed: " << rc << std::endl;
            pid_ = -1;
        } else {
            pid_ = pid;
        }
    }

    ~ServerProcess() { stop(); }

    ServerProcess(const ServerProcess&) = delete;
    ServerProcess& operator=(const ServerProcess&) = delete;

    bool valid() const { return pid_ > 0; }

    // 返回 false 表示子进程已经退出（会被回收）。
    bool running() {
        if (pid_ <= 0) {
            return false;
        }

        int status = 0;
        pid_t ret = ::waitpid(pid_, &status, WNOHANG);
        if (ret == pid_) {
            pid_ = -1;
            return false;
        }
        return true;
    }

    void stop() {
        if (pid_ <= 0) {
            return;
        }

        ::kill(pid_, SIGKILL);
        int status = 0;
        while (::waitpid(pid_, &status, 0) < 0) {
            if (errno != EINTR) {
                break;
            }
        }
        pid_ = -1;
    }

private:
    pid_t pid_ = -1;
};

// 同步 Echo 客户端，复用同一个 stub，避免 1 万次调用时反复构造。
class EchoClient {
public:
    explicit EchoClient(RpcChannel* channel) : stub_(channel) {}

    bool echo(const std::string& message, std::string* out, std::string* error) {
        RpcController controller;
        rpctest::EchoRequest request;
        rpctest::EchoResponse response;

        request.set_message(message);
        stub_.Echo(&controller, &request, &response, nullptr);

        if (controller.Failed()) {
            if (error != nullptr) {
                *error = controller.ErrorText();
            }
            return false;
        }

        if (out != nullptr) {
            *out = response.message();
        }
        return true;
    }

private:
    rpctest::EchoService_Stub stub_;
};

// 等待 server 真正可用。server 启动和客户端连接有竞态，所以调用方重试。
inline bool waitForEcho(EchoClient* client,
                        int attempts,
                        int intervalMs,
                        std::string* lastError) {
    for (int i = 0; i < attempts; ++i) {
        std::string out;
        std::string error;
        if (client->echo("ready", &out, &error) && out == "echo: ready") {
            return true;
        }
        if (lastError != nullptr) {
            *lastError = error;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return false;
}

}  // namespace rpctestutil
