// RPC 集成测试：
//   1. 基本调用 / 1MB 大消息
//   2. 同一 channel 多线程并发调用
//   3. 慢请求不阻塞快请求（服务端线程池并发）
//   4. unknown service -> 服务端立即回错误响应
//   5. 响应超时 + 迟到响应不会污染后续请求
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "EventLoopThread.h"
#include "InetAddress.h"
#include "RpcChannel.h"
#include "RpcController.h"
#include "RpcProvider.h"
#include "rpc_echo.pb.h"

namespace {

std::atomic<int> g_checks{0};
std::atomic<int> g_failures{0};

#define CHECK(cond)                                                     \
    do {                                                                \
        ++g_checks;                                                     \
        if (!(cond)) {                                                  \
            std::cerr << "FAILED: " #cond " at " << __FILE__ << ":"     \
                      << __LINE__ << std::endl;                         \
            ++g_failures;                                               \
        }                                                               \
    } while (0)

class EchoServiceImpl : public rpctest::EchoService {
public:
    void Echo(google::protobuf::RpcController*,
              const rpctest::EchoRequest* request,
              rpctest::EchoResponse* response,
              google::protobuf::Closure* done) override {
        response->set_message("echo: " + request->message());
        done->Run();
    }
};

class SlowServiceImpl : public rpctest::SlowService {
public:
    void Slow(google::protobuf::RpcController*,
              const rpctest::SlowRequest* request,
              rpctest::SlowResponse* response,
              google::protobuf::Closure* done) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(request->delay_ms()));
        response->set_message("slow done");
        done->Run();
    }
};

bool callEcho(RpcChannel* channel, const std::string& message,
              std::string* out, std::string* err) {
    rpctest::EchoService_Stub stub(channel);
    RpcController controller;
    rpctest::EchoRequest request;
    rpctest::EchoResponse response;

    request.set_message(message);
    stub.Echo(&controller, &request, &response, nullptr);

    if (controller.Failed()) {
        if (err != nullptr) *err = controller.ErrorText();
        return false;
    }
    if (out != nullptr) *out = response.message();
    return true;
}

}  // namespace

int main() {
    constexpr uint16_t kPort = 19093;

    // ---------------- 服务端 ----------------
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();

    EchoServiceImpl echoService;
    SlowServiceImpl slowService;

    RpcProvider provider(serverLoop, InetAddress(kPort, "127.0.0.1"), "RpcTestServer", 4);
    provider.notifyService(&echoService);
    provider.notifyService(&slowService);
    provider.start();

    // ---------------- 客户端 ----------------
    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();

    {
        auto channel = std::make_shared<RpcChannel>(
            clientLoop, "127.0.0.1", kPort, 3000);

        // 1) 基本调用
        {
            std::string out;
            std::string err;
            CHECK(callEcho(channel.get(), "hello", &out, &err));
            CHECK(out == "echo: hello");
        }

        // 2) 1MB 大消息
        {
            std::string big(1024 * 1024, 'x');
            std::string out;
            std::string err;
            CHECK(callEcho(channel.get(), big, &out, &err));
            CHECK(out == "echo: " + big);
        }

        // 3) 同一 channel 多线程并发
        {
            constexpr int kThreads = 8;
            constexpr int kCalls = 50;
            std::atomic<int> okCount{0};
            std::atomic<int> failCount{0};
            std::mutex failMtx;
            std::string firstError;

            std::vector<std::thread> threads;
            for (int t = 0; t < kThreads; ++t) {
                threads.emplace_back([&, t]() {
                    for (int i = 0; i < kCalls; ++i) {
                        std::string msg = "t" + std::to_string(t) + "-" + std::to_string(i);
                        std::string out;
                        std::string err;
                        if (callEcho(channel.get(), msg, &out, &err) &&
                            out == "echo: " + msg) {
                            ++okCount;
                        } else {
                            ++failCount;
                            std::lock_guard<std::mutex> lock(failMtx);
                            if (firstError.empty()) {
                                firstError = err.empty()
                                    ? ("unexpected response: " + out)
                                    : err;
                            }
                        }
                    }
                });
            }
            for (auto& th : threads) th.join();
            CHECK(okCount.load() == kThreads * kCalls);
            CHECK(failCount.load() == 0);
            if (!firstError.empty()) {
                std::cerr << "first concurrent error: " << firstError << std::endl;
            }
        }

        // 4) 慢请求不阻塞快请求：服务端由线程池并发处理
        {
            std::atomic<bool> slowDone{false};
            std::atomic<bool> fastDone{false};

            std::thread slowThread([&]() {
                rpctest::SlowService_Stub stub(channel.get());
                RpcController controller;
                rpctest::SlowRequest request;
                rpctest::SlowResponse response;
                request.set_delay_ms(400);
                stub.Slow(&controller, &request, &response, nullptr);
                CHECK(!controller.Failed());
                CHECK(response.message() == "slow done");
                slowDone.store(true);
            });

            std::this_thread::sleep_for(std::chrono::milliseconds(30));

            auto fastStart = std::chrono::steady_clock::now();
            std::string out;
            CHECK(callEcho(channel.get(), "fast", &out, nullptr));
            CHECK(out == "echo: fast");
            auto fastElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - fastStart).count();

            CHECK(fastElapsed < 250);  // 远小于慢请求的 400ms
            CHECK(!slowDone.load());   // 快请求返回时慢请求还在执行
            fastDone.store(true);

            slowThread.join();
            CHECK(slowDone.load());
            CHECK(fastDone.load());
        }

        // 5) unknown service：服务端回错误响应，客户端立即失败
        {
            rpctest::UnknownService_Stub stub(channel.get());
            RpcController controller;
            rpctest::UnknownRequest request;
            rpctest::UnknownResponse response;

            auto start = std::chrono::steady_clock::now();
            stub.Call(&controller, &request, &response, nullptr);
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();

            CHECK(controller.Failed());
            CHECK(controller.ErrorText().find("unknown service") != std::string::npos);
            CHECK(elapsed < 1000);  // 不应该等满 3s 超时
        }

        // 6) 超时 + 迟到响应：不污染后续请求
        {
            auto slowChannel = std::make_shared<RpcChannel>(
                clientLoop, "127.0.0.1", kPort, 100);  // 100ms 超时

            {
                rpctest::SlowService_Stub stub(slowChannel.get());
                RpcController controller;
                rpctest::SlowRequest request;
                rpctest::SlowResponse response;
                request.set_delay_ms(500);

                stub.Slow(&controller, &request, &response, nullptr);
                CHECK(controller.Failed());
                CHECK(controller.ErrorText().find("timeout") != std::string::npos);
            }

            // 慢响应稍后才会到达；此时同一个 channel 再发一个 Echo，
            // 迟到的 Slow 响应必须按 request_id 被丢弃。
            std::string out;
            std::string err;
            CHECK(callEcho(slowChannel.get(), "after-timeout", &out, &err));
            CHECK(out == "echo: after-timeout");
        }
    }

    // 先停掉两个 EventLoop 线程，再让 RpcProvider / EventLoopThread 析构，
    // 避免 TcpServer 的资源析构与 IO 线程并发。
    clientLoop->quit();
    clientThread.quit();
    serverLoop->quit();
    serverThread.quit();

    if (g_failures.load() == 0) {
        std::cout << "ALL RPC INTEGRATION TESTS PASSED (" << g_checks.load()
                  << " checks)" << std::endl;
        return 0;
    }
    std::cerr << g_failures.load() << " / " << g_checks.load()
              << " checks FAILED" << std::endl;
    return 1;
}
