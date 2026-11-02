// 跨进程验收测试：
//   1. 父进程通过 posix_spawn 启动独立的 Echo server 进程；
//   2. 父进程通过 RpcChannel 调用另一个进程中的服务；
//   3. 覆盖 1MB 大消息 + 连续 10000 次同步调用。
#include "EventLoop.h"
#include "EventLoopThread.h"
#include "Logger.h"
#include "RpcChannel.h"
#include "rpcTestUtil.h"

#include <iostream>
#include <memory>
#include <string>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <rpcEchoServer-binary>" << std::endl;
        return 2;
    }

    setLogLevel(WARN);

    uint16_t port = rpctestutil::findFreePort();
    if (port == 0) {
        std::cerr << "CROSS_PROCESS_TEST_FAILED: cannot find a free port" << std::endl;
        return 1;
    }

    rpctestutil::ServerProcess server(argv[1], port);
    if (!server.valid()) {
        std::cerr << "CROSS_PROCESS_TEST_FAILED: cannot start server process" << std::endl;
        return 1;
    }

    if (!rpctestutil::waitForPort(port, 5000)) {
        std::cerr << "CROSS_PROCESS_TEST_FAILED: server port did not become ready"
                  << std::endl;
        return 1;
    }

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();

    auto channel = std::make_shared<RpcChannel>(
        clientLoop, "127.0.0.1", port, 1000);
    rpctestutil::EchoClient client(channel.get());

    std::string error;
    if (!rpctestutil::waitForEcho(&client, 40, 100, &error)) {
        std::cerr << "CROSS_PROCESS_TEST_FAILED: server not ready: " << error << std::endl;
        return 1;
    }

    // 大消息跨进程传输
    std::string big(1024 * 1024, 'x');
    std::string out;
    if (!client.echo(big, &out, &error) || out != "echo: " + big) {
        std::cerr << "CROSS_PROCESS_TEST_FAILED: 1MB echo failed: " << error << std::endl;
        return 1;
    }

    // 连续同步调用：验证长度前缀、粘包/半包处理在真实进程间持续正确
    constexpr int kCalls = 10000;
    for (int i = 0; i < kCalls; ++i) {
        std::string message = "seq-" + std::to_string(i);
        if (!client.echo(message, &out, &error) || out != "echo: " + message) {
            std::cerr << "CROSS_PROCESS_TEST_FAILED at call " << i
                      << ": " << error << std::endl;
            return 1;
        }
    }

    channel.reset();
    clientLoop->quit();
    clientThread.quit();
    server.stop();

    std::cout << "CROSS_PROCESS_TEST_PASSED (" << kCalls << " calls + 1MB message)"
              << std::endl;
    return 0;
}
