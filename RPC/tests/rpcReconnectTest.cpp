// 断线重连验收测试：
//   1. 启动独立 Echo server 进程，确认可以调用；
//   2. kill server，确认在途连接被感知、调用失败；
//   3. 在同一端口重启 server，客户端 TcpClient 自动重连；
//   4. 不重建 RpcChannel，再次调用成功。
#include "EventLoop.h"
#include "EventLoopThread.h"
#include "Logger.h"
#include "RpcChannel.h"
#include "rpcTestUtil.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <rpcEchoServer-binary>" << std::endl;
        return 2;
    }

    setLogLevel(WARN);

    uint16_t port = rpctestutil::findFreePort();
    if (port == 0) {
        std::cerr << "RECONNECT_TEST_FAILED: cannot find a free port" << std::endl;
        return 1;
    }

    rpctestutil::ServerProcess server(argv[1], port);
    if (!server.valid()) {
        std::cerr << "RECONNECT_TEST_FAILED: cannot start server process" << std::endl;
        return 1;
    }

    if (!rpctestutil::waitForPort(port, 5000)) {
        std::cerr << "RECONNECT_TEST_FAILED: server port did not become ready" << std::endl;
        return 1;
    }

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();

    auto channel = std::make_shared<RpcChannel>(
        clientLoop, "127.0.0.1", port, 300);
    rpctestutil::EchoClient client(channel.get());

    std::string error;
    if (!rpctestutil::waitForEcho(&client, 40, 100, &error)) {
        std::cerr << "RECONNECT_TEST_FAILED: server not ready: " << error << std::endl;
        return 1;
    }

    std::string out;
    if (!client.echo("before-kill", &out, &error) || out != "echo: before-kill") {
        std::cerr << "RECONNECT_TEST_FAILED: initial echo failed: " << error << std::endl;
        return 1;
    }

    // 杀掉 server，等待同一个 channel 感知到失败。
    server.stop();

    bool sawFailure = false;
    for (int i = 0; i < 20; ++i) {
        std::string failedOut;
        std::string failedError;
        if (!client.echo("after-kill", &failedOut, &failedError)) {
            sawFailure = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (!sawFailure) {
        std::cerr << "RECONNECT_TEST_FAILED: call still succeeded after server was killed"
                  << std::endl;
        return 1;
    }

    // 给 TcpClient 的断线回调一点时间，再在同一端口启动新 server。
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    rpctestutil::ServerProcess restartedServer(argv[1], port);
    if (!restartedServer.valid()) {
        std::cerr << "RECONNECT_TEST_FAILED: cannot restart server process" << std::endl;
        return 1;
    }

    if (!rpctestutil::waitForPort(port, 5000)) {
        std::cerr << "RECONNECT_TEST_FAILED: restarted server port did not become ready"
                  << std::endl;
        return 1;
    }

    if (!rpctestutil::waitForEcho(&client, 80, 100, &error)) {
        std::cerr << "RECONNECT_TEST_FAILED: channel did not reconnect: " << error
                  << std::endl;
        return 1;
    }

    if (!client.echo("after-restart", &out, &error) || out != "echo: after-restart") {
        std::cerr << "RECONNECT_TEST_FAILED: echo after restart failed: " << error
                  << std::endl;
        return 1;
    }

    channel.reset();
    clientLoop->quit();
    clientThread.quit();
    restartedServer.stop();

    std::cout << "RECONNECT_TEST_PASSED" << std::endl;
    return 0;
}
