// 独立进程的 Echo RPC server，供跨进程 / 重连验收测试启动。
#include "EventLoopThread.h"
#include "InetAddress.h"
#include "Logger.h"
#include "RpcProvider.h"
#include "rpc_echo.pb.h"

#include <cstdlib>
#include <iostream>
#include <unistd.h>

namespace {

class EchoServiceImpl : public rpctest::EchoService {
public:
    void Echo(google::protobuf::RpcController*,
              const rpctest::EchoRequest* request,
              rpctest::EchoResponse* response,
              google::protobuf::Closure* done) override {
        response->set_message("echo: " + request->message());
        if (done != nullptr) {
            done->Run();
        }
    }
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <port>" << std::endl;
        return 2;
    }

    int port = std::atoi(argv[1]);
    if (port <= 0 || port > 65535) {
        std::cerr << "invalid port: " << argv[1] << std::endl;
        return 2;
    }

    // 测试只保留警告以上日志，避免 1 万次调用产生巨量输出。
    setLogLevel(WARN);

    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();

    EchoServiceImpl echoService;
    RpcProvider provider(serverLoop,
                         InetAddress(static_cast<uint16_t>(port), "127.0.0.1"),
                         "RpcEchoServer",
                         4);
    provider.notifyService(&echoService);
    provider.start();

    std::cout << "SERVER_READY " << port << std::endl;
    std::cout.flush();

    // 父测试进程会通过 SIGKILL 停止本进程。
    for (;;) {
        ::pause();
    }
}
