# ============================================================
# RPC 模块
# proto 生成 + 编解码 + Provider/Channel + 测试
# ============================================================
find_package(Protobuf REQUIRED)

set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/RpcHeader.proto
)

# 生成 *.pb.cc/h 到 ${CMAKE_CURRENT_BINARY_DIR}
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(rpc_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(rpc_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(rpc_proto PUBLIC protobuf::libprotobuf)

# ============================================================
# RPC 框架库
# ============================================================
add_library(rpc STATIC
    src/RpcCodec.cc
    src/RpcProvider.cc
    src/RpcChannel.cc)

target_include_directories(rpc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/NetLib/include
)

target_link_libraries(rpc PUBLIC rpc_proto netlib protobuf::libprotobuf pthread)

# ============================================================
# 测试
# ============================================================
add_executable(protoSmokeTest tests/protoSmokeTest.cpp)
target_link_libraries(protoSmokeTest PRIVATE rpc_proto)

add_executable(rpcCodecTest tests/rpcCodecTest.cpp)
target_link_libraries(rpcCodecTest PRIVATE rpc)

# 集成测试用的 proto
protobuf_generate_cpp(RPC_TEST_SRCS RPC_TEST_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/rpc_echo.proto)

add_library(rpc_test_proto STATIC ${RPC_TEST_SRCS} ${RPC_TEST_HDRS})
target_include_directories(rpc_test_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(rpc_test_proto PUBLIC protobuf::libprotobuf)

add_executable(rpcIntegrationTest tests/rpcIntegrationTest.cpp)
target_link_libraries(rpcIntegrationTest PRIVATE
    rpc rpc_test_proto netlib protobuf::libprotobuf pthread)

# 独立进程 Echo server：供跨进程 / 重连测试启动
add_executable(rpcEchoServer tests/rpcEchoServer.cpp)
target_link_libraries(rpcEchoServer PRIVATE
    rpc rpc_test_proto netlib protobuf::libprotobuf pthread)

# 跨进程验收：父进程启动独立 server，连续 10000 次调用 + 1MB 消息
add_executable(rpcCrossProcessTest tests/rpcCrossProcessTest.cpp)
target_link_libraries(rpcCrossProcessTest PRIVATE
    rpc rpc_test_proto netlib protobuf::libprotobuf pthread)
add_dependencies(rpcCrossProcessTest rpcEchoServer)

# 断线重连验收：杀掉 server，再在同一端口重启，复用同一个 RpcChannel
add_executable(rpcReconnectTest tests/rpcReconnectTest.cpp)
target_link_libraries(rpcReconnectTest PRIVATE
    rpc rpc_test_proto netlib protobuf::libprotobuf pthread)
add_dependencies(rpcReconnectTest rpcEchoServer)

add_test(NAME protoSmokeTest COMMAND protoSmokeTest)
add_test(NAME rpcCodecTest COMMAND rpcCodecTest)
add_test(NAME rpcIntegrationTest COMMAND rpcIntegrationTest)
add_test(NAME rpcCrossProcessTest
    COMMAND rpcCrossProcessTest $<TARGET_FILE:rpcEchoServer>)
add_test(NAME rpcReconnectTest
    COMMAND rpcReconnectTest $<TARGET_FILE:rpcEchoServer>)

set_tests_properties(rpcCrossProcessTest PROPERTIES TIMEOUT 90)
set_tests_properties(rpcReconnectTest PROPERTIES TIMEOUT 90)
