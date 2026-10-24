# ============================================================
# RPC 模块
# 当前阶段：把 proto 生成 C++ 代码，产出 rpc_proto 静态库
# 后续阶段：在此目录实现 RpcProvider / RpcChannel / RpcController
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
# RPC 框架库（当前只包含编解码器）
# ============================================================
add_library(rpc STATIC src/RpcCodec.cc)

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

add_test(NAME protoSmokeTest COMMAND protoSmokeTest)
add_test(NAME rpcCodecTest COMMAND rpcCodecTest)
