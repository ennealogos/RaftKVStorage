# ============================================================
# RPC 模块
# 当前阶段：把 proto 生成 C++ 代码，产出 rpc_proto 静态库
# 后续阶段：在此目录实现 RpcProvider / RpcChannel / RpcController
# ============================================================
find_package(Protobuf REQUIRED)

set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv.proto
)

# 生成 raft.pb.cc/h、kv.pb.cc/h 到 ${CMAKE_CURRENT_BINARY_DIR}
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(rpc_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(rpc_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(rpc_proto PUBLIC protobuf::libprotobuf)

# ============================================================
# 协议编解码冒烟测试
# ============================================================
add_executable(proto_smoke_test tests/proto_smoke_test.cpp)
target_link_libraries(proto_smoke_test PRIVATE rpc_proto)
