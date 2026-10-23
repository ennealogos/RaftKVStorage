cmake_minimum_required(VERSION 3.22)
project(RaftKVStorage CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

enable_testing()

# 网络库（Reactor 模式）
add_subdirectory(NetLib)

# RPC 框架与协议
add_subdirectory(RPC)

# 本地参考实现（reference/ 被 .gitignore 忽略；不存在时自动跳过）
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/reference/rpc_framework/CMakeLists.txt)
    add_subdirectory(reference/rpc_framework)
endif()
