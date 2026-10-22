cmake_minimum_required(VERSION 3.22)
project(RaftKVStorage CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 网络库（Reactor 模式）
add_subdirectory(NetLib)

# RPC 框架与协议
add_subdirectory(RPC)
