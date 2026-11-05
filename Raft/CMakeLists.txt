# ============================================================
# Raft 核心（2A 选举 + 2B 日志复制/提交）
# ============================================================
add_library(raft_core STATIC src/Raft.cc)
target_include_directories(raft_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(raft_core PUBLIC pthread)

add_executable(raft2aTest tests/raft2aTest.cpp)
target_include_directories(raft2aTest PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft2aTest PRIVATE raft_core pthread)
target_compile_options(raft2aTest PRIVATE -Wall -Wextra -Wpedantic)

add_test(NAME raft2aTest COMMAND raft2aTest)

add_executable(raft2bTest tests/raft2bTest.cpp)
target_include_directories(raft2bTest PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft2bTest PRIVATE raft_core pthread)
target_compile_options(raft2bTest PRIVATE -Wall -Wextra -Wpedantic)

add_test(NAME raft2bTest COMMAND raft2bTest)
