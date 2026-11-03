# ============================================================
# Storage 模块（header-only）
# ============================================================
add_library(storage INTERFACE)
target_include_directories(storage INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(storageTest tests/storageTest.cpp)
target_link_libraries(storageTest PRIVATE storage)
target_compile_options(storageTest PRIVATE -Wall -Wextra -Wpedantic)

add_test(NAME storageTest COMMAND storageTest)
