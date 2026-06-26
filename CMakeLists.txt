cmake_minimum_required(VERSION 3.16)

project(lfqueue VERSION 0.1.0 LANGUAGES CXX)

add_library(lfqueue INTERFACE)
target_compile_features(lfqueue INTERFACE cxx_std_17)
target_include_directories(
    lfqueue
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

if(MSVC)
    target_compile_options(lfqueue INTERFACE /W4 /permissive-)
else()
    target_compile_options(lfqueue INTERFACE -Wall -Wextra -Wpedantic)
endif()

find_package(Threads REQUIRED)

include(CTest)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

add_subdirectory(benchmarks)
