foreach(bench throughput_bench padding_bench latency_bench)
    add_executable(${bench} ${bench}.cpp)
    target_link_libraries(${bench} PRIVATE lfqueue Threads::Threads)
endforeach()
