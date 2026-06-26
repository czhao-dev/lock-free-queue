add_executable(correctness_test correctness_test.cpp)
target_link_libraries(correctness_test PRIVATE lfqueue Threads::Threads)

add_test(NAME correctness_test COMMAND correctness_test)
