find_package(GTest REQUIRED)
find_package(Arrow REQUIRED)
include_directories(${Arrow_INCLUDE_DIRS})

if (WITH_ASAN STREQUAL "True")
  set(CMAKE_CXX_FLAGS
    "-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -g -fsanitize=address ${CMAKE_CXX_FLAGS}"
  )
endif()

file(GLOB_RECURSE MILVUS_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*.cpp")

# Remove s3_test.cpp from the main test suite
list(REMOVE_ITEM MILVUS_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/s3_test_direct_io.cpp")
list(REMOVE_ITEM MILVUS_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/s3_test_file_stream.cpp")
list(REMOVE_ITEM MILVUS_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/s3_test_memory_buffer.cpp")
list(REMOVE_ITEM MILVUS_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/s3_test_memory_to_disk.cpp")
list(REMOVE_ITEM MILVUS_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/s3_test_nullstream_vs_memory.cpp")

add_executable(milvus_test ${MILVUS_TEST_SOURCES})

target_include_directories(milvus_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

set_target_properties(milvus_test PROPERTIES
  CXX_STANDARD 17
)

target_link_libraries(
  milvus_test PRIVATE milvus-storage GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(milvus_test)

# Create separate executable for s3_test.cpp
# add_executable(s3_test ${PROJECT_SOURCE_DIR}/test/s3_test.cpp)
# target_link_libraries(s3_test PRIVATE AWS::aws-sdk-cpp-identity-management AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-s3-crt)
# set_target_properties(s3_test PROPERTIES
#   CXX_STANDARD 17
# )

# add_executable(s3_test_memory_buffer ${PROJECT_SOURCE_DIR}/test/s3_test_memory_buffer.cpp)
# target_link_libraries(s3_test_memory_buffer PRIVATE AWS::aws-sdk-cpp-identity-management AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-s3-crt)
# set_target_properties(s3_test_memory_buffer PROPERTIES
#   CXX_STANDARD 17
# )

# add_executable(s3_test_memory_to_disk ${PROJECT_SOURCE_DIR}/test/s3_test_memory_to_disk.cpp)
# target_link_libraries(s3_test_memory_to_disk PRIVATE AWS::aws-sdk-cpp-identity-management AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-s3-crt)
# set_target_properties(s3_test_memory_to_disk PROPERTIES
#   CXX_STANDARD 17
# )

# add_executable(s3_test_file_stream ${PROJECT_SOURCE_DIR}/test/s3_test_file_stream.cpp)
# target_link_libraries(s3_test_file_stream PRIVATE AWS::aws-sdk-cpp-identity-management AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-s3-crt)
# set_target_properties(s3_test_file_stream PROPERTIES
#   CXX_STANDARD 17
# )

# add_executable(s3_test_direct_io ${PROJECT_SOURCE_DIR}/test/s3_test_direct_io.cpp)
# target_link_libraries(s3_test_direct_io PRIVATE AWS::aws-sdk-cpp-identity-management AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-s3-crt)
# set_target_properties(s3_test_direct_io PROPERTIES
#   CXX_STANDARD 17
# )

# add_executable(s3_test_nullstream_vs_memory ${PROJECT_SOURCE_DIR}/test/s3_test_nullstream_vs_memory.cpp)
# target_link_libraries(s3_test_nullstream_vs_memory PRIVATE AWS::aws-sdk-cpp-identity-management AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-s3-crt)
# set_target_properties(s3_test_nullstream_vs_memory PROPERTIES
#   CXX_STANDARD 17
# )


