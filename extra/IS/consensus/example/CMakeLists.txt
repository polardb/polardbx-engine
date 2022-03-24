cmake_minimum_required(VERSION 2.8)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -O0 -fsanitize=address -fno-omit-frame-pointer -ggdb")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -O0")
SET (EXTRA_LINK tcmalloc_and_profiler)

INCLUDE_DIRECTORIES (${CMAKE_CURRENT_SOURCE_DIR}/../output/include/aliconsensus
  ${CMAKE_CURRENT_SOURCE_DIR}/../output/include/easy)

link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../output/lib
  ${CMAKE_CURRENT_SOURCE_DIR}/../../dependency/rocksdb/)

add_executable (echo_debug echo_debug.cc)
target_link_libraries (echo_debug aliconsensus protobuf rocksdb pthread snappy z bz2
  lz4 rt ssl crypto myeasy)

add_executable (leak_check leak_check.cc)
target_link_libraries (leak_check aliconsensus protobuf rocksdb pthread snappy z bz2
  lz4 rt ssl crypto myeasy)

ADD_EXECUTABLE (benchmark3 benchmark3.cc)
TARGET_LINK_LIBRARIES (benchmark3 aliconsensus protobuf rocksdb pthread snappy z bz2
  lz4 rt ssl crypto myeasy)

ADD_EXECUTABLE (benchmark benchmark.cc)
TARGET_LINK_LIBRARIES (benchmark aliconsensus protobuf rocksdb pthread snappy z bz2
  lz4 rt ssl crypto myeasy)
add_dependencies (benchmark
  ${CMAKE_CURRENT_SOURCE_DIR}/../output/lib/libaliconsensus.a)

ADD_EXECUTABLE (benchmark_compression benchmark_compression.cc)
TARGET_LINK_LIBRARIES (benchmark_compression aliconsensus protobuf pthread z bz2
  lz4 rt ssl crypto myeasy)

