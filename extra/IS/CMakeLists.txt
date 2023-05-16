cmake_minimum_required(VERSION 3.0)

project(consensus VERSION 1.0.0)

option(WITH_TESTS "build test" OFF)

include(cmake/protobuf.cmake)
include(cmake/rocksdb.cmake)
include(cmake/easy.cmake)
include(cmake/mysql.cmake)

set(CONSENSUS_INCLUDE_DIR
  ${CMAKE_CURRENT_SOURCE_DIR}/src/algorithm
  ${CMAKE_CURRENT_SOURCE_DIR}/src/client
  ${CMAKE_CURRENT_SOURCE_DIR}/src/net
  ${CMAKE_CURRENT_SOURCE_DIR}/src/protocol
  ${CMAKE_CURRENT_SOURCE_DIR}/src/service
  ${CMAKE_CURRENT_SOURCE_DIR}/src/common
  ${CMAKE_CURRENT_SOURCE_DIR}/src/server
  ${CMAKE_CURRENT_SOURCE_DIR}/src/log
  ${CMAKE_CURRENT_SOURCE_DIR}/src/config
  )

include_directories(${CONSENSUS_INCLUDE_DIR})

set(COMMON_FLAGS "-Wno-unused-function -Wno-format-truncation -Wno-type-limits -Wno-unused-variable -Wno-empty-body -Wno-dangling-else -Wno-vla -Wno-unused-value -Wno-deprecated-declarations -Wno-unused-but-set-variable -Wno-implicit-fallthrough -Wno-unused-parameter -Wno-sign-compare -Wno-maybe-uninitialized -Wno-undef -Wno-cast-qual -fno-strict-aliasing")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS} -Wno-jump-misses-init -Wno-discarded-qualifiers -Wno-old-style-declaration")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} -Wno-reorder -Wno-suggest-override -Wno-extra-semi -Wno-class-memaccess -Wno-overloaded-virtual -Wno-suggest-attribute=format")

# prepare environment
set(DEPENDENCY_PATH ${CMAKE_CURRENT_SOURCE_DIR}/dependency)
if (WITH_TESTS)
  IMPORT_ROCKSDB()
endif ()
IMPORT_EASY()
IMPORT_MYSQL()

add_subdirectory(src)
if (WITH_TESTS)
  add_subdirectory(unittest)
endif ()
