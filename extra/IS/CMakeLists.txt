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

set(COMMON_FLAGS "-Wno-unused-function -Wno-type-limits -Wno-unused-variable -Wno-empty-body -Wno-dangling-else -Wno-vla -Wno-unused-value -Wno-deprecated-declarations -Wno-unused-but-set-variable -Wno-implicit-fallthrough -Wno-unused-parameter -Wno-unused-result -Wno-sign-compare -Wno-uninitialized -Wno-undef -Wno-cast-qual -Wno-strict-aliasing -Wno-incompatible-pointer-types-discards-qualifiers -Wno-missing-field-initializers -Wno-conditional-uninitialized")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS} -Wno-ignored-qualifiers")
# some flags are only supported by gnu compiler
if (CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-discarded-qualifiers -Wno-no-conditional-uninitialized -Wno-jump-misses-init -Wno-old-style-declaration")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} -Wno-reorder -Wno-suggest-override -Wno-extra-semi -Wno-overloaded-virtual -Wno-inconsistent-missing-destructor-override -Wno-non-virtual-dtor -Wno-shadow-field -Wno-pessimizing-move -Wno-deprecated-copy-with-user-provided-dtor")
if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess -Wno-format-truncation")
endif()

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
