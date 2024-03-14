MACRO(IMPORT_ROCKSDB)
  set(ROCKSDB_VERSION 6.29.4)
  set(ROCKSDB_DIR ${DEPENDENCY_PATH}/rocksdb-${ROCKSDB_VERSION})

  if (NOT EXISTS ${ROCKSDB_DIR})
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E tar xzvf ${DEPENDENCY_PATH}/rocksdb-${ROCKSDB_VERSION}.tar.gz
      WORKING_DIRECTORY ${DEPENDENCY_PATH}
      RESULT_VARIABLE tar_result
    )
  endif ()

  # backup the original values
  set(WITH_TESTS_BACKUP ${WITH_TESTS})

  set(WITH_SNAPPY ON)
  set(WITH_ZSTD OFF)
  set(WITH_GFLAGS OFF)
  set(FAIL_ON_WARNINGS OFF)
  set(WITH_BENCHMARK_TOOLS OFF)
  set(WITH_TESTS OFF)
  set(WITH_JEMALLOC OFF)
  set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

  # ignore errors in dependency build
  set(ORIGINAL_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  string(FIND "${CMAKE_CXX_FLAGS}" "-Werror" WERROR_POS)
  if (NOT WERROR_POS EQUAL -1)
      string(REPLACE "-Werror" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

  add_subdirectory(${ROCKSDB_DIR})
  include_directories(${ROCKSDB_DIR}/include)

  set(CMAKE_CXX_FLAGS "${ORIGINAL_CXX_FLAGS}")

  # restore the original values
  set(WITH_TESTS ${WITH_TESTS_BACKUP})
ENDMACRO(IMPORT_ROCKSDB)