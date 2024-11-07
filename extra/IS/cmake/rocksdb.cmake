# Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.
# 
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
# 
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation. The authors
# of MySQL hereby grant you an additional permission to link the program and
# your derivative works with the separately licensed software that they have
# included with MySQL.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
# for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA


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