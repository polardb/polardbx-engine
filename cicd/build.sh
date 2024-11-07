#!/usr/bin/bash

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

source cicd/common.sh
set -e
CMAKE_BIN=${CMAKE_BIN_PATH}
CORES=$(nproc)

clean_build_root() {
  rm -rf ${CICD_BUILD_ROOT}
  if [ ! -d "${CICD_BUILD_ROOT}" ]; then
    mkdir "${CICD_BUILD_ROOT}"
  fi

  if [ -d "${RESULT_PATH}" ]; then
    rm -rf "${RESULT_PATH}"
  fi

  if [ ! -d "${RESULT_PATH}" ]; then
    mkdir -p "${RESULT_PATH}"
  fi
}

export_compile_flags() {
  CFLAGS="$@"
  CXXFLAGS="$@"
  export CFLAGS CXXFLAGS
}

configure_and_build() {
  clean_build_root
  cd "${CICD_BUILD_ROOT}" && \
  ${CMAKE_BIN} .. "$@" && \
  ${CMAKE_BIN} --build . -- -j "${CORES}"
}

cat "${BOOST_PATH}".* >"${BOOST_PATH}"

COMMON_COMPILER_FLAGS=(
  "-fdiagnostics-color=always"
  "-fexceptions"
  "-fno-omit-frame-pointer"
  "-fstack-protector-strong"
)

COMMON_CMAKE_FLAGS=(
  "-DWITH_SSL=openssl"
  "-DDOWNLOAD_BOOST=1"
  "-DWITH_BOOST=${BOOST_DIRECTORY}"
  "-DMYSQL_MAINTAINER_MODE=1"
  "-DWITH_NDB=0"
)

RELEASE_COMPILER_FLAGS=("${COMMON_COMPILER_FLAGS[@]}")
RELEASE_COMPILER_FLAGS+=(
  "-O3"
  "-g"
  "-D_FORTIFY_SOURCE=2"
)
RELEASE_CMAKE_FLAGS=("${COMMON_CMAKE_FLAGS[@]}")
RELEASE_CMAKE_FLAGS+=(
  "-DENABLE_GCOV=0"
  "-DWITH_TESTS=0"
  "-DWITH_DEBUG=0"
  "-DCMAKE_BUILD_TYPE=Release"
)

DEBUG_COMPILER_FLAGS=("${COMMON_COMPILER_FLAGS[@]}")
DEBUG_COMPILER_FLAGS+=(
  "-O0"
  "-g3"
)
DEBUG_CMAKE_FLAGS=("${COMMON_CMAKE_FLAGS[@]}")
DEBUG_CMAKE_FLAGS+=(
  "-DWITH_TESTS=1"
  "-DWITH_DEBUG=1"
  "-DCMAKE_BUILD_TYPE=Debug"
)


if [ "${TEST_TYPE_ENUM}" -eq "${MERGE_PRECHECK}" ]; then
  # release build
  export_compile_flags "${RELEASE_COMPILER_FLAGS[@]}"
  configure_and_build "${RELEASE_CMAKE_FLAGS[@]}"

  # debug build
  export_compile_flags "${DEBUG_COMPILER_FLAGS[@]}"
  configure_and_build "${DEBUG_CMAKE_FLAGS[@]}"
else
  COMPILER_FLAGS=("${DEBUG_COMPILER_FLAGS[@]}")
  CMAKE_FLAGS=("${DEBUG_CMAKE_FLAGS[@]}")
  if [ "${TEST_TYPE_ENUM}" -eq "${MANUAL}" ] ||
    [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ] ||
    [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
    [ "${TEST_TYPE_ENUM}" -eq "${MERGE_PRECHECK}" ]; then
    CMAKE_FLAGS+=(
      "-DENABLE_GCOV=0"
    )
  elif  [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ]; then
    CMAKE_FLAGS+=(
      "-DENABLE_GCOV=1"
    )
  fi
  export_compile_flags "${COMPILER_FLAGS[@]}"
  configure_and_build "${CMAKE_FLAGS[@]}"
fi
