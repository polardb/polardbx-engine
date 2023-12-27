#!/usr/bin/bash

source cicd/common.sh
CMAKE_BIN=${CMAKE_BIN_PATH}



if [ ! -d "${CICD_BUILD_ROOT}" ]; then
  mkdir "${CICD_BUILD_ROOT}"
fi

if [ ! -d "${RESULT_PATH}" ]; then
  mkdir -p "${RESULT_PATH}"
fi

cat "${BOOST_PATH}".*  > "${BOOST_PATH}"

CMAKE_FLAGS=(
"-DWITH_SSL=openssl"
"-DWITH_TESTS=1"
"-DDOWNLOAD_BOOST=1"
"-DWITH_BOOST=${BOOST_DIRECTORY}"
)

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ]; then
  CMAKE_FLAGS+=(
  "-DCMAKE_BUILD_TYPE=Debug"
  "-DWITH_ASAN=1"
  "-DWITH_TESTS=1"
  )
elif [ "${TEST_TYPE_ENUM}" -eq "${MERGE_CHECK}" ]; then
  CMAKE_FLAGS+=(
    "-DCMAKE_BUILD_TYPE=Release"
    )
fi

cd "${CICD_BUILD_ROOT}" && \
${CMAKE_BIN} .. "${CMAKE_FLAGS[@]}"
