#!/usr/bin/bash

source cicd/common.sh
CMAKE_BIN=${CMAKE_BIN_PATH}

if [ ! -d "${CICD_BUILD_ROOT}" ]; then
  mkdir "${CICD_BUILD_ROOT}"
fi

if [ -d "${RESULT_PATH}" ]; then
  rm -rf "${RESULT_PATH}"
fi

if [ ! -d "${RESULT_PATH}" ]; then
  mkdir -p "${RESULT_PATH}"
fi

cat "${BOOST_PATH}".* >"${BOOST_PATH}"

CMAKE_FLAGS=(
  "-DWITH_SSL=openssl"
  "-DDOWNLOAD_BOOST=1"
  "-DWITH_TESTS=1"
  "-DWITH_BOOST=${BOOST_DIRECTORY}"
)

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ]; then
  CMAKE_FLAGS+=(
    "-DCMAKE_BUILD_TYPE=Debug"
    "-DWITH_DEBUG=1"
  )
elif [ "${TEST_TYPE_ENUM}" -eq "${MERGE_PRECHECK}" ]; then
  CMAKE_FLAGS+=(
    "-DCMAKE_BUILD_TYPE=RelWithDebInfo"
  )
elif [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ]; then
  CMAKE_FLAGS+=(
    "-DCMAKE_BUILD_TYPE=Debug"
    "-DWITH_DEBUG=1"
    "-DENABLE_GCOV=1"
  )
fi

cd "${CICD_BUILD_ROOT}" && \
${CMAKE_BIN} .. "${CMAKE_FLAGS[@]}"
