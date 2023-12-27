#!/usr/bin/bash

CMAKE_BIN=${CMAKE_BIN_PATH}
CORES=$(nproc)



cd "${CICD_BUILD_ROOT}" && \
${CMAKE_BIN} --build . -- -j "${CORES}"
