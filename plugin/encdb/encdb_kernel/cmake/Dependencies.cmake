# Copyright (c) 2019-2020 Alibaba DAMO DBLab.

# add local module path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)
find_package(Threads REQUIRED)
# find openssl
include(FindOpenSSL)
