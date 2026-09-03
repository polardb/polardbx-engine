# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

add_library(encdb-type INTERFACE)

target_include_directories(encdb-type INTERFACE
    ${ENCDB_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
