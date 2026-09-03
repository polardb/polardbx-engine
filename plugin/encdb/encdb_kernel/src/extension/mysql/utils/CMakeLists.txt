# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

set(ENCDB_MYSQL_UTILS_FILES
    include/mysql.hh
    include/mysql_utils.h
    include/masking_util.h
    src/mysql_utils.cpp
    src/masking_util.cpp
    )

###########################
# static library
###########################

# add library
set(TARGET_NAME encdb-mysql-utils)
add_library(${TARGET_NAME} STATIC ${ENCDB_MYSQL_UTILS_FILES})

# set library properties
target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(${TARGET_NAME} PRIVATE
    $<TARGET_PROPERTY:encdb-type,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-common,INTERFACE_INCLUDE_DIRECTORIES>
    ${OPENSSL_INCLUDE_DIR}
    )

set_target_properties(${TARGET_NAME} PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS}")
