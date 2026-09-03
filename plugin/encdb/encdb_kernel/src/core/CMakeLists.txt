# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

#######################
# ENCDB_CORE_FILES
#######################

set(ENCDB_CORE_FILES
    include/encdb_uts.h
    include/enc_aux.h
    src/enc_aux.cpp
    src/encdb_internal.cpp
    )

##########################
# core module objects
##########################

# add library
set(TARGET_NAME encdb-core)
add_library(${TARGET_NAME} OBJECT ${ENCDB_CORE_FILES})
add_dependencies(${TARGET_NAME} encdb-crypto)

# set library properties
target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(${TARGET_NAME} PRIVATE
    ${GLOBAL_APP_INC_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    $<TARGET_PROPERTY:encdb-type,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-common,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-crypto,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-key,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-rule,INTERFACE_INCLUDE_DIRECTORIES>
    )
set_target_properties(${TARGET_NAME} PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS}")
