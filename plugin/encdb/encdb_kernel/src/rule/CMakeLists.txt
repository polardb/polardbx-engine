# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

set(ENCDB_RULE_FILES
    include/encdb_rules.h
    src/encdb_rules.cpp
    include/encdb_sens_rule.h
    src/encdb_sens_rule.cpp
    )

###########################
# rule module static library
###########################

# add library
set(TARGET_NAME encdb-rule)
add_library(${TARGET_NAME} OBJECT ${ENCDB_RULE_FILES})

# set library properties
target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(${TARGET_NAME} PRIVATE
    $<TARGET_PROPERTY:encdb-type,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-common,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-crypto,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:encdb-key,INTERFACE_INCLUDE_DIRECTORIES>
    ${OPENSSL_INCLUDE_DIR}
    )

set_target_properties(${TARGET_NAME} PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS}")
