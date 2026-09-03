# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

option(ENCDB_BUILD_WITH_TEE    "Enable TEE, build TEE host DL" OFF)

set(ENCDB_KEY_FILES
    include/encdb_key_exception.h
    include/key_mgmt.h
    src/encdb_key_exception.cpp
    src/key_mgmt.cpp
    src/ra.h
    src/ra.cpp
)

if(ENCDB_BUILD_WITH_TEE)
    set(ENCDB_KEY_FILES ${ENCDB_KEY_FILES}
        src/encdb_tdx_attest.cpp
    )
endif(ENCDB_BUILD_WITH_TEE)

###########################
# key module static library
###########################

# add library
set(TARGET_NAME encdb-key)
add_library(${TARGET_NAME} STATIC ${ENCDB_KEY_FILES})

# set library properties
target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(${TARGET_NAME} PRIVATE
    $<TARGET_PROPERTY:encdb-core,INTERFACE_INCLUDE_DIRECTORIES>
    ${OPENSSL_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${GLOBAL_APP_INC_DIRS}  # generated headers e.g. EncDBConfig.h
    )

set_target_properties(${TARGET_NAME} PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS}")
target_link_libraries(${TARGET_NAME} PRIVATE ${ENCDB_APP_LINK_FLAGS})
if(ENCDB_BUILD_WITH_TEE)
    target_link_libraries(${TARGET_NAME} PRIVATE "-ltdx_attest -lsgx_dcap_quoteverify")
endif(ENCDB_BUILD_WITH_TEE)

target_link_libraries(${TARGET_NAME} PRIVATE encdb-crypto encdb-common encdb-type)
