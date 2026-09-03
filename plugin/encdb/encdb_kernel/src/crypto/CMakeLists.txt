# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.

set(ENCDB_CRYPTO_BASE_FILES
    src/encdb_crypto_exception.cpp
    src/encdb_rand.cpp
    src/encdb_hash.cpp
    src/encdb_symmetric.cpp
    src/encdb_asymmetric.cpp
    src/encdb_cipher_suite.cpp
    src/encdb_cipher.cpp
    include/encdb_cipher_types.h
    src/encdb_cipher_types.cpp
    include/encdb_symmetric_stream.h
    src/encdb_symmetric_stream.cpp
    )

###############################
# ENCDB_CRYPTO_HOST_FILES
###############################

set(ENCDB_CRYPTO_HOST_FILES
    ${ENCDB_CRYPTO_BASE_FILES}
    # for sm4 gcm support
    src/sm4.h
    src/sm4.c
    src/encdb_crypto_provider_openssl.cpp
    )


##############################
# crypto module static library
##############################

# add library
set(TARGET_NAME encdb-crypto)
add_library(${TARGET_NAME} STATIC ${ENCDB_CRYPTO_HOST_FILES})
add_dependencies(${TARGET_NAME} OpenSSL::Crypto)
# set library properties
target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
# set OpenSSL header directories
target_include_directories(${TARGET_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})

set_target_properties(${TARGET_NAME} PROPERTIES COMPILE_FLAGS "${GLOBAL_APP_CXX_FLAGS}")
target_link_libraries(${TARGET_NAME} PRIVATE ${ENCDB_APP_LINK_FLAGS})
# prepare openssl flags, need to find crypto lib path explicitly

# encdb-crypto will not automatically export openssl symbols by using `--exclude-libs=libcrypto`
if(ENCDB_DB_TYPE STREQUAL POLARDB_MYSQL OR ENCDB_DB_TYPE STREQUAL NO_DB)
    set(OPENSSL_LD_FLAGS "-L${OPENSSL_LIBRARY_DIR} -Wl,--exclude-libs=libcrypto -l:libcrypto.a -ldl -fPIC")
elseif(ENCDB_DB_TYPE STREQUAL RDS_MYSQL)
    set(OPENSSL_LD_FLAGS "-L${OPENSSL_LIBRARY_DIR} -l:libcrypto.a -ldl -fPIC")
endif()
target_link_libraries(${TARGET_NAME} PRIVATE "${OPENSSL_LD_FLAGS}")

target_link_libraries(${TARGET_NAME} PRIVATE encdb-type encdb-common)
