cmake_minimum_required(VERSION 3.0)

add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)

if(ENCDB_DB_TYPE STREQUAL RDS_MYSQL)
    # Reuse system OpenSSL paths from main build (WITH_SSL=system) if available;
    # fallback to bundled extra/openssl otherwise.
    if(DEFINED OPENSSL_INCLUDE_DIR AND DEFINED OPENSSL_LIBRARY)
        get_filename_component(OPENSSL_ROOT_DIR ${OPENSSL_INCLUDE_DIR} DIRECTORY)
        get_filename_component(OPENSSL_LIBRARY_DIR ${OPENSSL_LIBRARY} DIRECTORY)
    else()
        set(OPENSSL_ROOT_DIR ${CMAKE_SOURCE_DIR}/extra/openssl)
        set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT_DIR}/include)
        set(OPENSSL_LIBRARY_DIR ${OPENSSL_ROOT_DIR})
    endif()
else()
    # NO_DB or POLARDB_MYSQL
    set(DOWNLOAD_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/third_party)
    set(INSTALL_DIR ${PROJECT_BINARY_DIR}/openssl_installs/${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR})

    include(ExternalProject)
    ################################################################################
    # openssl
    ################################################################################
    set(PKG_NAME "Tongsuo")
    set(PKG_VERSION "8.3.2")
    set(PKG_FILE "${DOWNLOAD_DIR}/${PKG_NAME}-${PKG_VERSION}.tar.gz")
    set(PKG_URL "https://github.com/Tongsuo-Project/Tongsuo/archive/refs/tags/8.3.2.tar.gz")
    set(PKG_MD5 "5885b4327c9fdc2efa4363dc41fb551d")

    # check whether we have openssl downloaded, if yes, skip download again
    if(EXISTS ${PKG_FILE})
        set(PKG_URL ${PKG_FILE})
    else()
        MESSAGE(STATUS "ERROR ${PKG_FILE} DOES NOT EXIST")
    endif()

    if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
        message(FATAL_ERROR "[ENCDB] ${CMAKE_SYSTEM_NAME} is not supported.")
    endif()
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL sw_64)
        set(CONFIGURE_COMMAND <SOURCE_DIR>/Configure --prefix=<INSTALL_DIR> -fPIC no-shared --with-rand-seed=devrandom -D__STDC_NO_ATOMICS__ linux-generic64)
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL aarch64)
        set(CONFIGURE_COMMAND <SOURCE_DIR>/Configure --prefix=<INSTALL_DIR> -fPIC no-shared --with-rand-seed=devrandom -D__STDC_NO_ATOMICS__ linux-aarch64)
    else(CMAKE_SYSTEM_PROCESSOR STREQUAL sw_64)
        set(CONFIGURE_COMMAND <SOURCE_DIR>/config --prefix=<INSTALL_DIR> -fPIC no-shared)
    endif(CMAKE_SYSTEM_PROCESSOR STREQUAL sw_64)
    message(STATUS "[ENCDB] OpenSSL CONFIGURE_COMMAND = ${CONFIGURE_COMMAND}")

    ExternalProject_add(${PKG_NAME}
        PREFIX ${INSTALL_DIR}
        DOWNLOAD_DIR ${DOWNLOAD_DIR}
        DOWNLOAD_NAME ${PKG_NAME}-${PKG_VERSION}.tar.gz
        URL ${PKG_URL}
        URL_MD5 ${PKG_MD5}
        BUILD_IN_SOURCE 1
        # ignore RELEASE when configuring Tongsuo
        CONFIGURE_COMMAND unset RELEASE && ${CONFIGURE_COMMAND}
        BUILD_COMMAND make -j
        INSTALL_COMMAND make install_sw -j # install without man docs
    )
    set(OPENSSL_ROOT_DIR ${INSTALL_DIR})
    set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT_DIR}/include)
    set(OPENSSL_LIBRARY_DIR ${OPENSSL_ROOT_DIR}/lib)

    add_dependencies(OpenSSL::SSL ${PKG_NAME})
    add_dependencies(OpenSSL::Crypto ${PKG_NAME})

endif()

# set openssl properties
file(MAKE_DIRECTORY ${OPENSSL_INCLUDE_DIR})
set_property(TARGET OpenSSL::SSL PROPERTY IMPORTED_LOCATION ${OPENSSL_LIBRARY_DIR}/libssl.a)
set_property(TARGET OpenSSL::SSL PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${OPENSSL_INCLUDE_DIR})

set_property(TARGET OpenSSL::Crypto PROPERTY IMPORTED_LOCATION ${OPENSSL_LIBRARY_DIR}/libcrypto.a)
set_property(TARGET OpenSSL::Crypto PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${OPENSSL_INCLUDE_DIR})

# set variables for cmake find
get_target_property(OPENSSL_SSL_LIBRARY OpenSSL::SSL IMPORTED_LOCATION)
get_target_property(OPENSSL_CRYPTO_LIBRARY OpenSSL::Crypto IMPORTED_LOCATION)
