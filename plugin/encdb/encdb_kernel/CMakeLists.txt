# Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
#

########################
# project-level settings
########################
cmake_minimum_required(VERSION 3.7)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)
# set cmake build type
if(NOT DEFINED CMAKE_BUILD_TYPE)
   set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build.")
endif(NOT DEFINED CMAKE_BUILD_TYPE)
# RDS_MYSQL or POLARDB_MYSQL
if(NOT DEFINED ENCDB_DB_TYPE)
   SET(ENCDB_DB_TYPE NO_DB CACHE STRING "Plugin module name")
endif(NOT DEFINED ENCDB_DB_TYPE)

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)

# configure lib and bin output dirs
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

set(ENCDB_MODULE_NAME encdb CACHE STRING "Plugin module name")

###############################
# configure build target option
###############################
message(STATUS "[ENCDB] building host DB plugin libraries")

##########################################
# configure EncDB common compilation flags
##########################################

if(CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64)
    set(ENCDB_COMMON_CFLAGS -m64)
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL aarch64)
    set(ENCDB_COMMON_CFLAGS "-DDUMMY")
    # set(ENCDB_COMMON_CFLAGS "-marm")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL i386)
    set(ENCDB_COMMON_CFLAGS -m32)
else(CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64)
    message(FATAL_ERROR "[ENCDB] NOT supported processor architecture ${CMAKE_SYSTEM_PROCESSOR}.")
endif(CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64)

if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(ENCDB_COMMON_CFLAGS "${ENCDB_COMMON_CFLAGS} -O0 -g -DDEBUG -UNDEBUG")
else(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(ENCDB_COMMON_CFLAGS "${ENCDB_COMMON_CFLAGS} -O2 -g -UDEBUG -DNDEBUG")
endif(CMAKE_BUILD_TYPE STREQUAL Debug)

set(ENCDB_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(APP_INC_DIRS "${ENCDB_INCLUDE_DIR}")
set(APP_C_FLAGS "${ENCDB_COMMON_CFLAGS} -fPIC -Wno-attributes ${APP_INC_FLAGS} ${ENCDB_COVERAGE_COMPILE_FLAGS}")
set(APP_CXX_FLAGS "${APP_C_FLAGS}")
set(APP_LINK_FLAGS "-ldl -lpthread")

set(GLOBAL_ENCDB_COMMON_CFLAGS ${ENCDB_COMMON_CFLAGS} CACHE INTERNAL "")
set(GLOBAL_APP_INC_DIRS ${APP_INC_DIRS} CACHE INTERNAL "")
set(GLOBAL_APP_CXX_FLAGS ${APP_CXX_FLAGS} CACHE INTERNAL "")

set(ENCDB_APP_LINK_FLAGS "${APP_LINK_FLAGS}")

message(STATUS "[ENCDB] ENCDB_APP_LINK_FLAGS = ${ENCDB_APP_LINK_FLAGS}")
message(STATUS "[ENCDB] CMAKE_SYSTEM_PROCESSOR = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "[ENCDB] ENCDB_COMMON_CFLAGS = ${ENCDB_COMMON_CFLAGS}")

#############
# build encdb
#############
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Dependencies.cmake)

# configure a header file to pass the extension configurations
configure_file(EncDBConfig.h.in EncDBConfig.h)

add_subdirectory(src)

############
# pack encdb
############

# CPack for packaging. set all cpack variables before include cpack
string(TIMESTAMP COMPILE_TIME "%Y%m%d%H%M%S")
find_package(Git QUIET)
if(GIT_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} log -1 --pretty=format:%h # current commit id
        # COMMAND ${GIT_EXECUTABLE} symbolic-ref --short -q HEAD # current branch
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_ID
        # OUTPUT_VARIABLE GIT_BRANCH_NAME
        ERROR_QUIET
        OUTPUT_STRIP_TRAILING_WHITESPACE
        )
endif(GIT_FOUND)

include(InstallRequiredSystemLibraries)
set(CPACK_PACKAGE_VENDOR "Alibaba Damo DBLab")
set(CPACK_PACKAGE_DESCRIPTION "Encrypted database powered by Alibaba Damo DBLab")
set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_CHECKSUM "SHA256")
set(CPACK_PACKAGE_NAME "${PROJECT_NAME}-${ENCDB_DB_TYPE}")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}-${COMPILE_TIME}-${GIT_COMMIT_ID}")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CMAKE_HOST_SYSTEM_PROCESSOR}")
include(CPack)
