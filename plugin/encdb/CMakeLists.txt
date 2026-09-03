# Copyright (c) 2014, 2022, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

# RDS_MYSQL POLARDB_MYSQL or NO_DB default to RDS_MYSQL

OPTION(WITH_ENCDB "Build EncDB encrypted database plugin" ON)
IF(NOT WITH_ENCDB)
  MESSAGE(STATUS "EncDB plugin is disabled (WITH_ENCDB=OFF)")
  RETURN()
ENDIF()

SET(ENCDB_DB_TYPE RDS_MYSQL CACHE STRING "Plugin module name")
set_property(CACHE ENCDB_DB_TYPE PROPERTY STRINGS RDS_MYSQL POLARDB_MYSQL NO_DB)

cmake_policy(SET CMP0048 NEW)

if(ENCDB_DB_TYPE STREQUAL RDS_MYSQL)
  cmake_policy(SET CMP0057 NEW)
  set(CLIENT_MIN_VERSION 1.2.13)
  set(CLIENT_RECOMMENDED_VERSION 1.2.15)
  SET(PLUGIN_SRC_DIR plugin/${MAJOR_VERSION}${MINOR_VERSION})
  # dependency
  INCLUDE(${PLUGIN_SRC_DIR}/configure.cmake)
  INCLUDE(${MYSQL_CMAKE_SCRIPT_DIR}/compile_flags.cmake)

  ### INCLUDE FILES ###

  INCLUDE_DIRECTORIES(
    ${CMAKE_SOURCE_DIR}/libbinlogevents/include/   # byteorder.h
    ${SSL_INCLUDE_DIRS}                            # SSL include dirs
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLUGIN_SRC_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLUGIN_SRC_DIR}/include/sql_service
    ${CMAKE_CURRENT_SOURCE_DIR}/${PLUGIN_SRC_DIR}/include/memory
    ${CMAKE_CURRENT_SOURCE_DIR}/encdb_kernel/src/extension/mysql/include
    ${CMAKE_CURRENT_SOURCE_DIR}/encdb_kernel/src/common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/encdb_kernel/src/key/include
    ${CMAKE_CURRENT_SOURCE_DIR}/encdb_kernel
    ${CMAKE_CURRENT_SOURCE_DIR}/encdb_kernel/src/crypto/include
    ${CMAKE_CURRENT_SOURCE_DIR}/encdb_kernel/src/type/include)

  ### SOURCES ###
  SET(ENCDB_PLUGIN_SOURCES
    ${PLUGIN_SRC_DIR}/src/plugin.cc
    ${PLUGIN_SRC_DIR}/src/sql_service/sql_resultset.cc
    ${PLUGIN_SRC_DIR}/src/sql_service/sql_service_context.cc
    ${PLUGIN_SRC_DIR}/src/sql_service/sql_service_context_base.cc
    ${PLUGIN_SRC_DIR}/src/sql_service/sql_service_interface.cc
    ${PLUGIN_SRC_DIR}/src/memory/encdb_memory.cc
  )

  SET(ENCDB_UDF_SOURCES
    ${PLUGIN_SRC_DIR}/src/udf.cc
  )
  # RDS_MYSQL POLARDB_MYSQL or NO_DB
  SET(ENCDB_DB_TYPE RDS_MYSQL CACHE STRING "Plugin module name")

  ADD_SUBDIRECTORY(encdb_kernel)

  ADD_DEFINITIONS(${SSL_DEFINES})

  IF(MAJOR_VERSION STREQUAL 5 AND MINOR_VERSION STREQUAL 7)
      SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  ELSEIF(MAJOR_VERSION STREQUAL 8 AND MINOR_VERSION STREQUAL 0)
      SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
      ADD_DEFINITIONS(-DLOG_COMPONENT_TAG="encdb")
  ELSE()
      MESSAGE(FATAL_ERROR "Unsupported RDS MySQL version ${MAJOR_VERSION}.${MINOR_VERSION}")
  ENDIF()
  ### Configuration ###
  SET(LIBRARIES_TO_LINK ${LZ4_LIBRARY} ${SSL_LIBRARIES} ${TIRPC_LIBRARIES} encdb-mysql)

  # declare the plugin itself
  MYSQL_ADD_PLUGIN(encdb ${ENCDB_PLUGIN_SOURCES}
                  MANDATORY STATIC_ONLY
                  LINK_LIBRARIES ${LIBRARIES_TO_LINK}
                  MODULE_OUTPUT_NAME "encdb")

  MYSQL_ADD_PLUGIN(encdb_udf ${ENCDB_UDF_SOURCES}
                  MODULE_ONLY
                  MODULE_OUTPUT_NAME "encdb_udf")
else()  # POLARDB_MYSQL or NO_DB
  project(EncDB VERSION 1.1.13)
  set(PROJECT_OLD_VERSION 1.1.12)
  set(CLIENT_MIN_VERSION 1.2.13)
  set(CLIENT_RECOMMENDED_VERSION 1.2.15)
  string(TOLOWER "${EncDB_VERSION_MAJOR}.${EncDB_VERSION_MINOR}" ENCDB_SUFFIX)
  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
      set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR}/build)
  endif()
  option(ENABLE_UNIT_TEST       "Enable unit tests"         OFF)
  option(ENABLE_COVERAGE        "Enable coverage data"      OFF)
  if(ENABLE_UNIT_TEST)
      include(CTest)
  endif()
  if(ENABLE_COVERAGE)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
      set(APP_LINK_FLAGS "${APP_LINK_FLAGS} -lgcov")
  endif()
  add_subdirectory(encdb_kernel)
endif()
