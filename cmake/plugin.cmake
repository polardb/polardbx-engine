# Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA 


GET_FILENAME_COMPONENT(MYSQL_CMAKE_SCRIPT_DIR ${CMAKE_CURRENT_LIST_FILE} PATH)
INCLUDE(${MYSQL_CMAKE_SCRIPT_DIR}/cmake_parse_arguments.cmake)

# MYSQL_ADD_PLUGIN(plugin_name source1...sourceN
# [STORAGE_ENGINE]
# [MANDATORY|DEFAULT]
# [STATIC_ONLY|MODULE_ONLY]
# [MODULE_OUTPUT_NAME module_name]
# [STATIC_OUTPUT_NAME static_name]
# [LINK_LIBRARIES lib1...libN]
# [DEPENDENCIES target1...targetN]

# MANDATORY   : not actually a plugin, always builtin
# DEFAULT     : builtin as static by default
# MODULE_ONLY : build only as shared library

# Append collections files for the plugin to the common files
# Make sure we don't copy twice if running cmake again

MACRO(PLUGIN_APPEND_COLLECTIONS plugin)
  SET(fcopied "${CMAKE_CURRENT_SOURCE_DIR}/tests/collections/FilesCopied")
  IF(NOT EXISTS ${fcopied})
    FILE(GLOB collections ${CMAKE_CURRENT_SOURCE_DIR}/tests/collections/*)
    FOREACH(cfile ${collections})
      FILE(READ ${cfile} contents)
      GET_FILENAME_COMPONENT(fname ${cfile} NAME)
      FILE(APPEND ${CMAKE_SOURCE_DIR}/mysql-test/collections/${fname} "${contents}")
      FILE(APPEND ${fcopied} "${fname}\n")
      MESSAGE(STATUS "Appended ${cfile}")
    ENDFOREACH()
  ENDIF()
ENDMACRO()

MACRO(MYSQL_ADD_PLUGIN)
  MYSQL_PARSE_ARGUMENTS(ARG
    "LINK_LIBRARIES;DEPENDENCIES;MODULE_OUTPUT_NAME;STATIC_OUTPUT_NAME"
    "STORAGE_ENGINE;STATIC_ONLY;MODULE_ONLY;MANDATORY;DEFAULT;DISABLED;TEST_ONLY;SKIP_INSTALL"
    ${ARGN}
  )
  
  # Add common include directories
  INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/include 
                    ${CMAKE_SOURCE_DIR}/sql
                    ${CMAKE_SOURCE_DIR}/libbinlogevents/include
                    ${CMAKE_SOURCE_DIR}/sql/auth
                    ${CMAKE_SOURCE_DIR}/regex
                    ${SSL_INCLUDE_DIRS}
                    ${ZLIB_INCLUDE_DIR})

  LIST(GET ARG_DEFAULT_ARGS 0 plugin) 
  SET(SOURCES ${ARG_DEFAULT_ARGS})
  LIST(REMOVE_AT SOURCES 0)
  STRING(TOUPPER ${plugin} plugin)
  STRING(TOLOWER ${plugin} target)
  
  # Figure out whether to build plugin
  IF(WITH_PLUGIN_${plugin})
    SET(WITH_${plugin} 1)
  ENDIF()

  IF(WITH_MAX_NO_NDB)
    SET(WITH_MAX 1)
    SET(WITHOUT_NDBCLUSTER 1)
  ENDIF()

  IF(ARG_DEFAULT)
    IF(NOT DEFINED WITH_${plugin} AND 
       NOT DEFINED WITH_${plugin}_STORAGE_ENGINE)
      SET(WITH_${plugin} 1)
    ENDIF()
  ENDIF()
  
  IF(WITH_${plugin}_STORAGE_ENGINE 
    OR WITH_{$plugin}
    OR WITH_ALL 
    OR WITH_MAX 
    AND NOT WITHOUT_${plugin}_STORAGE_ENGINE
    AND NOT WITHOUT_${plugin}
    AND NOT ARG_MODULE_ONLY)
     
    SET(WITH_${plugin} 1)
  ELSEIF(WITHOUT_${plugin}_STORAGE_ENGINE OR WITH_NONE OR ${plugin}_DISABLED)
    SET(WITHOUT_${plugin} 1)
    SET(WITH_${plugin}_STORAGE_ENGINE 0)
    SET(WITH_${plugin} 0)
  ENDIF()
  
  
  IF(ARG_MANDATORY)
    SET(WITH_${plugin} 1)
  ENDIF()

  
  IF(ARG_STORAGE_ENGINE)
    SET(with_var "WITH_${plugin}_STORAGE_ENGINE" )
  ELSE()
    SET(with_var "WITH_${plugin}")
  ENDIF()
  
  IF(NOT ARG_DEPENDENCIES)
    SET(ARG_DEPENDENCIES)
  ENDIF()
  SET(BUILD_PLUGIN 1)
  # Build either static library or module
  IF (WITH_${plugin} AND NOT ARG_MODULE_ONLY)
    ADD_CONVENIENCE_LIBRARY(${target} STATIC ${SOURCES})

    # Plugins are taken to be part of the server (so MYSQL_SERVER).
    # However, also make sure that when building the plugin statically, we
    # don't try to export any symbols pulled in from header files.
    # This would be harmless in the server, but some of the statically linked
    # plugins (like the performance schema plugin) are also linked into unit
    # tests, which don't necessarily link in all the dependencies of such
    # symbols.
    SET_TARGET_PROPERTIES(${target}
      PROPERTIES COMPILE_DEFINITIONS "MYSQL_SERVER;MYSQL_NO_PLUGIN_EXPORT")
    SET_TARGET_PROPERTIES(${target}
      PROPERTIES COMPILE_FLAGS ${SSL_DEFINES})

    ADD_DEPENDENCIES(${target} GenError ${ARG_DEPENDENCIES})
    
    IF(ARG_STATIC_OUTPUT_NAME)
      SET_TARGET_PROPERTIES(${target} PROPERTIES 
      OUTPUT_NAME ${ARG_STATIC_OUTPUT_NAME})
    ENDIF()

    # Link the plugin into mysqld.
    SET (MYSQLD_STATIC_PLUGIN_LIBS ${MYSQLD_STATIC_PLUGIN_LIBS}
       ${target} CACHE INTERNAL "" FORCE)

    IF(ARG_MANDATORY)
      SET(${with_var} ON CACHE INTERNAL "Link ${plugin} statically to the server" 
       FORCE)
    ELSE()	
      SET(${with_var} ON CACHE BOOL "Link ${plugin} statically to the server" 
       FORCE)
    ENDIF()

    SET(THIS_PLUGIN_REFERENCE " builtin_${target}_plugin,")
    SET(PLUGINS_IN_THIS_SCOPE
      "${PLUGINS_IN_THIS_SCOPE}${THIS_PLUGIN_REFERENCE}")

    IF(ARG_MANDATORY)
      SET (mysql_mandatory_plugins  
        "${mysql_mandatory_plugins} ${PLUGINS_IN_THIS_SCOPE}" 
        PARENT_SCOPE)
    ELSE()
      SET (mysql_optional_plugins  
        "${mysql_optional_plugins} ${PLUGINS_IN_THIS_SCOPE}"
        PARENT_SCOPE)
    ENDIF()

  ELSEIF(NOT WITHOUT_${plugin} AND NOT ARG_STATIC_ONLY  AND NOT DISABLE_SHARED)
    IF(NOT ARG_MODULE_OUTPUT_NAME)
      IF(ARG_STORAGE_ENGINE)
        SET(ARG_MODULE_OUTPUT_NAME "ha_${target}")
      ELSE()
        SET(ARG_MODULE_OUTPUT_NAME "${target}")
      ENDIF()
    ENDIF()

    ADD_VERSION_INFO(${target} MODULE SOURCES)
    ADD_LIBRARY(${target} MODULE ${SOURCES}) 
    SET_TARGET_PROPERTIES (${target} PROPERTIES PREFIX ""
      COMPILE_DEFINITIONS "MYSQL_DYNAMIC_PLUGIN")
    TARGET_LINK_LIBRARIES (${target} mysqlservices)

    # Plugin uses symbols defined in mysqld executable.
    # Some operating systems like Windows and OSX and are pretty strict about 
    # unresolved symbols. Others are less strict and allow unresolved symbols
    # in shared libraries. On Linux for example, CMake does not even add 
    # executable to the linker command line (it would result into link error). 
    # Use MYSQL_PLUGIN_API for symbols to be exported/imported.
    IF(WIN32 OR APPLE)
      TARGET_LINK_LIBRARIES (${target} mysqld ${ARG_LINK_LIBRARIES})
    ELSE()
      TARGET_LINK_LIBRARIES (${target} ${ARG_LINK_LIBRARIES})
    ENDIF()
    ADD_DEPENDENCIES(${target} GenError ${ARG_DEPENDENCIES})

    IF(NOT ARG_MODULE_ONLY)
      # set cached variable, e.g with checkbox in GUI
      SET(${with_var} OFF CACHE BOOL "Link ${plugin} statically to the server" 
       FORCE)
    ENDIF()
    SET_TARGET_PROPERTIES(${target} PROPERTIES 
      OUTPUT_NAME "${ARG_MODULE_OUTPUT_NAME}")  

    # Store all plugins in the same directory, for easier testing.
    SET_TARGET_PROPERTIES(${target} PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/plugin_output_directory
      )

    # Install dynamic library
    IF(NOT ARG_SKIP_INSTALL)
      SET(INSTALL_COMPONENT Server)
      IF(ARG_TEST_ONLY)
        SET(INSTALL_COMPONENT Test)
      ENDIF()
      MYSQL_INSTALL_TARGETS(${target}
        DESTINATION ${INSTALL_PLUGINDIR}
        COMPONENT ${INSTALL_COMPONENT})
      INSTALL_DEBUG_TARGET(${target}
        DESTINATION ${INSTALL_PLUGINDIR}/debug
        COMPONENT ${INSTALL_COMPONENT})
      # For internal testing in PB2, append collections files
      IF(DEFINED ENV{PB2WORKDIR})
        PLUGIN_APPEND_COLLECTIONS(${plugin})
      ENDIF()
    ENDIF()
  ELSE()
    IF(WITHOUT_${plugin})
      # Update cache variable
      STRING(REPLACE "WITH_" "WITHOUT_" without_var ${with_var})
      SET(${without_var} ON CACHE BOOL "Don't build ${plugin}" 
       FORCE)
    ENDIF()
    SET(BUILD_PLUGIN 0)
  ENDIF()

  IF(BUILD_PLUGIN AND ARG_LINK_LIBRARIES)
    TARGET_LINK_LIBRARIES (${target} ${ARG_LINK_LIBRARIES})
  ENDIF()

ENDMACRO()


# Add all CMake projects under storage  and plugin 
# subdirectories, configure sql_builtin.cc
MACRO(CONFIGURE_PLUGINS)
  FILE(GLOB dirs_storage ${CMAKE_SOURCE_DIR}/storage/*)
  IF(NOT DISABLE_SHARED)
    FILE(GLOB dirs_plugin ${CMAKE_SOURCE_DIR}/plugin/*)
    IF(WITH_RAPID)
      FILE(GLOB dirs_rapid_plugin ${CMAKE_SOURCE_DIR}/rapid/plugin/*)
    ENDIF(WITH_RAPID)
  ENDIF()
  
  FOREACH(dir ${dirs_storage} ${dirs_plugin} ${dirs_rapid_plugin})
    IF (EXISTS ${dir}/CMakeLists.txt)
      ADD_SUBDIRECTORY(${dir})
    ENDIF()
  ENDFOREACH()
ENDMACRO()
