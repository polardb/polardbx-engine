# Copyright (c) 2015, 2020, Oracle and/or its affiliates. All rights reserved.
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
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# Standard PROTOBUF_GENERATE_CPP modified to generate both
# protobuf C++ files, considering that protobuf source can be "bundled"
# or "system". Where its behavior can be altered by following parameters:
#
# * OUTPUT_DIRECTORY - `protoc` is going to write C++ source to this
#                       directory.
# * DEPENDENCIES - adds steps which need to be executed before generating
#                  source
# * IMPORT_DIRECTORIES - force `protoc`, to look for dependent `proto` files
#                        in pointed location. If this options is not set, the
#                        function is going to generate import directory list
#                        using location of `proto` files.
# * ADDITIONAL_COMMENT - in case when the build script generates the
#                        same `proto` files twice with different option
#                        then this options allows to differentiate those
#                        runs in build output.
#
# Example:
#  MYSQL_PROTOBUF_GENERATE_CPP(
#         MY_CPP_SOURCE MY_CPP_HEADERS
#         some_path/my1.proto
#         some_path/my2.proto
#         other_path/other_package.proto
#         OUTPUT_DIRECTORY ${CMAKE_BINARY_DIRECTORY}/generated/
#         DEPENDENCIES some_other_target
#    )
#
#  MYSQL_PROTOBUF_GENERATE_CPP(
#         MY_CPP_SOURCE MY_CPP_HEADERS
#         some_path/my1.proto
#    )
#
FUNCTION(MYSQL_PROTOBUF_GENERATE_CPP GENERATED_SOURCE GENERATED_HEADERS)
  SET(GENERATION_OPTIONS)
  SET(GENERATION_ONE_VALUE_KW
    OUTPUT_DIRECTORY
    ADDITIONAL_COMMENT)
  SET(GENERATION_MULTI_VALUE_KW
    DEPENDENCIES
    IMPORT_DIRECTORIES)

  CMAKE_PARSE_ARGUMENTS(GENERATE
    "${GENERATION_OPTIONS}"
    "${GENERATION_ONE_VALUE_KW}"
    "${GENERATION_MULTI_VALUE_KW}"
    ${ARGN}
    )

  SET(${GENERATED_SOURCE})
  SET(${GENERATED_HEADERS})
  SET(PROTOC_ADDITION_OPTIONS)
  SET(PROTO_FILES ${GENERATE_UNPARSED_ARGUMENTS})

  IF(NOT PROTO_FILES)
    MESSAGE(SEND_ERROR
      "Error: MYSQL_PROTOBUF_GENERATE_CPP() called without any proto files")
    RETURN()
  ENDIF()

  IF(NOT GENERATE_OUTPUT_DIRECTORY)
    SET(GENERATE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  ENDIF()

  # Build import directory
  # If there are several files processed using this function,
  # it assumes that all of those "proto" files might have
  # dependencies between each other.
  IF(NOT GENERATE_IMPORT_DIRECTORIES)
    FOREACH(PROTO_FILE ${PROTO_FILES})
      GET_FILENAME_COMPONENT(FULL_PATH_PROTO_FILE ${PROTO_FILE} ABSOLUTE)
      GET_FILENAME_COMPONENT(DIRECTORY_OF_PROTO_FILE ${FULL_PATH_PROTO_FILE} DIRECTORY)
      IF(DIRECTORY_OF_PROTO_FILE AND NOT "${DIRECTORY_OF_PROTO_FILE}" IN_LIST GENERATE_IMPORT_DIRECTORIES)
        LIST(APPEND GENERATE_IMPORT_DIRECTORIES "${DIRECTORY_OF_PROTO_FILE}")
      ENDIF()
    ENDFOREACH()
  ENDIF()

  FOREACH(IMPORT_DIRECTORY ${GENERATE_IMPORT_DIRECTORIES})
    LIST(APPEND PROTOC_ADDITION_OPTIONS "-I${IMPORT_DIRECTORY}")
  ENDFOREACH()

  FOREACH(PROTO_FILE ${PROTO_FILES})
    GET_FILENAME_COMPONENT(FULL_PATH_PROTO_FILE ${PROTO_FILE} ABSOLUTE)
    GET_FILENAME_COMPONENT(WITHOUT_DIR_AND_EXT_PROTO_FILE ${PROTO_FILE} NAME_WE)

    SET(GENERATED_CC_FILE "${GENERATE_OUTPUT_DIRECTORY}/${WITHOUT_DIR_AND_EXT_PROTO_FILE}.pb.cc")
    SET(GENERATED_H_FILE "${GENERATE_OUTPUT_DIRECTORY}/${WITHOUT_DIR_AND_EXT_PROTO_FILE}.pb.h")

    ADD_CUSTOM_COMMAND(
      OUTPUT "${GENERATED_CC_FILE}"
             "${GENERATED_H_FILE}"
      COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --cpp_out "${GENERATE_OUTPUT_DIRECTORY}"
           -I "${PROTOBUF_INCLUDE_DIR}"
           ${PROTOC_ADDITION_OPTIONS}
           ${FULL_PATH_PROTO_FILE}
      DEPENDS
           ${FULL_PATH_PROTO_FILE}
           ${PROTOBUF_PROTOC_EXECUTABLE}
           ${GENERATE_DEPENDENCIES}
      COMMENT "Running C++ protobuf compiler on ${PROTO_FILE} ${GENERATE_ADDITIONAL_COMMENT}"
      VERBATIM)

    LIST(APPEND ${GENERATED_SOURCE} "${GENERATED_CC_FILE}")
    LIST(APPEND ${GENERATED_HEADERS} "${GENERATED_H_FILE}")
  ENDFOREACH()

  SET_SOURCE_FILES_PROPERTIES(
    ${${GENERATED_SOURCE}} ${${GENERATED_HEADERS}}
    PROPERTIES GENERATED TRUE)

  SET(${GENERATED_SOURCE} ${${GENERATED_SOURCE}} PARENT_SCOPE)
  SET(${GENERATED_HEADERS} ${${GENERATED_HEADERS}} PARENT_SCOPE)
ENDFUNCTION()

# Generates protobuf .cc file names for a set of .proto files.
FUNCTION(MYSQL_PROTOBUF_GENERATE_CPP_NAMES SRC_NAMES)
  IF(NOT ARGN)
    MESSAGE(SEND_ERROR
  "Error: MYSQL_PROTOBUF_GENERATE_CPP_NAMES() called without any proto files")
    RETURN()
  ENDIF()

  SET(${SRC_NAMES})

  FOREACH(FIL ${ARGN})
    GET_FILENAME_COMPONENT(FIL_WE ${FIL} NAME_WE)
    LIST(APPEND ${SRC_NAMES} "${PROTOBUF_FULL_GENERATE_DIR}/${FIL_WE}.pb.cc")
  ENDFOREACH()

  SET_SOURCE_FILES_PROPERTIES(${${SRC_NAMES}} PROPERTIES GENERATED TRUE)

  SET(${SRC_NAMES} ${${SRC_NAMES}} PARENT_SCOPE)
ENDFUNCTION()


# Generate a static C++ library which contains contains C++ definitions
# of protobuf messages.
#
# This function reuses following parameters from `MYSQL_PROTOBUF_GENERATE_CPP`:
# * OUTPUT_DIRECTORY - `protoc` is going to write C++ source to this
#                        directory.
# * DEPENDENCIES - adds steps which need to be executed before generating
#                  source
# * IMPORT_DIRECTORIES - force `protoc`, to look for dependent `proto` files
#                        in pointed location. If this options is not set, the
#                        function is going to generate import directory list
#                        using location of `proto` files.
# * ADDITIONAL_COMMENT - in case when the build script generates the
#                        same `proto` files twice with different option
#                        then this options allows to differentiate those
#                        runs in build output.
#
# Example:
#  MYSQL_PROTOBUF_GENERATE_CPP_LIBRARY(
#         MY_LIBRARY_WITH_MESSAGES
#         some_path/my1.proto
#         some_path/my2.proto
#         other_path/other_package.proto
#         OUTPUT_DIRECTORY ${CMAKE_BINARY_DIRECTORY}/generated/
#         DEPENDENCIES some_other_target
#    )
#
#  MYSQL_PROTOBUF_GENERATE_CPP(
#         MY_LIBRARY_WITH_MESSAGES
#         some_path/my1.proto
#    )
FUNCTION(MYSQL_PROTOBUF_GENERATE_CPP_LIBRARY TARGET_NAME)
  # Forward other arguments to MYSQL_PROTOBUF_GENERATE_CPP,
  # because of it, `MYSQL_PROTOBUF_GENERATE_CPP_LIBRARY` is going
  # to inherit format of parameters from `MYSQL_PROTOBUF_GENERATE_CPP`
  MYSQL_PROTOBUF_GENERATE_CPP(
    PROTO_SRCS PROTO_HDRS
    ${ARGN}
  )

  MY_INCLUDE_SYSTEM_DIRECTORIES(PROTOBUF)

  ADD_LIBRARY(${TARGET_NAME} STATIC
    ${PROTO_SRCS}
  )
  # Generate it only if needed by other targets
  SET_PROPERTY(TARGET ${TARGET_NAME} PROPERTY EXCLUDE_FROM_ALL TRUE)

  SET(MY_PROTOBUF_FLAGS "")
  SET(MY_PUBLIC_PROTOBUF_FLAGS "")

  IF(MY_COMPILER_IS_GNU_OR_CLANG)
    STRING_APPEND(MY_PUBLIC_PROTOBUF_FLAGS " -Wno-unused-parameter -Wno-undef")

    STRING_APPEND(MY_PROTOBUF_FLAGS
      " -Wno-ignored-qualifiers -Wno-sign-compare -Wno-unused-variable -Wno-undef"
    )

    MY_CHECK_CXX_COMPILER_WARNING("-Wunused-but-set-parameter" HAS_WARN_FLAG)
    IF(HAS_WARN_FLAG)
      STRING_APPEND(MY_PUBLIC_PROTOBUF_FLAGS " ${HAS_WARN_FLAG}")
    ENDIF()

    MY_CHECK_CXX_COMPILER_WARNING("-Wextra-semi" HAS_WARN_FLAG)
    IF(HAS_WARN_FLAG)
      STRING_APPEND(MY_PUBLIC_PROTOBUF_FLAGS " ${HAS_WARN_FLAG}")
    ENDIF()
  ENDIF()

  IF(MSVC)
    IF(WIN32_CLANG)
      SET(MY_PROTOBUF_FLAGS "${MY_PROTOBUF_FLAGS} -Wno-sign-compare")
      SET(MY_PUBLIC_PROTOBUF_FLAGS "${MY_PUBLIC_PROTOBUF_FLAGS} -Wno-sign-compare")
    ELSE()
      SET(MY_PROTOBUF_FLAGS "${MY_PROTOBUF_FLAGS} /wd4018")
    ENDIF()
  ENDIF(MSVC)

  ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS
          "${MY_PROTOBUF_FLAGS} ${MY_PUBLIC_PROTOBUF_FLAGS}"
  )

  SET(MY_PUBLIC_PROTOBUF_FLAGS ${MY_PUBLIC_PROTOBUF_FLAGS} PARENT_SCOPE)
ENDFUNCTION()
