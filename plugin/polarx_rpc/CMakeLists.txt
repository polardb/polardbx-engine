# Copyright (c) 2016, 2021, Alibaba and/or its affiliates. All rights reserved.
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

INCLUDE(${MYSQL_CMAKE_SCRIPT_DIR}/compile_flags.cmake)

INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIRS})
INCLUDE(polarx_rpc_protobuf.cmake)

INCLUDE(${CMAKE_SOURCE_DIR}/storage/innobase/innodb.cmake)

IF (NOT PROTOBUF_FOUND)
    MESSAGE(WARNING "Disabling polarx_rpc plugin.")
ELSE ()
    SET(POLARX_RPC_PROJECT_DIR "${PROJECT_SOURCE_DIR}/plugin/polarx_rpc")

    SET(PROTOBUF_POLARX_RPC_FILES
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_datatypes.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_connection.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_expect.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_expr.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_sql.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_session.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_notice.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_resultset.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_exec_plan.proto"
            "${POLARX_RPC_PROJECT_DIR}/protocol/polarx_physical_backfill.proto"
    )

    SET(GENERATED_LITE_PROTO_FILES)
    # Generate protobuf-lite versions of proto files.
    FOREACH (PROTOBUF_POLARX_RPC_FILE ${PROTOBUF_POLARX_RPC_FILES})
        GET_FILENAME_COMPONENT(POLARX_RPC_NAME_WD ${PROTOBUF_POLARX_RPC_FILE} NAME)
        LIST(APPEND GENERATED_LITE_PROTO_FILES
                ${CMAKE_CURRENT_BINARY_DIR}/protobuf_lite/${POLARX_RPC_NAME_WD})
        ADD_CUSTOM_COMMAND(
                OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/protobuf_lite/${POLARX_RPC_NAME_WD}"
                COMMAND ${CMAKE_COMMAND}
                -D CURRENT_BINARY_DIR="${CMAKE_CURRENT_BINARY_DIR}"
                -D PROTO_FILE="${PROTOBUF_POLARX_RPC_FILE}"
                -D PROTO_FILE_FLAGS="PROTOBUF_LITE"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/process_protobuf_file.cmake
                DEPENDS ${PROTOBUF_POLARX_RPC_FILE})
        SET_SOURCE_FILES_PROPERTIES(
                "${CMAKE_CURRENT_BINARY_DIR}/protobuf_lite/${POLARX_RPC_NAME_WD}"
                PROPERTIES GENERATED TRUE)
    ENDFOREACH ()

    # Be sure to convert all proto files to protobuf_lite before running protoc
    ADD_CUSTOM_TARGET(GenLiteProtos_polarx_rpc ALL DEPENDS ${GENERATED_LITE_PROTO_FILES})

    POLARX_RPC_PROTOBUF_GENERATE_CPP(
            PROTO_SRCS PROTO_HDRS
            PROTO_LITE_SRCS PROTO_LITE_HDRS
            ${PROTOBUF_POLARX_RPC_FILES})

    MY_CHECK_CXX_COMPILER_FLAG("-Wignored-qualifiers" HAVE_NO_IGNORED_QUALIFIERS)
    IF (HAVE_NO_IGNORED_QUALIFIERS)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-ignored-qualifiers")
    ENDIF ()

    MY_CHECK_CXX_COMPILER_FLAG("-Wsign-compare" CXX_HAVE_SIGN_COMPARE)
    IF (CXX_HAVE_SIGN_COMPARE)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-sign-compare")
    ENDIF ()

    MY_CHECK_CXX_COMPILER_FLAG("-Wunused-variable" CXX_HAVE_UNUSED_VARIABLE)
    IF (CXX_HAVE_UNUSED_VARIABLE)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-unused-variable")
    ENDIF ()

    ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-shadow-field")
    ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-unused-const-variable")
    ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-deprecated-declarations")
    ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS "-Wno-pessimizing-move")

    SET(polarx_rpc_PROTOBUF_SRC ${PROTO_LITE_SRCS} ${PROTO_LITE_HDRS})

    IF (MSVC)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} ${PROTO_LITE_SRCS} COMPILE_FLAGS "/wd4018")
    ENDIF (MSVC)

    # done protobuf

    INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR} ${POLARX_RPC_PROJECT_DIR})

    INCLUDE_DIRECTORIES(SYSTEM
            ${BOOST_INCLUDE_DIR}
            ${PROTOBUF_INCLUDE_DIRS}
    )

    ADD_CONVENIENCE_LIBRARY(polarx_rpc_protobuf ${polarx_rpc_PROTOBUF_SRC})

    SET(polarx_rpc_SRC
            "polarx_rpc.cc"
            "server/server_variables.cc"
            "session/session.cc"
            "session/session_base.cc"
            "session/session_manager.cc"
            "session/flow_control.cc"
            "coders/streaming_command_delegate.cc"
            "coders/callback_command_delegate.cc"
            "coders/polarx_encoder.cc"
            "coders/notices.cc"
            "executor/bloomfilter.cc"
            "executor/executor.cc"
            "executor/expr.cc"
            "executor/handler_api.cc"
            "executor/meta.cc"
            "executor/murmurhash3.cc"
            "executor/parse.cc"
            "executor/protocol.cc"
            "executor/physical_backfill.cc"
            "utility/perf.cc"
            "sql_query/query_formatter.cc"
            "sql_query/query_string_builder.cc"
            "sql_query/sql_statement_builder.cc"
            "secure/account_verification_handler.cc"
            "secure/native_verification.cc")

    ADD_COMPILE_FLAGS(${polarx_rpc_SRC} COMPILE_FLAGS "-Wno-shadow-field")
    ADD_COMPILE_FLAGS(${polarx_rpc_SRC} COMPILE_FLAGS "-Wno-unused-const-variable")
    ADD_COMPILE_FLAGS(${polarx_rpc_SRC} COMPILE_FLAGS "-Wno-deprecated-declarations")
    ADD_COMPILE_FLAGS(${polarx_rpc_SRC} COMPILE_FLAGS "-Wno-pessimizing-move")

    ADD_COMPILE_FLAGS(${polarx_rpc_SRC} ${polarx_rpc_PROTOBUF_SRC} COMPILE_FLAGS)

    MYSQL_ADD_PLUGIN(polarx_rpc ${polarx_rpc_SRC} DEFAULT MODULE_OUTPUT_NAME "polarx_rpc")

    TARGET_LINK_LIBRARIES(polarx_rpc polarx_rpc_protobuf)

    # done polarx_rpc module

    # start polarx_rpc_test

    # protobuf full

    ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS)
    ADD_LIBRARY(polarx_rpc_protobuf_full STATIC ${PROTO_SRCS})
    # Generate it only if needed by other targets
    SET_PROPERTY(TARGET polarx_rpc_protobuf_full PROPERTY EXCLUDE_FROM_ALL TRUE)

    # client

    SET(CLIENT_SRC
            tests/client/authentication/mysql41_hash.cc
            tests/client/authentication/password_hasher.cc
            tests/client/authentication/sha256_scramble_generator.cc
            tests/client/xprotocol_impl.cc
            tests/client/xrow_impl.cc
            tests/client/xquery_result_impl.cc
            tests/client/xsession_impl.cc
            tests/client/xconnection_impl.cc
            tests/client/xrow.cc
    )

    ADD_COMPILE_FLAGS(${CLIENT_SRC} COMPILE_FLAGS "-Wno-shadow-field")
    ADD_COMPILE_FLAGS(${CLIENT_SRC} COMPILE_FLAGS "-Wno-unused-const-variable")
    ADD_COMPILE_FLAGS(${CLIENT_SRC} COMPILE_FLAGS "-Wno-deprecated-declarations")
    ADD_COMPILE_FLAGS(${CLIENT_SRC} COMPILE_FLAGS "-Wno-pessimizing-move")

    ADD_COMPILE_FLAGS(${CLIENT_SRC} COMPILE_FLAGS)
    ADD_LIBRARY(polarx_rpc_client STATIC ${CLIENT_SRC})
    ADD_DEPENDENCIES(polarx_rpc_client polarx_rpc_protobuf_full GenError)

    # polarx rpc test

    SET(XTEST_SRC
            tests/driver/driver_command_line_options.cc
            tests/driver/json_to_any_handler.cc
            tests/driver/mysqlx_charset.cc
            tests/driver/mysqlxtest.cc

            tests/driver/common/command_line_options.cc
            tests/driver/common/message_matcher.cc
            tests/driver/common/utils_mysql_parsing.cc
            tests/driver/common/utils_string_parsing.cc

            tests/driver/connector/connection_manager.cc
            tests/driver/connector/mysqlx_all_msgs.cc
            tests/driver/connector/result_fetcher.cc
            tests/driver/connector/session_holder.cc
            tests/driver/connector/warning.cc

            tests/driver/formatters/console.cc
            tests/driver/formatters/message_formatter.cc

            tests/driver/parsers/message_parser.cc

            tests/driver/processor/command_multiline_processor.cc
            tests/driver/processor/command_processor.cc
            tests/driver/processor/dump_message_block_processor.cc
            tests/driver/processor/macro_block_processor.cc
            tests/driver/processor/send_message_block_processor.cc
            tests/driver/processor/sql_block_processor.cc
            tests/driver/processor/sql_stmt_processor.cc
            tests/driver/processor/stream_processor.cc

            tests/driver/processor/commands/command.cc
            tests/driver/processor/commands/expected_error.cc
            tests/driver/processor/commands/expected_warnings.cc
            tests/driver/processor/commands/macro.cc
            tests/driver/processor/commands/mysqlxtest_error_names.cc
    )

    ADD_COMPILE_FLAGS(${XTEST_SRC} COMPILE_FLAGS "-Wno-shadow-field")
    ADD_COMPILE_FLAGS(${XTEST_SRC} COMPILE_FLAGS "-Wno-unused-const-variable")
    ADD_COMPILE_FLAGS(${XTEST_SRC} COMPILE_FLAGS "-Wno-deprecated-declarations")
    ADD_COMPILE_FLAGS(${XTEST_SRC} COMPILE_FLAGS "-Wno-pessimizing-move")

    ADD_COMPILE_FLAGS(${XTEST_SRC} COMPILE_FLAGS)
    MYSQL_ADD_EXECUTABLE(polarx_rpc_test ${XTEST_SRC})
    ADD_DEPENDENCIES(polarx_rpc_test GenError)
    TARGET_LINK_LIBRARIES(polarx_rpc_test
            mysys strings mysqlclient polarx_rpc_client polarx_rpc_protobuf_full ${PROTOBUF_LIBRARY})

ENDIF ()
