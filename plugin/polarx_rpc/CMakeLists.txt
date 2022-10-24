
INCLUDE(${MYSQL_CMAKE_SCRIPT_DIR}/compile_flags.cmake)

SET(XPLUGIN_LOG_PROTOBUF 1 CACHE STRING "Enable logging of protobuf messages")

INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIRS})
INCLUDE(polarx_rpc_protobuf.cmake)

IF(NOT PROTOBUF_FOUND)
    MESSAGE(WARNING "Disabling polarx_rpc plugin.")
ELSE()
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
            )

    SET(GENERATED_LITE_PROTO_FILES)
    # Generate protobuf-lite versions of proto files.
    FOREACH(PROTOBUF_POLARX_RPC_FILE ${PROTOBUF_POLARX_RPC_FILES})
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
    ENDFOREACH()

    # Be sure to convert all proto files to protobuf_lite before running protoc
    ADD_CUSTOM_TARGET(GenLiteProtos_polarx_rpc ALL DEPENDS ${GENERATED_LITE_PROTO_FILES})

    POLARX_RPC_PROTOBUF_GENERATE_CPP(
            PROTO_SRCS PROTO_HDRS
            PROTO_LITE_SRCS PROTO_LITE_HDRS
            ${PROTOBUF_POLARX_RPC_FILES})

    MY_CHECK_CXX_COMPILER_FLAG("-Wignored-qualifiers" HAVE_NO_IGNORED_QUALIFIERS)
    IF(HAVE_NO_IGNORED_QUALIFIERS)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS
                "-Wno-ignored-qualifiers")
    ENDIF()

    MY_CHECK_CXX_COMPILER_FLAG("-Wsign-compare" CXX_HAVE_SIGN_COMPARE)
    IF(CXX_HAVE_SIGN_COMPARE)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS
                "-Wno-sign-compare")
    ENDIF()

    MY_CHECK_CXX_COMPILER_FLAG("-Wunused-variable" CXX_HAVE_UNUSED_VARIABLE)
    IF(CXX_HAVE_UNUSED_VARIABLE)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} COMPILE_FLAGS
                "-Wno-unused-variable")
    ENDIF()

    SET(polarx_rpc_PROTOBUF_SRC ${PROTO_LITE_SRCS} ${PROTO_LITE_HDRS})
    SET(polarx_rpc_PROTOBUF_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf_lite")

    IF(MSVC)
        ADD_COMPILE_FLAGS(${PROTO_SRCS} ${PROTO_LITE_SRCS} COMPILE_FLAGS "/wd4018")
    ENDIF(MSVC)

    # done protobuf

    INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

    INCLUDE_DIRECTORIES(SYSTEM
            ${BOOST_INCLUDE_DIR}
            ${PROTOBUF_INCLUDE_DIRS}
            ${polarx_rpc_PROTOBUF_DIR}
            )

    ADD_CONVENIENCE_LIBRARY(polarx_rpc_protobuf ${polarx_rpc_PROTOBUF_SRC})

    MYSQL_ADD_PLUGIN(polarx_rpc
            polarx_rpc.cc
            server/server_variables.cc
            session/session.cc
            session/session_base.cc
            session/session_manager.cc
            session/flow_control.cc
            coders/streaming_command_delegate.cc
            coders/callback_command_delegate.cc
            coders/polarx_encoder.cc
            coders/notices.cc
            executor/bloomfilter.cc
            executor/executor.cc
            executor/expr.cc
            executor/handler_api.cc
            executor/meta.cc
            executor/murmurhash3.cc
            executor/parse.cc
            executor/protocol.cc
            utility/perf.cc
            sql_query/query_formatter.cc
            sql_query/query_string_builder.cc
            sql_query/sql_statement_builder.cc
            secure/account_verification_handler.cc
            secure/native_verification.cc
            DEFAULT MODULE_OUTPUT_NAME "polarx_rpc")

    TARGET_LINK_LIBRARIES(polarx_rpc polarx_rpc_protobuf)

ENDIF()
