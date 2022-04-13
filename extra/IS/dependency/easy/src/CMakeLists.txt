cmake_minimum_required(VERSION 2.8)

SET (HEADER_FILE
  thread/easy_uthread.h
  util/easy_hash.h
  util/easy_pool.h
  util/easy_buf.h
  util/easy_inet.h
  util/easy_array.h
  util/easy_time.h
  util/easy_string.h
  io/easy_client.h
  io/ev.h
  io/easy_socket.h
  io/easy_summary.h
  io/easy_file.h
  io/easy_baseth_pool.h
  io/easy_request.h
  io/easy_ssl.h
  io/easy_log.h
  io/easy_io_struct.h
  io/easy_io.h
  io/easy_message.h
  io/easy_connection.h
  packet/easy_kfc_handler.h
  packet/easy_simple_handler.h
  packet/http/http_parser.h
  packet/http/easy_http_handler.h
  packet/easy_tbnet.h
  include/easy_define.h
  include/easy_list.h
  include/easy_atomic.h
  )

SET (PRJ_SRC
  thread/easy_uthread.c
  util/easy_string.c
  util/easy_pool.c
  util/easy_time.c
  util/easy_hash.c
  util/easy_inet.c
  util/easy_array.c
  util/easy_buf.c
  io/easy_io.c
  io/easy_file.c
  io/easy_socket.c
  io/easy_request.c
  io/easy_baseth_pool.c
  io/easy_summary.c
  io/easy_ssl.c
  io/easy_message.c
  io/easy_client.c
  io/easy_log.c
  io/easy_connection.c
  io/ev.c
  packet/easy_kfc_handler.c
  packet/http/easy_http_handler.c
  packet/http/http_parser.c
  )
IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -D_GNU_SOURCE -D__STDC_LIMIT_MACROS -Wall -Werror -fPIC -fno-strict-aliasing -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GNU_SOURCE -D__STDC_LIMIT_MACROS -O0")
ELSE()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -D_GNU_SOURCE -D__STDC_LIMIT_MACROS -Wall -Werror -fPIC -fno-strict-aliasing -O0 -DEASY_SPIN_USE_SYS")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GNU_SOURCE -D__STDC_LIMIT_MACROS -O0 -DEASY_SPIN_USE_SYS")
ENDIF()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DOPENSSL_API_COMPAT=0x00908000L")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOPENSSL_API_COMPAT=0x00908000L")
INCLUDE_DIRECTORIES (include io packet thread util packet/http)

SET(SSL_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/../../../../openssl-1.1.0l/include/)
INCLUDE_DIRECTORIES(SYSTEM ${SSL_INCLUDE_DIRS})

add_library (myeasy STATIC ${PRJ_SRC})
TARGET_LINK_LIBRARIES (myeasy ev)

install(TARGETS myeasy DESTINATION lib)
install(FILES ${HEADER_FILE} DESTINATION include/easy)
