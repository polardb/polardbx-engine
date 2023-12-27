cmake_minimum_required(VERSION 3.0)

SET(HEADER_FILE
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
  #io/easy_ssl.h
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

SET(PRJ_SRC
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
  #io/easy_ssl.c
  io/easy_message.c
  io/easy_client.c
  io/easy_connection.c
  io/ev.c
  packet/easy_kfc_handler.c
  packet/http/easy_http_handler.c
  packet/http/http_parser.c
  )

LIST(APPEND PRJ_SRC_INTERNAL ${PRJ_SRC} io/easy_log.c)
LIST(APPEND PRJ_SRC io/easy_log.cc)

IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DEASY_SPIN_USE_SYS")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DEASY_SPIN_USE_SYS")
ENDIF ()
INCLUDE_DIRECTORIES(include io packet thread util packet/http)

add_library(myeasy STATIC ${PRJ_SRC})
TARGET_LINK_LIBRARIES(myeasy -pthread)
add_dependencies(myeasy GenError)
add_library(myeasy_internal STATIC ${PRJ_SRC_INTERNAL})
TARGET_LINK_LIBRARIES(myeasy_internal -pthread)