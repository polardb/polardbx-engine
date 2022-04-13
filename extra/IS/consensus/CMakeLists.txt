cmake_minimum_required(VERSION 2.8)

SET (CONSENSUS_SRC_FILES
  algorithm/configuration.cc
  algorithm/paxos.cc
  algorithm/witness.cc
  #  algorithm/paxos_log.cc
  algorithm/rd_paxos_log.cc
  algorithm/file_paxos_log.cc
  algorithm/mem_paxos_log.cc
  algorithm/paxos_server.cc
  algorithm/paxos_log_cache.cc
  algorithm/paxos_error.cc
  algorithm/msg_compress.cc
  algorithm/crc.cc
  algorithm/log_meta_cache.cc
  client/learner_client.cc
  client/learner_server.cc
  client/single_leader.cc
  protocol/paxos.pb.cc
  net/easyNet.cc
  service/service.cc
  service/thread_timer.cc
  service/client_service.cc
  )

SET (CONSENSUS_HEADER_FILES
  include/net.h
  include/paxos_server.h
  include/client_service.h
  include/paxos_configuration.h
  include/paxos.h
  include/witness.h
  include/files.h
  include/paxos_log.h
  include/rd_paxos_log.h
  include/file_paxos_log.h
  include/mem_paxos_log.h
  include/consensus.h
  include/easyNet.h
  include/service.h
  include/thread_timer.h
  include/learner_client.h
  include/learner_server.h
  include/single_leader.h
  include/single_process_queue.h
  include/paxos_log_cache.h
  include/paxos_error.h
  include/msg_compress.h
  include/crc.h
  include/log_meta_cache.h
  include/paxos_option.h
  protocol/paxos.pb.h
  )

OPTION(WITH_DEBUG "Debug Mode" ON)
OPTION(WITH_DEBUG_LOG_CACHE "Debug Mode For Log Cache" OFF)
OPTION(WITH_TSAN "ThreadSanitizer Mode" OFF)
OPTION(WITH_ASAN "AddressSanitizer Mode" OFF)
OPTION(WITH_GPT "GooglePerfTools Enable" OFF)
OPTION(WITH_PROTOBUF3 "Build with protobuf3" OFF)
OPTION(MINIMAL_MAKE "Only make libaliconsensus.a" OFF)

IF(WITH_TSAN)
  MESSAGE(STATUS "Build with ThreadSanitizer Enable!")
  SET (EXTRA_SAN_FLAG -fsanitize=thread)
ENDIF()

IF(WITH_ASAN)
  MESSAGE(STATUS "Build with AddressSanitizer Enable!")
  SET (EXTRA_SAN_FLAG "-fsanitize=address")
ENDIF()

IF(WITH_GPT)
  MESSAGE(STATUS "Build with GooglePerfTools Enable!")
  SET (EXTRA_LINK tcmalloc_and_profiler)
ENDIF()

IF(WITH_DEBUG)
  MESSAGE(STATUS "Build with debug mode!")
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -O0 -fno-omit-frame-pointer ${EXTRA_SAN_FLAG}")
ELSE()
  MESSAGE(STATUS "Build with release mode!")
  SET(CMAKE_BUILD_TYPE "RelWithDebInfo")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -O3 -fPIC")
ENDIF()

IF(NOT MINIMAL_MAKE)
  MESSAGE(STATUS "Build with full mode!")
ELSE()
  MESSAGE(STATUS "Build with minimal mode!")
ENDIF()

INCLUDE_DIRECTORIES (include ${CMAKE_INSTALL_PREFIX}/include/easy
  ${CMAKE_CURRENT_SOURCE_DIR}/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/mysql/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/io/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/util/
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src/thread/
  )

SET (LIBEASY_OUTPUT
  ${CMAKE_INSTALL_PREFIX}/lib/libmyeasy.a
  )
IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
add_custom_command (
  OUTPUT ${LIBEASY_OUTPUT}
  COMMAND [ -d output ] && mkdir output || echo "create output"
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src &&
  (rm -fr bu && mkdir bu && cd bu && cmake -D
  CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} .. && cd ..) && cd
  bu && make -j && make install
  COMMENT "building libmyeasy"
)
ELSE()
add_custom_command (
  OUTPUT ${LIBEASY_OUTPUT}
  COMMAND [ -d output ] && mkdir output || echo "create output"
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src &&
  (rm -fr bu && mkdir bu && cd bu && cmake -D
  CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DEASY_SPIN_USE_SYS=1 .. && cd ..) && cd
  bu && make -j && make install
  COMMENT "building libmyeasy"
)
ENDIF()
add_custom_target (libmyeasy
  ALL DEPENDS ${LIBEASY_OUTPUT}
  )

#IF(NOT MINIMAL_MAKE)
#  SET (ROCKSDB_OUTPUT
#    ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/librocksdb.a
#    )
#  add_custom_command (
#    OUTPUT ${ROCKSDB_OUTPUT}
#    COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/ &&
#    (make clean) &&
#    (make static_lib -j)
#    COMMENT "building rocksdb"
#    )
#  add_custom_target (librocksdb
#    ALL DEPENDS ${ROCKSDB_OUTPUT}
#    )
#ENDIF()

link_directories(
  ${CMAKE_INSTALL_PREFIX}/lib
#  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/
  /usr/lib64/mysql/
  /usr/lib/mysql/
  )

IF(WITH_PROTOBUF3)
  SET (PROTOBUF_OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/protobuf-3.6.1/bu/lib64/libprotobuf.a)
  add_custom_command (
    OUTPUT ${PROTOBUF_OUTPUT}
    COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/protobuf-3.6.1/cmake &&
    (cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=../bu) &&
    (make install -j) && ([ -d ../bu/lib64 ] || mv ../bu/lib ../bu/lib64)
    COMMENT "building protobuf3"
  )
  add_custom_target (libprotobuf
    ALL DEPENDS ${PROTOBUF_OUTPUT}
  )
  INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../dependency/protobuf-3.6.1/bu/include/)
  link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../dependency/protobuf-3.6.1/bu/lib64/)
ENDIF()

ADD_LIBRARY (aliconsensus STATIC ${CONSENSUS_SRC_FILES})
TARGET_LINK_LIBRARIES (aliconsensus protobuf pthread snappy z
  bz2 lz4 rt ssl crypto myeasy ${EXTRA_LINK})

install(TARGETS aliconsensus DESTINATION lib)
install(FILES ${CONSENSUS_HEADER_FILES} DESTINATION include/aliconsensus)

set( CMAKE_EXPORT_COMPILE_COMMANDS 1 )
