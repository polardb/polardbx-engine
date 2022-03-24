cmake_minimum_required(VERSION 2.8)

SET( CMAKE_CXX_COMPILER /opt/rh/devtoolset-2/root/usr/bin/g++ )
message("CMAKE_CXX_COMPILER ${CMAKE_CXX_COMPILER} " )

SET( CMAKE_C_COMPILER /opt/rh/devtoolset-2/root/usr/bin/gcc )
message("CMAKE_C_COMPILER ${CMAKE_C_COMPILER} " )

SET (PRJ_SRC
  ../consensus/algorithm/configuration.cc
  ../consensus/algorithm/raft.cc
  ../consensus/algorithm/raft_server.cc
  ../consensus/algorithm/rd_raft_log.cc
  ../consensus/protocol/raft.pb.cc
  ../consensus/net/easyNet.cc
  ../consensus/service/service.cc
  ../consensus/service/client_service.cc
  ./service/state_machine.cc
  ./service/state_machine_service.cc
  ./service/state_machine_server.cc
  ./service/flags.cc
  ./storage/data_storage.cc
  ./memcached/text_request.cc
  ./memcached/text_response.cc
  ./memcached/memcached_easyNet.cc
  ./memcached/memcached_object.cc
  )

SET (CLIENT_SRC
   ./service/flags.cc
   ./client/cli_sdk.cc
   ./client/dst_cli_sdk.cc
    )

SET (UT_SRC
  unittest/rd_raft_log-t.cc
  unittest/text_request-t.cc
  unittest/text_response-t.cc
  unittest/memcached_object-t.cc  
  unittest/gunit_test_main.cc
  )

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -O0")

SET (GFLAGS_OUTPUT
  ${CMAKE_CURRENT_SOURCE_DIR}/output/lib/libgflags.a
  )
add_custom_command (
  OUTPUT ${GFLAGS_OUTPUT}
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/gflags-2.1.2/ && ([ -f
  Makefile ] || (cmake -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/output .) ) && make -j && make install
  COMMENT "building gflags"
)

add_custom_target (libgflags
  ALL DEPENDS ${GFLAGS_OUTPUT}
  )

SET (LIBMEMCACHED_OUTPUT
  ${CMAKE_CURRENT_SOURCE_DIR}/output/lib/libmemcached.a
  )
add_custom_command (
  OUTPUT ${LIBMEMCACHED_OUTPUT}
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/libmemcached-1.0.18/ && ([ -f
  Makefile ] || (./configure --prefix=${CMAKE_CURRENT_SOURCE_DIR}/output --disable-sasl --enable-static --with-memcached LDFLAGS=-Wl,--as-neede))
   && make -j && make install
  COMMENT "building libmemcached"
)

add_custom_target (libmemcached
  ALL DEPENDS ${LIBMEMCACHED_OUTPUT}
  )

SET (SNAPPY_OUTPUT
  ${CMAKE_CURRENT_SOURCE_DIR}/output/lib/libsnappy.a
  )
add_custom_command (
  OUTPUT ${SNAPPY_OUTPUT}
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/snappy-1.1.1/
  && ([ -f Makefile ] || (./configure --prefix=${CMAKE_CURRENT_SOURCE_DIR}/output))
  && make -j && make install
  COMMENT "building libsnappy"
  )
                  
add_custom_target (libsnappy
  ALL DEPENDS ${SNAPPY_OUTPUT}
  ) 
  
SET (ROCKSDB_OUTPUT
  ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/librocksdb.a
  )
add_custom_command (
  OUTPUT ${ROCKSDB_OUTPUT}
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/ && 
  (export CC=/opt/rh/devtoolset-2/root/usr/bin/gcc && 
   export CXX=/opt/rh/devtoolset-2/root/usr/bin/g++ && 
   make static_lib -j)
  COMMENT "building rocksdb"
)

add_custom_target (librocksdb
  ALL DEPENDS ${ROCKSDB_OUTPUT}
  )

SET (LIBEASY_OUTPUT
  ${CMAKE_CURRENT_SOURCE_DIR}/output/lib/libmyeasy.a
  )

add_custom_command (
  OUTPUT ${LIBEASY_OUTPUT}
  #COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src && ([ -f
  #Makefile ] || (sh bootstrap.sh && ./configure
  #--prefix=${CMAKE_CURRENT_SOURCE_DIR}/output) ) && make -j && make install
  
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/easy/src && ([ -d
  bu ] || (mkdir bu && cd bu && cmake -D
  CMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/output .. && cd ..) ) && cd
  bu && make -j && make install
  COMMENT "building libmyeasy"
)

add_custom_target (libmyeasy
  ALL DEPENDS ${LIBEASY_OUTPUT}
  )

INCLUDE_DIRECTORIES (../consensus/include ${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/include/
  ${CMAKE_CURRENT_SOURCE_DIR}/output/include/easy/ 
  ${CMAKE_CURRENT_SOURCE_DIR}/output/include/)

LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../dependency/rocksdb/
  ${CMAKE_CURRENT_SOURCE_DIR}/output/lib)

ADD_EXECUTABLE(rd_server ./service/server.cc ${PRJ_SRC})
ADD_EXECUTABLE(sample ./sample/sample.cc ${CLIENT_SRC})
ADD_EXECUTABLE(func_test ./test/func_test.cc ${CLIENT_SRC})
ADD_EXECUTABLE(dst_func_test ./test/dst_func_test.cc ${CLIENT_SRC})
ADD_EXECUTABLE(cli ./client/cli.cc ${CLIENT_SRC})
ADD_EXECUTABLE(dstcli ./client/dst_cli.cc ${CLIENT_SRC})
SET(EXECUTABLE_OUTPUT_PATH ./sample)

TARGET_LINK_LIBRARIES (rd_server protobuf gflags pthread librocksdb.a
  libmemcached.a rt libsnappy.a z bz2 lz4 ssl crypto libmyeasy.a) 
TARGET_LINK_LIBRARIES (cli gflags pthread libmemcached.a libmyeasy.a ssl crypto)
TARGET_LINK_LIBRARIES (dstcli gflags pthread libmemcached.a libmyeasy.a ssl crypto)
TARGET_LINK_LIBRARIES (sample gflags pthread libmemcached.a libmyeasy.a ssl crypto)
TARGET_LINK_LIBRARIES (func_test gflags pthread libmemcached.a libmyeasy.a ssl crypto)
TARGET_LINK_LIBRARIES (dst_func_test gflags pthread libmemcached.a libmyeasy.a ssl crypto)

ADD_EXECUTABLE (gtest_run ${PRJ_SRC} ${UT_SRC})
TARGET_LINK_LIBRARIES (gtest_run gtest protobuf gflags pthread librocksdb.a
  libmemcached.a rt libsnappy.a z bz2 lz4 ssl crypto libmyeasy.a)
