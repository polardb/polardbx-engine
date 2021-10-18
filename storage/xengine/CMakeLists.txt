# TODO: Copyrights

#IF (NOT EXISTS "${CMAKE_SOURCE_DIR}/storage/xengine/xengine/Makefile")
#  MESSAGE(SEND_ERROR "Missing Makefile in xengine directory. Try \"git submodule update\".")
#ENDIF()

# suppress warning of new FixedQueue<T>, since using C++17
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-vla -Wno-overloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-missing-format-attribute -Wno-format-security -Wno-non-virtual-dtor -faligned-new=32 -Wno-cast-qual")
set(XENGINE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/storage/xengine/core")
CHECK_FUNCTION_EXISTS(sched_getcpu  HAVE_SCHED_GETCPU)
IF(HAVE_SCHED_GETCPU)
  ADD_DEFINITIONS(-DHAVE_SCHED_GETCPU=1)
ENDIF()

# get a list of xengine library source files
# run with env -i to avoid passing variables
EXECUTE_PROCESS(
  COMMAND env -i ${CMAKE_SOURCE_DIR}/storage/xengine/get_xengine_files.sh
  OUTPUT_VARIABLE SCRIPT_OUTPUT
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
# split the list into lines
STRING(REGEX MATCHALL "[^\n]+" XENGINE_LIB_SOURCES ${SCRIPT_OUTPUT})



INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/sql
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/extra/lz4/lib
  ${CMAKE_SOURCE_DIR}/extra/zstd/lib
  ${CMAKE_SOURCE_DIR}/storage/innobase
  ${CMAKE_SOURCE_DIR}/storage/innobase/include
  ${CMAKE_SOURCE_DIR}/storage/xengine
  ${CMAKE_SOURCE_DIR}/storage/xengine/handler
  ${CMAKE_SOURCE_DIR}/storage/xengine/util
  ${CMAKE_SOURCE_DIR}/storage/xengine/api
  ${CMAKE_SOURCE_DIR}/storage/xengine/dict
  ${CMAKE_SOURCE_DIR}/storage/xengine/index
  ${CMAKE_SOURCE_DIR}/storage/xengine/trx
  ${CMAKE_SOURCE_DIR}/storage/xengine/core
  ${CMAKE_SOURCE_DIR}/storage/xengine/core/include
  ${CMAKE_SOURCE_DIR}/storage/xengine/core/db
  #  ${CMAKE_SOURCE_DIR}/storage/xengine/core/IS
  #${CMAKE_SOURCE_DIR}/storage/xengine/xstream
  #${CMAKE_SOURCE_DIR}/storage/xengine/xstream/crc
)

ADD_DEFINITIONS(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -DHAVE_ZLIB)

# need xstream_extent
#SET(XSTREAM_SOURCES
#  xstream/ds_buffer.cc
#  xstream/ds_local.cc
#  xstream/ds_stdout.cc
#  xstream/datasink.cc
#  xstream/xstream_read.cc
#  xstream/xstream_write.cc
#  xstream/xcrypt_write.cc
#  )

SET(XENGINE_SOURCES
  handler/ha_xengine.cc 
  handler/ha_xengine.h 
  handler/ha_xengine_proto.h
  handler/handler_alter.cc
  handler/handler_alter.h
  handler/xdb_i_s.cc 
  handler/xdb_i_s.h
  api/xengine_api.cc
  api/xengine_api.h
  dict/dd_operations.cc 
  dict/dd_operations.h
  dict/xdb_datadic.cc 
  dict/xdb_datadic.h
  dict/xdb_cf_options.cc 
  dict/xdb_cf_options.h
  dict/xdb_cf_manager.cc 
  dict/xdb_cf_manager.h
  dict/log_ddl.cc 
  dict/log_ddl.h
  dict/xdb_sst_info.cc 
  dict/xdb_sst_info.h
  dict/properties_collector.cc 
  dict/properties_collector.h
  index/xdb_index_merge.cc 
  index/xdb_index_merge.h
  index/event_listener.cc 
  index/event_listener.h
  util/xdb_mutex_wrapper.cc 
  util/xdb_mutex_wrapper.h
  util/xdb_utils.cc 
  util/xdb_utils.h 
  util/xdb_buff.h
  util/xdb_threads.cc 
  util/xdb_threads.h
  util/ib_ut0counter.h
  util/xdb_comparator.h
  util/atomic_stat.h
  ${XENGINE_LIB_SOURCES}
  #${XSTREAM_SOURCES}
)

IF(WITH_FB_TSAN)
  SET(PIC_EXT "_pic")
ELSE()
  SET(PIC_EXT "")
ENDIF()

#ADD_SUBDIRECTORY(${CMAKE_SOURCE_DIR}/storage/xengine/core/IS ${CMAKE_BINARY_DIR}/IS.build)
SET(xengine_static_libs )
IF (NOT "$ENV{WITH_SNAPPY}" STREQUAL "")
  SET(xengine_static_libs ${xengine_static_libs}
  $ENV{WITH_SNAPPY}/lib/libsnappy${PIC_EXT}.a)
  ADD_DEFINITIONS(-DHAVE_SNAPPY)
ENDIF()

IF (NOT "$ENV{WITH_LZ4}" STREQUAL "")
  SET(xengine_static_libs ${xengine_static_libs}
  $ENV{WITH_LZ4}/lib/liblz4${PIC_EXT}.a)
  ADD_DEFINITIONS(-DHAVE_LZ4)
ENDIF()

# link ZSTD by default
SET(WITH_ZSTD 1)
IF (WITH_ZSTD)
#  if (EXISTS ${CMAKE_SOURCE_DIR}/extra/zstd/lib/libzstd${PIC_EXT}.a)
#    MESSAGE("Use existing zstd library ${CMAKE_SOURCE_DIR}/extra/zstd/lib/libzstd${PIC_EXT}.a")
#  else()
#    MESSAGE("compile bundled zstd library")
#    EXECUTE_PROCESS(COMMAND tar zxvf zstd.tar.gz 
#                    WORKING_DIRECTORY  ${CMAKE_SOURCE_DIR}/extra/)
#    EXECUTE_PROCESS(COMMAND make -j
#                    WORKING_DIRECTORY  ${CMAKE_SOURCE_DIR}/extra/zstd/lib)
#  endif()
  SET(xengine_static_libs ${xengine_static_libs} ${ZSTD_LIBRARY})
  ADD_DEFINITIONS(-DHAVE_ZSTD)
ENDIF()

#SET(IS_LIB ${CMAKE_BINARY_DIR}/IS.build/libis_all.a)
#SET(xengine_static_libs ${xengine_static_libs} ${ZLIB_LIBRARY} "-lrt" "-laio" ${IS_LIB})
SET(xengine_static_libs ${xengine_static_libs} ${ZLIB_LIBRARY} "-lrt" "-laio")

if (WITH_TBB)
  include_directories(${CMAKE_SOURCE_DIR}/storage/xengine/core/third-party/tbb/include)
  SET(xengine_static_libs ${xengine_static_libs} ${TBB_LIBRARIES})
endif()


if(WITH_FPAG_COMPACTION)
  add_definitions(-DFPGA_COMPACTION)
  set(FPGA_COMPACTION_LIBS fpga_compaction glog)
  list(APPEND ${xengine_static_libs} ${FPGA_COMPACTION_LIBS})
endif()

IF (WITH_XENGINE_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(xengine_se ${XENGINE_SOURCES} STORAGE_ENGINE DEFAULT STATIC_ONLY
    LINK_LIBRARIES ${xengine_static_libs}
    #  DEPENDENCIES libis_all tbb
  )
ENDIF()

IF(WITH_EMBEDDED_SERVER)
  ADD_SUBDIRECTORY(unittest)
ENDIF()

IF (WITH_XENGINE_STORAGE_ENGINE)
  # TODO: read this file list from src.mk:TOOL_SOURCES
  SET(XENGINE_TOOL_SOURCES
    ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/ldb_tool.cc
    ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/ldb_cmd.cc
    ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/sst_dump_tool.cc
    ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/backup_check_tool.cc 
  )
  MYSQL_ADD_EXECUTABLE(sst_dump ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/sst_dump.cc ${XENGINE_TOOL_SOURCES})
  TARGET_LINK_LIBRARIES(sst_dump xengine_se)

  MYSQL_ADD_EXECUTABLE(ldb ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/ldb.cc ${XENGINE_TOOL_SOURCES})
  TARGET_LINK_LIBRARIES(ldb xengine_se)

  MYSQL_ADD_EXECUTABLE(mysql_ldb ${CMAKE_SOURCE_DIR}/storage/xengine/tools/mysql_ldb.cc ${XENGINE_TOOL_SOURCES})
  TARGET_LINK_LIBRARIES(mysql_ldb xengine_se)

  MYSQL_ADD_EXECUTABLE(backup_check ${CMAKE_SOURCE_DIR}/storage/xengine/core/tools/backup_check.cc ${XENGINE_TOOL_SOURCES})
  TARGET_LINK_LIBRARIES(backup_check xengine_se)
ENDIF()

#ADD_SUBDIRECTORY(xstream)
