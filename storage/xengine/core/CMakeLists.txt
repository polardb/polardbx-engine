# Prerequisites for Windows:
#     This cmake build is for Windows 64-bit only.
#
# Prerequisites:
#     You must have at least Visual Studio 2015 Update 3. Start the Developer Command Prompt window that is a part of Visual Studio installation.
#     Run the build commands from within the Developer Command Prompt window to have paths to the compiler and runtime libraries set.
#     You must have git.exe in your %PATH% environment variable.
#
# To build Rocksdb for Windows is as easy as 1-2-3-4-5:
#
# 1. Update paths to third-party libraries in thirdparty.inc file
# 2. Create a new directory for build artifacts
#        mkdir build
#        cd build
# 3. Run cmake to generate project files for Windows, add more options to enable required third-party libraries.
#    See thirdparty.inc for more information.
#        sample command: cmake -G "Visual Studio 14 Win64" -DGFLAGS=1 -DSNAPPY=1 -DJEMALLOC=1 -DJNI=1 ..
# 4. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m ot use all avail cores)
#        msbuild rocksdb.sln
#
#        rocksdb.sln build features exclusions of test only code in Release. If you build ALL_BUILD then everything
#        will be attempted but test only code does not build in Release mode.
#
# 5. And release mode (/m[:<N>] is also supported)
#        msbuild rocksdb.sln /p:Configuration=Release
#
# Linux:
#
# 1. Install a recent toolchain such as devtoolset-3 if you're on a older distro. C++11 required.
# 2. mkdir build; cd build
# 3. cmake ..
# 4. make -j

cmake_minimum_required(VERSION 2.6)
project(xengine)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

OPTION(WITH_QUERY_TRACE "WITH_QUERY_TRACE" ON)
if (WITH_QUERY_TRACE)
  add_definitions(-DWITH_QUERY_TRACE)
endif()

OPTION(WITH_STRESS_CHECK "WITH_STRESS_CHECK" OFF)
if (WITH_STRESS_CHECK)
  add_definitions(-DWITH_STRESS_CHECK)
endif()

if(MSVC)
  include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.inc)
else()
  option(WITH_JEMALLOC "build with JeMalloc" OFF)
  if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    # FreeBSD has jemaloc as default malloc
    # but it does not have all the jemalloc files in include/...
    set(WITH_JEMALLOC ON)
  else()
    if(WITH_JEMALLOC)
      find_package(JeMalloc REQUIRED)
      #add_definitions(-DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE)
      include_directories(${JEMALLOC_INCLUDE_DIR})
    endif()
  endif()
  option(WITH_SNAPPY "build with SNAPPY" OFF)
  if(WITH_SNAPPY)
    find_package(snappy REQUIRED)
    add_definitions(-DSNAPPY)
    include_directories(${SNAPPY_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${SNAPPY_LIBRARIES})
  endif()
endif()

if(WIN32)
  execute_process(COMMAND powershell -Command "Get-Date -format MM_dd_yyyy" OUTPUT_VARIABLE DATE)
  execute_process(COMMAND powershell -Command "Get-Date -format HH:mm:ss" OUTPUT_VARIABLE TIME)
  string(REGEX REPLACE "(..)_(..)_..(..).*" "\\1/\\2/\\3" DATE "${DATE}")
  string(REGEX REPLACE "(..):(.....).*" " \\1:\\2" TIME "${TIME}")
  set(GIT_DATE_TIME "${DATE} ${TIME}")
else()
  execute_process(COMMAND date "+%Y/%m/%d %H:%M:%S" OUTPUT_VARIABLE DATETIME)
  string(REGEX REPLACE "\n" "" DATETIME ${DATETIME})
  set(GIT_DATE_TIME "${DATETIME}")
endif()

find_package(Git)

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} --git-dir=${CMAKE_CURRENT_SOURCE_DIR}/.git rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()

string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")

if(NOT WIN32)
  execute_process(COMMAND
      "./build_tools/version.sh" "full"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE ROCKSDB_VERSION
  )
  string(STRIP "${ROCKSDB_VERSION}" ROCKSDB_VERSION)
  execute_process(COMMAND
      "./build_tools/version.sh" "major"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE ROCKSDB_VERSION_MAJOR
  )
  string(STRIP "${ROCKSDB_VERSION_MAJOR}" ROCKSDB_VERSION_MAJOR)
endif()

if(WIN32)
  option(WITH_AVX2 "build with AVX2" ON)
  if(WITH_AVX2)
    if(MSVC)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
    else()
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
    endif()
  endif()
else()
  option(WITH_SSE42 "build with SSE4.2" ON)
  if(WITH_SSE42)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
  endif()
endif()

set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(util/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
add_library(build_version OBJECT ${BUILD_VERSION_CC})
target_include_directories(build_version PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/util)

#option(WITH_IS "build with IS" ON)
#if (WITH_IS)
#  message(STATUS "IS library enabled")
#  set (IS_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/IS)
#  if (NOT EXISTS ${IS_ROOT_DIR})
#    message(STATUS "${IS_ROOT_DIR} is not exists, install IS first.")
#  else()
#    find_package(IS REQUIRED)
#    if (IS_FOUND)
#      #message(STATUS "found is dir: ${IS_INCLUDE_DIR} ${IS_LIBRARIES}")
#      include_directories(${IS_INCLUDE_DIR})
#      include_directories(${IS_INCLUDE_DIR}/memory)
#      include_directories(${IS_INCLUDE_DIR}/xpaxos/include)
#      include_directories(${IS_INCLUDE_DIR}/dependency/tbb/include)
#      list(APPEND THIRDPARTY_LIBS ${IS_LIBRARIES})
#      if(WITH_TBB)
#        list(APPEND THIRDPARTY_LIBS ${TBB_LIBRARIES})
#      endif()
#    endif()
#  endif()
#endif()

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /W3 /wd4127 /wd4800 /wd4996 /wd4351")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -W -Wextra -Wall")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")
  if(MINGW)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0")
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -fno-omit-frame-pointer -DNDEBUG")
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
    if(HAVE_OMIT_LEAF_FRAME_POINTER)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
    endif()
  endif()
endif()

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" ON)
if(FAIL_ON_WARNINGS)
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
  else() # assume GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()

option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  add_definitions(-DROCKSDB_TSAN_RUN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -ldl -pthread -static-libasan")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -pthread -D_GLIBCXX_USE_CXX11_ABI=0")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -pthread")
  if(WITH_JEMALLOC)
    message(FATAL "ASAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_TSAN "build with TSAN" OFF)
if(WITH_TSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pthread -pie")
  add_definitions(-DROCKSDB_TSAN_RUN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -pthread -fPIC")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -pthread -fPIC")
  if(WITH_JEMALLOC)
    message(FATAL "TSAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_UBSAN "build with UBSAN" OFF)
if(WITH_UBSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
  if(WITH_JEMALLOC)
    message(FATAL "UBSAN does not work well with JeMalloc")
  endif()
endif()

# Used to run CI build and tests so we can run faster
set(OPTIMIZE_DEBUG_DEFAULT 0)        # Debug build is unoptimized by default use -DOPTDBG=1 to optimize

if(DEFINED OPTDBG)
   set(OPTIMIZE_DEBUG ${OPTDBG})
else()
   set(OPTIMIZE_DEBUG ${OPTIMIZE_DEBUG_DEFAULT})
endif()

if(MSVC)
  if((${OPTIMIZE_DEBUG} EQUAL 1))
    message(STATUS "Debug optimization is enabled")
    set(CMAKE_CXX_FLAGS_DEBUG "/Oxt /MDd")
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm /MDd")
  endif()
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /MD")

  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -D_GLIBCXX_USE_CXX11_ABI=0")
endif()

option(ROCKSDB_LITE "Build RocksDBLite version" OFF)
if(ROCKSDB_LITE)
  add_definitions(-DROCKSDB_LITE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
  add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    add_definitions(-DIOS_CROSS_COMPILE -DROCKSDB_LITE)
    # no debug info for IOS, that will make our library big
    add_definitions(-DNDEBUG)
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
  if(MINGW)
    add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
  endif()
endif()

if(NOT WIN32)
  add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)

if(WITH_FALLOCATE)
  include(CheckCSourceCompiles)
  CHECK_C_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DROCKSDB_FALLOCATE_PRESENT)
  endif()
endif()

#include(CheckFunctionExists)
#CHECK_FUNCTION_EXISTS(malloc_usable_size HAVE_MALLOC_USABLE_SIZE)
option(HAVE_MALLOC_USABLE_SIZE "HAVE_MALLOC_USABLE_SIZE" OFF)
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DROCKSDB_MALLOC_USABLE_SIZE)
endif()

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gmock-1.7.0/fused-src)
find_package(Threads REQUIRED)

add_subdirectory(third-party/gmock-1.7.0/fused-src/)

#support gflags using system's
find_package(gflags REQUIRED)
# Note: maybe need google or gflags according to system's version
add_definitions(-DGFLAGS=gflags)
#add_definitions(-DGFLAGS=google)

# import ADD_CONVENIENCE_LIBRARY from MySQL cmake util module
INCLUDE(${CMAKE_SOURCE_DIR}/../../../cmake/libutils.cmake)
# import zlib related macroes
INCLUDE(zlib)
# import zstd related macroes
INCLUDE(zstd)

# Add bundled or system zlib.
MYSQL_CHECK_ZLIB_WITH_COMPRESS()
IF(BUILD_BUNDLED_ZLIB)
  SET(ZLIB_HOME ${CMAKE_SOURCE_DIR}/../../../extra/zlib)
  INCLUDE_DIRECTORIES(${ZLIB_HOME})
  BUILD_MYSQL_BUNDLED_ZLIB(${ZLIB_HOME} ${CMAKE_BINARY_DIR}/third-party/zlib)
  SET(ZLIB_LIBRARY ${CMAKE_BINARY_DIR}/archive_output_directory/libz.a)
ENDIF()
add_definitions(-DZLIB)
list(APPEND THIRDPARTY_LIBS ${ZLIB_LIBRARY})

# Add bundled or system zstd.
MYSQL_CHECK_ZSTD()
IF(BUILD_BUNDLED_ZSTD)
  SET(ZSTD_HOME ${CMAKE_SOURCE_DIR}/../../../extra/zstd)
  #  INCLUDE_DIRECTORIES(${ZSTD_HOME}/lib)
  INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../../../include ${ZSTD_HOME}/lib)
  BUILD_MYSQL_BUNDLED_ZSTD(${ZSTD_HOME} ${CMAKE_BINARY_DIR}/third-party/zstd)
  SET(ZSTD_LIBRARY ${CMAKE_BINARY_DIR}/archive_output_directory/libzstd.a)
ENDIF()
ADD_DEFINITIONS(-DZSTD)
list(APPEND THIRDPARTY_LIBS ${ZSTD_LIBRARY})

#for zstd compile to find my_compiler.h
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../../../include)

IF(WITH_TBB)
  MESSAGE("Building TBB...${CMAKE_CXX_FLAGS}")
  SET (TBB_LIB
    ${CMAKE_BINARY_DIR}/third-party/tbb/build/lib/libtbb.so
    ${CMAKE_BINARY_DIR}/third-party/tbb/build/lib/libtbb.so.2
    #    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/build/lib/libtbbmalloc.so
    #    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/build/lib/libtbbmalloc.so.2
    #    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/build/lib/libtbbmalloc_proxy.so.2
  )
  EXECUTE_PROCESS(COMMAND tar -xzf ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb.tar.gz
                   WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/third-party/)
  EXECUTE_PROCESS(COMMAND make clean 
                  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/third-party/tbb)
  EXECUTE_PROCESS(COMMAND make CFLAGS=-fPIC CXXFLAGS=-fPIC -j
                  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/third-party/tbb)
  include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/include)
  list(APPEND THIRDPARTY_LIBS ${TBB_LIB})
ENDIF()
# Main library source code

set(SOURCES
        cache/clock_cache.cc
        cache/lru_cache.cc
        cache/sharded_cache.cc
        cache/row_cache.cc
        db/builder.cc
        db/c.cc
        db/column_family.cc
        db/compacted_db_impl.cc
        #        db/major_compaction_iterator.cc
        db/convenience.cc
        db/db_filesnapshot.cc
        db/db_impl.cc
        db/db_impl_write.cc
        db/db_impl_compaction_flush.cc
        db/db_impl_files.cc
        db/db_impl_open.cc
        db/db_impl_debug.cc
        db/db_impl_experimental.cc
        db/db_impl_readonly.cc
        db/db_info_dumper.cc
        db/db_iter.cc
        db/dbformat.cc
        db/dump_job.cc
        db/event_helpers.cc
        db/experimental.cc
        db/file_indexer.cc
        db/flush_job.cc
        db/flush_scheduler.cc
        db/forward_iterator.cc
        db/internal_stats.cc
        db/log_reader.cc
        db/log_writer.cc
        db/managed_iterator.cc
        db/memtable.cc
        db/memtable_list.cc
        db/merge_helper.cc
        db/merge_operator.cc
        db/range_del_aggregator.cc
        db/recovery_point.cc
        db/repair.cc
        db/snapshot_impl.cc
        db/table_cache.cc
        db/table_properties_collector.cc
        db/transaction_log_impl.cc
        db/version_builder.cc
        db/version_edit.cc
        db/version_set.cc
        db/wal_manager.cc
        db/write_batch.cc
        db/write_batch_base.cc
        db/write_controller.cc
        db/write_thread.cc
        db/batch_group.cc
        db/pipline_queue_manager.cc
        db/replay_task.cc
        db/replay_thread_pool.cc
        db/replay_threadpool_executor.cc
        env/env.cc
        env/env_chroot.cc
        env/env_hdfs.cc
        env/memenv.cc
        logger/logger.cc
        memory/chunk_allocator.cc
        memory/mod_info.cc
        memory/alloc_mgr.cc
        memtable/hash_cuckoo_rep.cc
        memtable/hash_linklist_rep.cc
        memtable/hash_skiplist_rep.cc
        memtable/memtable_allocator.cc
        memtable/skiplistrep.cc
        memtable/art.cc
        memtable/art_node.cc
        memtable/artrep.cc
        memtable/vectorrep.cc
        monitoring/histogram.cc
        monitoring/histogram_windowing.cc
        monitoring/instrumented_mutex.cc
        monitoring/iostats_context.cc
        monitoring/perf_level.cc
        monitoring/query_perf_context.cc
        monitoring/statistics.cc
        monitoring/thread_status_impl.cc
        monitoring/thread_status_updater.cc
        monitoring/thread_status_util.cc
        monitoring/thread_status_util_debug.cc
        options/cf_options.cc
        options/db_options.cc
        options/options.cc
        options/options_helper.cc
        options/options_parser.cc
        options/options_sanity_check.cc
        port/stack_trace.cc
        storage/change_info.cc
        storage/data_file.cc
        storage/extent_meta_manager.cc
        storage/extent_space.cc
        storage/extent_space_manager.cc
        storage/io_extent.cc
        storage/large_object_extent_manager.cc
        storage/multi_version_extent_meta_layer.cc
        #storage/shrink_extent_spaces.cc
        storage/shrink_job.cc
        storage/storage_common.cc
        storage/storage_log_entry.cc
        storage/storage_manager.cc
        storage/storage_meta_struct.cc
        storage/storage_logger.cc
        storage/table_space.cc
        table/adaptive_table_factory.cc
        table/block.cc
        table/block_based_filter_block.cc
        table/block_based_table_builder.cc
        table/block_based_table_factory.cc
        table/block_based_table_reader.cc
        table/extent_table_builder.cc
        table/extent_table_factory.cc
        table/extent_table_reader.cc
        table/block_builder.cc
        table/block_prefix_index.cc
        table/bloom_block.cc
        table/cuckoo_table_builder.cc
        table/cuckoo_table_factory.cc
        table/cuckoo_table_reader.cc
        table/flush_block_policy.cc
        table/format.cc
        table/full_filter_block.cc
        table/filter_manager.cc
        table/get_context.cc
        table/index_builder.cc
        table/iterator.cc
        table/merging_iterator.cc
        table/meta_blocks.cc
        table/partitioned_filter_block.cc
        table/persistent_cache_helper.cc
        table/plain_table_builder.cc
        table/plain_table_factory.cc
        table/plain_table_index.cc
        table/plain_table_key_coding.cc
        table/plain_table_reader.cc
        table/sst_file_writer.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        table/sstable_scan_iterator.cc
        tools/backup_check_tool.cc
        tools/db_bench_tool.cc
        tools/dump/db_dump_tool.cc
        tools/ldb_cmd.cc
        tools/ldb_tool.cc
        tools/sst_dump_tool.cc
        util/arena.cc
        #util/auto_roll_logger.cc
        util/bloom.cc
        util/coding.cc
        util/compaction_job_stats_impl.cc
        util/comparator.cc
        util/concurrent_arena.cc
        util/crc32c.cc
        util/delete_scheduler.cc
        util/dynamic_bloom.cc
        util/event_logger.cc
        util/ebr.cc
        util/file_reader_writer.cc
        util/concurrent_direct_file_writer.cc
        util/file_util.cc
        util/filename.cc
        util/filter_policy.cc
        util/hash.cc
        #util/logger_v2.cc
        util/log_buffer.cc
        util/murmurhash.cc
        util/random.cc
        util/rate_limiter.cc
        util/slice.cc
        util/sst_file_manager_impl.cc
        util/status.cc
        util/status_message.cc
        util/string_util.cc
        util/sync_point.cc
        util/testutil.cc
        util/thread_local.cc
        util/threadpool_imp.cc
        util/transaction_test_util.cc
        util/xxhash.cc
        util/to_string.cc
        util/misc_utility.cc
        #util/print_util.cc
        util/memory_stat.cc
        util/aio_wrapper.cc
        #util/alloc_mgr.cc
        #util/mod_info.cc
        compact/meta_data.cc
        compact/reuse_block_merge_iterator.cc
        compact/range_iterator.cc
        #        compact/minor_compaction.cc
        compact/compaction.cc
        compact/mt_ext_compaction.cc
        compact/split_compaction.cc
        compact/compaction_tasks_picker.cc
        compact/compaction_stats.cc
        compact/compaction_job.cc
        compact/task_type.cc
        compact/compaction_iterator.cc
        compact/new_compaction_iterator.cc
        fpga/multi_way_block_merge.cc
        fpga/multi_way_block_merge_stats.cc
        fpga/fpga_compaction_job.cc
        utilities/backupable/backupable_db.cc
        utilities/blob_db/blob_db.cc
        utilities/checkpoint/checkpoint.cc
        utilities/checkpoint/hotbackup_impl.cc
        utilities/col_buf_decoder.cc
        utilities/col_buf_encoder.cc
        utilities/column_aware_encoding_util.cc
        utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc
        utilities/date_tiered/date_tiered_db_impl.cc
        utilities/document/document_db.cc
        utilities/document/json_document.cc
        utilities/document/json_document_builder.cc
        utilities/env_mirror.cc
        utilities/env_timed.cc
        utilities/geodb/geodb_impl.cc
        utilities/leveldb_options/leveldb_options.cc
        utilities/lua/rocks_lua_compaction_filter.cc
        utilities/memory/memory_util.cc
        utilities/merge_operators/max.cc
        utilities/merge_operators/put.cc
        utilities/merge_operators/string_append/stringappend.cc
        utilities/merge_operators/string_append/stringappend2.cc
        utilities/merge_operators/uint64add.cc
        utilities/option_change_migration/option_change_migration.cc
        utilities/options/options_util.cc
        utilities/persistent_cache/block_cache_tier.cc
        utilities/persistent_cache/block_cache_tier_file.cc
        utilities/persistent_cache/block_cache_tier_metadata.cc
        utilities/persistent_cache/persistent_cache_tier.cc
        utilities/persistent_cache/volatile_tier_impl.cc
        utilities/redis/redis_lists.cc
        utilities/simulator_cache/sim_cache.cc
        utilities/spatialdb/spatial_db.cc
        utilities/table_properties_collectors/compact_on_deletion_collector.cc
        utilities/transactions/optimistic_transaction_db_impl.cc
        utilities/transactions/optimistic_transaction_impl.cc
        utilities/transactions/transaction_base.cc
        utilities/transactions/transaction_db_impl.cc
        utilities/transactions/transaction_db_mutex_impl.cc
        utilities/transactions/transaction_impl.cc
        utilities/transactions/transaction_lock_mgr.cc
        utilities/transactions/transaction_util.cc
        utilities/ttl/db_ttl_impl.cc
        utilities/write_batch_with_index/write_batch_with_index.cc
        utilities/write_batch_with_index/write_batch_with_index_internal.cc
        utilities/parallel_read/parallel_read.cc
    )

if(WIN32)
  list(APPEND SOURCES
    port/win/io_win.cc
    port/win/env_win.cc
    port/win/env_default.cc
    port/win/port_win.cc
    port/win/win_logger.cc
    port/win/win_thread.cc
    port/win/xpress_win.cc)
else()
  list(APPEND SOURCES
    port/port_posix.cc
    env/env_posix.cc
    env/io_posix.cc)
endif()

if(WITH_FPAG_COMPACTION)
  add_definitions(-DFPGA_COMPACTION)
  set(FPGA_COMPACTION_LIBS fpga_compaction glog)
  list(APPEND THIRDPARTY_LIBS ${FPGA_COMPACTION_LIBS})
endif()

option(WITH_LIBRADOS "Build with librados" OFF)
if(WITH_LIBRADOS)
list(APPEND SOURCES
utilities/env_librados.cc)
list(APPEND THIRDPARTY_LIBS rados)
endif()

# add common target to build objects from files for shared/static library target
add_library(xengine-objs OBJECT ${SOURCES})
set_property(TARGET xengine-objs PROPERTY POSITION_INDEPENDENT_CODE 1)
set(LIB_OBJS $<TARGET_OBJECTS:xengine-objs> $<TARGET_OBJECTS:build_version>)

if(WIN32)
  set(SYSTEM_LIBS ${SYSTEM_LIBS} Shlwapi.lib Rpcrt4.lib)
  set(XENGINE_STATIC_LIB xenginelib${ARTIFACT_SUFFIX})
  set(XENGINE_IMPORT_LIB xengine${ARTIFACT_SUFFIX})
  set(LIBS ${XENGINE_STATIC_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
else()
  set(SYSTEM_LIBS ${CMAKE_THREAD_LIBS_INIT} rt aio)
  set(XENGINE_STATIC_LIB xengine${ARTIFACT_SUFFIX})
  set(XENGINE_SHARED_LIB xengine-shared)
  set(XENGINE_IMPORT_LIB ${XENGINE_SHARED_LIB})
  #set(LIBS ${XENGINE_SHARED_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set(LIBS ${XENGINE_STATIC_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

  add_library(${XENGINE_SHARED_LIB} SHARED ${LIB_OBJS})
  target_link_libraries(${XENGINE_SHARED_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${XENGINE_SHARED_LIB} PROPERTIES
                        LINKER_LANGUAGE CXX
                        VERSION ${ROCKSDB_VERSION}
                        SOVERSION ${ROCKSDB_VERSION_MAJOR}
                        CXX_STANDARD 11
                        OUTPUT_NAME "xengine")
endif()

add_library(${XENGINE_STATIC_LIB} STATIC ${LIB_OBJS})
target_link_libraries(${XENGINE_STATIC_LIB}
  ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

if(WIN32)
  add_library(${XENGINE_IMPORT_LIB} SHARED ${LIB_OBJS})
  target_link_libraries(${XENGINE_IMPORT_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${XENGINE_IMPORT_LIB} PROPERTIES
    COMPILE_DEFINITIONS "ROCKSDB_DLL;ROCKSDB_LIBRARY_EXPORTS")
  if(MSVC)
    set_target_properties(${XENGINE_STATIC_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${XENGINE_STATIC_LIB}.pdb")
    set_target_properties(${XENGINE_IMPORT_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${XENGINE_IMPORT_LIB}.pdb")
  endif()
endif()

option(WITH_JNI "build with JNI" OFF)
if(WITH_JNI OR JNI)
  message(STATUS "JNI library is enabled")
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/java)
else()
  message(STATUS "JNI library is disabled")
endif()

set(TESTS
        memtable/art_test.cc
        cache/cache_test.cc
        cache/row_cache_test.cc
        #cache/lru_cache_test.cc
        #db/column_family_test.cc
        #db/compact_files_test.cc
        #db/compaction_iterator_test.cc
        #db/comparator_db_test.cc
        #db/corruption_test.cc
        #db/cuckoo_table_db_test.cc
        #db/db_basic_test.cc
        db/bugfix_39705890_allocate_extent_failed_test.cc
        db/bugfix_25168148_test.cc
        db/db_block_cache_test.cc
        #db/db_bloom_filter_test.cc
        #db/db_compaction_filter_test.cc
        #        db/db_compaction_test.cc
        #db/db_dynamic_level_test.cc
        #db/db_flush_test.cc
        db/db_inplace_update_test.cc
        db/db_recovery_failed_test.cc
        db/db_transaction_pipline_test.cc
        #db/db_io_failure_test.cc
        db/db_iter_test.cc
        #db/db_iterator_test.cc
        #db/db_log_iter_test.cc
        db/db_memtable_test.cc
        #db/db_merge_operator_test.cc
        #db/db_options_test.cc
        #db/db_properties_test.cc
        #db/db_range_del_test.cc
        #db/db_sst_test.cc
        #db/db_table_properties_test.cc
        #db/db_tailing_iter_test.cc
        #db/db_test.cc
        #db/db_test2.cc
        #db/db_wal_test.cc
        db/dbformat_test.cc
        #db/delete_extents_test.cc
        #db/deletefile_test.cc
        #db/external_sst_file_basic_test.cc
        #db/external_sst_file_test.cc
        #db/fault_injection_test.cc
        db/file_indexer_test.cc
        db/filename_test.cc
        #db/flush_job_test.cc
        #db/listener_test.cc
        db/log_test.cc
        #db/manual_compaction_test.cc
        #db/memtable_list_test.cc
        db/merge_helper_test.cc
        #db/merge_test.cc
        db/options_file_test.cc
        db/perf_context_test.cc
        #db/plain_table_db_test.cc
        #db/prefix_test.cc
        #db/repair_test.cc
        db/recover_no_space_test.cc
        #db/table_properties_collector_test.cc
        #db/version_edit_test.cc
        db/shrink_job_test.cc
        db/version_set_test.cc
        db/wal_manager_test.cc
        db/write_batch_test.cc
        #db/write_callback_test.cc
        db/write_controller_test.cc
        #db/end_key_pushdown_test.cc
        db/parallel_recovery_test.cc
        db/2pc_parallel_recovery_test.cc
        db/replay_thread_pool_test.cc
        env/env_basic_test.cc
        #env/env_test.cc
        env/mock_env_test.cc
        memory/alloc_mgr_test.cc
        memory/chunk_allocator_test.cc
        memtable/inlineskiplist_test.cc
        memtable/skiplist_test.cc
        monitoring/histogram_test.cc
        monitoring/iostats_context_test.cc
        #monitoring/statistics_test.cc
        #options/options_settable_test.cc
        options/options_test.cc
        storage/data_file_test.cc
        storage/extent_space_test.cc
        #storage/extent_space_manager_test.cc
        #storage/io_extent_test.cc
        #storage/level0_meta_test.cc
        #storage/shrink_extent_spaces_test.cc
        storage/storage_common_test.cc
        #storage/storage_manager_test.cc
        storage/storage_logger_test.cc
        storage/table_space_test.cc
        table/block_based_filter_block_test.cc
        #table/block_test.cc
        table/full_filter_block_test.cc
        table/merger_test.cc
        #table/extent_table_test.cc
        table/in_mem_extent_table_test.cc
        table/sstable_scan_iterator_test.cc
        tools/ldb_cmd_test.cc
        #tools/reduce_levels_test.cc
        #tools/sst_dump_test.cc
        #tools/backup_check_test.cc
        util/arena_test.cc
        #util/alloc_mgr_test.cc
        #util/auto_roll_logger_test.cc
        util/autovector_test.cc
        util/bloom_test.cc
        util/aio_test.cc
        util/coding_test.cc
        util/crc32c_test.cc
        util/delete_scheduler_test.cc
        util/dynamic_bloom_test.cc
        util/increment_number_allocator_test.cc
        #util/event_logger_test.cc
        util/file_reader_writer_test.cc
        util/filelock_test.cc
        util/heap_test.cc
        util/rate_limiter_test.cc
        util/ring_buffer_test.cc
        util/thread_list_test.cc
        util/thread_local_test.cc
        util/concurrent_direct_file_writer_test.cc
        util/serialization_test.cc
        util/fast_latch_test.cc
        util/time_interval_test.cc
        util/concurrent_hash_map_test.cc
        #util/page_arena_test.cc
        #compact/reuse_block_merge_iterator_test.cc
        compact/compaction_test.cc
        #compact/minor_compaction_test.cc
        #compact/minor_compaction_util_test.cc
        #utilities/backupable/backupable_db_test.cc
        #utilities/blob_db/blob_db_test.cc
        #utilities/checkpoint/checkpoint_test.cc
        utilities/checkpoint/hotbackup_test.cc
        #utilities/column_aware_encoding_test.cc
        #utilities/date_tiered/date_tiered_test.cc
        #utilities/document/document_db_test.cc
        #utilities/document/json_document_test.cc
        #utilities/geodb/geodb_test.cc
        #utilities/lua/rocks_lua_test.cc
        #utilities/memory/memory_test.cc
        #utilities/merge_operators/string_append/stringappend_test.cc
        #utilities/object_registry_test.cc
        #utilities/option_change_migration/option_change_migration_test.cc
        #utilities/options/options_util_test.cc
        #utilities/persistent_cache/hash_table_test.cc
        #utilities/persistent_cache/persistent_cache_test.cc
        #utilities/redis/redis_lists_test.cc
        #utilities/spatialdb/spatial_db_test.cc
        #utilities/table_properties_collectors/compact_on_deletion_collector_test.cc
        #utilities/transactions/optimistic_transaction_test.cc
        #utilities/transactions/transaction_test.cc
        #utilities/ttl/ttl_test.cc
        #utilities/write_batch_with_index/write_batch_with_index_test.cc
        utilities/parallel_read/parallel_read_test.cc
)
if(WITH_LIBRADOS)
  list(APPEND TESTS utilities/env_librados_test.cc)
endif()

set(BENCHMARKS
  cache/cache_bench.cc
  memtable/memtablerep_bench.cc
  tools/db_bench.cc
  tools/perf_memtable_search.cc
  table/table_reader_bench.cc
  utilities/column_aware_encoding_exp.cc
  utilities/persistent_cache/hash_table_bench.cc)
add_library(testharness OBJECT util/testharness.cc)
foreach(sourcefile ${BENCHMARKS})
  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile}
    $<TARGET_OBJECTS:testharness>)
  target_link_libraries(${exename}${ARTIFACT_SUFFIX} gmock ${LIBS} gflags)
endforeach(sourcefile ${BENCHMARKS})

# For test util library that is build only in DEBUG mode
# and linked to tests. Add test only code that is not #ifdefed for Release here.
set(TESTUTIL_SOURCE
    db/db_test_util.cc
    env/mock_env.cc
    monitoring/thread_status_updater_debug.cc
    table/mock_table.cc
    util/fault_injection_test_env.cc
)
# test utilities are only build in debug
enable_testing()
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
set(TESTUTILLIB testutillib${ARTIFACT_SUFFIX})
add_library(${TESTUTILLIB} STATIC ${TESTUTIL_SOURCE})
if(MSVC)
  set_target_properties(${TESTUTILLIB} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/testutillib${ARTIFACT_SUFFIX}.pdb")
endif()
set_target_properties(${TESTUTILLIB}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )

# Tests are excluded from Release builds
set(TEST_EXES ${TESTS})

foreach(sourcefile ${TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile}
      $<TARGET_OBJECTS:testharness>)
    set_target_properties(${exename}${ARTIFACT_SUFFIX}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )
    target_link_libraries(${exename}${ARTIFACT_SUFFIX}
      testutillib${ARTIFACT_SUFFIX} gmock ${LIBS} gflags)
    if(NOT "${exename}" MATCHES "db_sanity_test")
      add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
      add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
    endif()
endforeach(sourcefile ${TEST_EXES})

# C executables must link to a shared object
#set(C_TESTS db/c_test.c)
#set(C_TEST_EXES ${C_TESTS})
#
#foreach(sourcefile ${C_TEST_EXES})
#    string(REPLACE ".c" "" exename ${sourcefile})
#    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
#    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
#    set_target_properties(${exename}${ARTIFACT_SUFFIX}
#      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
#      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
#      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
#      )
#    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${XENGINE_IMPORT_LIB} testutillib${ARTIFACT_SUFFIX} gflags)
#    add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
#    add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
#endforeach(sourcefile ${C_TEST_EXES})
endif()
add_subdirectory(tools)

# Installation and packaging for Linux
if(NOT WIN32)
install(TARGETS ${XENGINE_STATIC_LIB} COMPONENT devel ARCHIVE DESTINATION lib64)
install(TARGETS ${XENGINE_SHARED_LIB} COMPONENT runtime DESTINATION lib64)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/xengine/"
        COMPONENT devel
        DESTINATION include/xengine)
#set(CMAKE_INSTALL_PREFIX /usr)
endif()
