# - Find Snappy
# Find the snappy compression library and includes
#
# IS_INCLUDE_DIR - where to find logger, etc.
# IS_LIBRARIES - List of libraries when using libis_all.a.
# IS_FOUND - True if libis_all.a found.

#set(IS_ROOT_DIR ../IS)

find_path(IS_INCLUDE_DIR
  NAMES logger concurrency memory sync
  HINTS ${IS_ROOT_DIR})

find_library(IS_LIBRARIES
  NAMES is_all
  HINTS ${IS_ROOT_DIR}/build ${IS_ROOT_DIR}/bu ${IS_ROOT_DIR}/lib)

find_library(TBB_LIBRARIES
  NAMES tbb
  HINTS ${IS_ROOT_DIR}/dependency/tbb/build/lib)
#SET(TBB_LIBRARIES ${IS_ROOT_DIR}/dependency/tbb/build/lib/libtbb.so ${IS_ROOT_DIR}/dependency/tbb/build/lib/libtbb.so.2)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(IS DEFAULT_MSG IS_LIBRARIES IS_INCLUDE_DIR)

mark_as_advanced(
  IS_LIBRARIES
  IS_INCLUDE_DIR)
