# Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

# cmake -DWITH_ZSTD=system|bundled
# bundled is the default

MACRO (FIND_SYSTEM_ZSTD)
  FIND_PATH(PATH_TO_ZSTD
    NAMES zstd.h
    PATH_SUFFIXES include)
  FIND_LIBRARY(ZSTD_SYSTEM_LIBRARY
    NAMES zstd
    PATH_SUFFIXES lib)
  IF (PATH_TO_ZSTD AND ZSTD_SYSTEM_LIBRARY)
    SET(SYSTEM_ZSTD_FOUND 1)
    SET(ZSTD_LIBRARY ${ZSTD_SYSTEM_LIBRARY})
    MESSAGE(STATUS "ZSTD_LIBRARY ${ZSTD_LIBRARY}")
  ENDIF()
ENDMACRO()

MACRO (MYSQL_USE_BUNDLED_ZSTD)
  SET(WITH_ZSTD "bundled" CACHE STRING "By default use bundled zstd library")
  SET(BUILD_BUNDLED_ZSTD 1)
  SET(ZSTD_LIBRARY zstd CACHE INTERNAL "Bundled zlib library")
  MESSAGE(STATUS "ZSTD_LIBRARY(Bundled) " ${ZSTD_LIBRARY})
ENDMACRO()

IF (NOT WITH_ZSTD)
  SET(WITH_ZSTD "bundled" CACHE STRING "By default use bundled zstd library")
ENDIF()

MACRO (MYSQL_CHECK_ZSTD)
  IF (WITH_ZSTD STREQUAL "bundled")
    MYSQL_USE_BUNDLED_ZSTD()
  ELSEIF(WITH_ZSTD STREQUAL "system")
    FIND_SYSTEM_ZSTD()
    IF (NOT SYSTEM_ZSTD_FOUND)
      MESSAGE(FATAL_ERROR "Cannot find system zstd libraries.")
    ENDIF()
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_ZSTD must be bundled or system")
  ENDIF()
ENDMACRO()

FUNCTION(BUILD_MYSQL_BUNDLED_ZSTD ZSTD_ROOT DEST)
  SET(ZSTD_LIB_DIR "${ZSTD_ROOT}/lib")
  INCLUDE_DIRECTORIES(${ZSTD_LIB_DIR}/common)

  SET(ZSTD_PUBLIC_HDRS
    ${ZSTD_LIB_DIR}/zstd.h
    ${ZSTD_LIB_DIR}/common/bitstream.h
    ${ZSTD_LIB_DIR}/common/compiler.h
    ${ZSTD_LIB_DIR}/common/error_private.h
    ${ZSTD_LIB_DIR}/common/fse.h
    ${ZSTD_LIB_DIR}/common/huf.h
    ${ZSTD_LIB_DIR}/common/mem.h
    ${ZSTD_LIB_DIR}/common/pool.h
    ${ZSTD_LIB_DIR}/common/threading.h
    ${ZSTD_LIB_DIR}/common/xxhash.h
    ${ZSTD_LIB_DIR}/common/zstd_errors.h
    ${ZSTD_LIB_DIR}/common/zstd_internal.h
    ${ZSTD_LIB_DIR}/compress/zstd_compress_internal.h
    ${ZSTD_LIB_DIR}/compress/zstd_double_fast.h
    ${ZSTD_LIB_DIR}/compress/zstd_fast.h
    ${ZSTD_LIB_DIR}/compress/zstd_lazy.h
    ${ZSTD_LIB_DIR}/compress/zstd_ldm.h
    ${ZSTD_LIB_DIR}/compress/zstdmt_compress.h
    ${ZSTD_LIB_DIR}/compress/zstd_opt.h
    ${ZSTD_LIB_DIR}/deprecated/zbuff.h
    ${ZSTD_LIB_DIR}/dictBuilder/divsufsort.h
    ${ZSTD_LIB_DIR}/dictBuilder/zdict.h
  )

  SET(ZSTD_SRCS
    ${ZSTD_LIB_DIR}/common/entropy_common.c
    ${ZSTD_LIB_DIR}/common/error_private.c
    ${ZSTD_LIB_DIR}/common/fse_decompress.c
    ${ZSTD_LIB_DIR}/common/pool.c
    ${ZSTD_LIB_DIR}/common/threading.c
    ${ZSTD_LIB_DIR}/common/xxhash.c
    ${ZSTD_LIB_DIR}/common/zstd_common.c
    ${ZSTD_LIB_DIR}/compress/fse_compress.c
    ${ZSTD_LIB_DIR}/compress/huf_compress.c
    ${ZSTD_LIB_DIR}/compress/zstd_compress.c
    ${ZSTD_LIB_DIR}/compress/zstd_double_fast.c
    ${ZSTD_LIB_DIR}/compress/zstd_fast.c
    ${ZSTD_LIB_DIR}/compress/zstd_lazy.c
    ${ZSTD_LIB_DIR}/compress/zstd_ldm.c
    ${ZSTD_LIB_DIR}/compress/zstdmt_compress.c
    ${ZSTD_LIB_DIR}/compress/zstd_opt.c
    ${ZSTD_LIB_DIR}/decompress/huf_decompress.c
    ${ZSTD_LIB_DIR}/decompress/zstd_decompress.c
    ${ZSTD_LIB_DIR}/deprecated/zbuff_common.c
    ${ZSTD_LIB_DIR}/deprecated/zbuff_compress.c
    ${ZSTD_LIB_DIR}/deprecated/zbuff_decompress.c
    ${ZSTD_LIB_DIR}/dictBuilder/cover.c
    ${ZSTD_LIB_DIR}/dictBuilder/divsufsort.c
    ${ZSTD_LIB_DIR}/dictBuilder/zdict.c
    ${ZSTD_LIB_DIR}/zstd.h
    ${ZSTD_LIB_DIR}/common/bitstream.h
    ${ZSTD_LIB_DIR}/common/compiler.h
    ${ZSTD_LIB_DIR}/common/error_private.h
    ${ZSTD_LIB_DIR}/common/fse.h
    ${ZSTD_LIB_DIR}/common/huf.h
    ${ZSTD_LIB_DIR}/common/mem.h
    ${ZSTD_LIB_DIR}/common/pool.h
    ${ZSTD_LIB_DIR}/common/threading.h
    ${ZSTD_LIB_DIR}/common/xxhash.h
    ${ZSTD_LIB_DIR}/common/zstd_errors.h
    ${ZSTD_LIB_DIR}/common/zstd_internal.h
    ${ZSTD_LIB_DIR}/compress/zstd_compress_internal.h
    ${ZSTD_LIB_DIR}/compress/zstd_double_fast.h
    ${ZSTD_LIB_DIR}/compress/zstd_fast.h
    ${ZSTD_LIB_DIR}/compress/zstd_lazy.h
    ${ZSTD_LIB_DIR}/compress/zstd_ldm.h
    ${ZSTD_LIB_DIR}/compress/zstdmt_compress.h
    ${ZSTD_LIB_DIR}/compress/zstd_opt.h
    ${ZSTD_LIB_DIR}/deprecated/zbuff.h
    ${ZSTD_LIB_DIR}/dictBuilder/divsufsort.h
    ${ZSTD_LIB_DIR}/dictBuilder/zdict.h
  )

  ADD_CONVENIENCE_LIBRARY(zstd
    ${ZSTD_SRCS} ${ZSTD_PUBLIC_HDRS} COMPILE_OPTIONS "-fPIC")

  # Always hide XXHash symbols
  ADD_DEFINITIONS(-DXXH_NAMESPACE=ZSTD_)

  OPTION(ZSTD_LEGACY_SUPPORT "LEGACY SUPPORT" OFF)
  OPTION(ZSTD_MULTITHREAD_SUPPORT "MULTITHREADING SUPPORT" OFF)
  OPTION(ZSTD_BUILD_PROGRAMS "BUILD PROGRAMS" ON)
  OPTION(ZSTD_BUILD_CONTRIB "BUILD CONTRIB" OFF)
  OPTION(ZSTD_BUILD_TESTS "BUILD TESTS" OFF)
  if (MSVC)
      OPTION(ZSTD_USE_STATIC_RUNTIME "LINK TO STATIC RUN-TIME LIBRARIES" OFF)
  endif ()

  IF (ZSTD_LEGACY_SUPPORT)
      MESSAGE(STATUS "ZSTD_LEGACY_SUPPORT defined!")
      ADD_DEFINITIONS(-DZSTD_LEGACY_SUPPORT=4)
  ELSE (ZSTD_LEGACY_SUPPORT)
      MESSAGE(STATUS "ZSTD_LEGACY_SUPPORT not defined!")
      ADD_DEFINITIONS(-DZSTD_LEGACY_SUPPORT=0)
  ENDIF (ZSTD_LEGACY_SUPPORT)
ENDFUNCTION()
