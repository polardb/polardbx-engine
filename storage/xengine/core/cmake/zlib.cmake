# Copyright (c) 2009, 2018, Oracle and/or its affiliates. All rights reserved.
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

MACRO (MYSQL_USE_BUNDLED_ZLIB)
  SET(BUILD_BUNDLED_ZLIB 1)
  SET(ZLIB_LIBRARY zlib CACHE INTERNAL "Bundled zlib library")
  SET(ZLIB_FOUND  TRUE)
  SET(WITH_ZLIB "bundled" CACHE STRING "Use bundled zlib")
ENDMACRO()

# MYSQL_CHECK_ZLIB_WITH_COMPRESS
# Provides the following configure options:
# WITH_ZLIB_BUNDLED
# If this is set, we use bindled zlib
# If this is not set,search for system zlib.
# if system zlib is not found, use bundled copy
# ZLIB_LIBRARIES, ZLIB_INCLUDE_DIR and ZLIB_SOURCES
# are set after this macro has run

MACRO (MYSQL_CHECK_ZLIB_WITH_COMPRESS)
  IF(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    # Use bundled zlib on some platforms by default (system one is too
    # old or not existent)
    IF (NOT WITH_ZLIB)
      SET(WITH_ZLIB "bundled"  CACHE STRING "By default use bundled zlib on this platform")
    ENDIF()
  ENDIF()

  IF(WITH_ZLIB STREQUAL "bundled")
    MYSQL_USE_BUNDLED_ZLIB()
  ELSE()
    SET(ZLIB_FIND_QUIETLY TRUE)
    INCLUDE(FindZLIB)
    IF(ZLIB_FOUND)
      INCLUDE(CheckFunctionExists)
      SET(SAVE_CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
      SET(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} z)
      CHECK_FUNCTION_EXISTS(crc32 HAVE_CRC32)
      CHECK_FUNCTION_EXISTS(compressBound HAVE_COMPRESSBOUND)
      CHECK_FUNCTION_EXISTS(deflateBound HAVE_DEFLATEBOUND)
      SET(CMAKE_REQUIRED_LIBRARIES ${SAVE_CMAKE_REQUIRED_LIBRARIES})
      IF(HAVE_CRC32 AND HAVE_COMPRESSBOUND AND HAVE_DEFLATEBOUND)
        SET(ZLIB_LIBRARY ${ZLIB_LIBRARIES} CACHE INTERNAL "System zlib library")
        SET(WITH_ZLIB "system" CACHE STRING
          "Which zlib to use (possible values are 'bundled' or 'system')")
        SET(ZLIB_SOURCES "")
      ELSE()
        SET(ZLIB_FOUND FALSE CACHE INTERNAL "Zlib found but not usable")
        MESSAGE(STATUS "system zlib found but not usable")
      ENDIF()
    ENDIF()
    IF(NOT ZLIB_FOUND)
      MYSQL_USE_BUNDLED_ZLIB()
    ENDIF()
  ENDIF()
ENDMACRO()

FUNCTION(BUILD_MYSQL_BUNDLED_ZLIB ZLIB_SRC DEST)
  #SET(CMAKE_CFLAGS "${CMAKE_CFLAGS} -fPIC")
  #SET(CMAKE_CXXFLAGS "${CMAKE_CXXFLAGS} -fPIC")
  include(CheckTypeSize)
  include(CheckFunctionExists)
  include(CheckIncludeFile)
  include(CheckCSourceCompiles)

  check_include_file(sys/types.h HAVE_SYS_TYPES_H)
  check_include_file(stdint.h    HAVE_STDINT_H)
  check_include_file(stddef.h    HAVE_STDDEF_H)

  SET(CMAKE_REQUIRED_DEFINITIONS -D_LARGEFILE64_SOURCE=1)
  IF(HAVE_SYS_TYPES_H)
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_SYS_TYPES_H)
  ENDIF()
  IF(HAVE_STDINT_H)
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_STDINT_H)
  ENDIF()
  IF(HAVE_STDDEF_H)
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_STDDEF_H)
  ENDIF()
  check_type_size(off64_t OFF64_T)
  IF(HAVE_OFF64_T)
    add_definitions(-D_LARGEFILE64_SOURCE=1)
  ENDIF()
  SET(CMAKE_REQUIRED_DEFINITIONS) # clear variable
  check_function_exists(fseeko HAVE_FSEEKO)
  IF(NOT HAVE_FSEEKO)
    add_definitions(-DNO_FSEEKO)
  ENDIF()
  check_include_file(unistd.h Z_HAVE_UNISTD_H)
  configure_file(${ZLIB_SRC}/zconf.h.cmakein ${DEST}/zconf.h @ONLY)
  include_directories(BEFORE SYSTEM ${ZLIB_SRC} ${DEST})
  SET(ZLIB_PUBLIC_HDRS ${DEST}/zconf.h ${ZLIB_SRC}/zlib.h)
  SET(ZLIB_PRIVATE_HDRS
    ${ZLIB_SRC}/crc32.h
    ${ZLIB_SRC}/deflate.h
    ${ZLIB_SRC}/gzguts.h
    ${ZLIB_SRC}/inffast.h
    ${ZLIB_SRC}/inffixed.h
    ${ZLIB_SRC}/inflate.h
    ${ZLIB_SRC}/inftrees.h
    ${ZLIB_SRC}/trees.h
    ${ZLIB_SRC}/zutil.h
  )
  SET(ZLIB_SRCS
    ${ZLIB_SRC}/adler32.c
    ${ZLIB_SRC}/compress.c
    ${ZLIB_SRC}/crc32.c
    ${ZLIB_SRC}/deflate.c
    ${ZLIB_SRC}/gzclose.c
    ${ZLIB_SRC}/gzlib.c
    ${ZLIB_SRC}/gzread.c
    ${ZLIB_SRC}/gzwrite.c
    ${ZLIB_SRC}/inflate.c
    ${ZLIB_SRC}/infback.c
    ${ZLIB_SRC}/inftrees.c
    ${ZLIB_SRC}/inffast.c
    ${ZLIB_SRC}/trees.c
    ${ZLIB_SRC}/uncompr.c
    ${ZLIB_SRC}/zutil.c
  )
  IF(NOT MINGW)
    SET(ZLIB_DLL_SRCS
        ${ZLIB_SRC}/win32/zlib1.rc # If present will override custom build rule below.
    )
  ENDIF()

  # parse the full version number from zlib.h and include in ZLIB_FULL_VERSION
  file(READ ${ZLIB_SRC}/zlib.h _zlib_h_contents)
  string(REGEX REPLACE ".*#define[ \t]+ZLIB_VERSION[ \t]+\"([-0-9A-Za-z.]+)\".*"
    "\\1" ZLIB_FULL_VERSION ${_zlib_h_contents})

  ADD_CONVENIENCE_LIBRARY(zlib
    ${ZLIB_SRCS} ${ZLIB_PUBLIC_HDRS} ${ZLIB_PRIVATE_HDRS}
    COMPILE_OPTIONS "-fPIC")

  IF(NOT CYGWIN)
    # This property causes shared libraries on Linux to have the full version
    # encoded into their final filename.  We disable this on Cygwin because
    # it causes cygz-${ZLIB_FULL_VERSION}.dll to be created when cygz.dll
    # seems to be the default.
    #
    # This has no effect with MSVC, on that platform the version info for
    # the DLL comes from the resource file win32/zlib1.rc
    set_target_properties(zlib PROPERTIES VERSION ${ZLIB_FULL_VERSION})
  ENDIF()

  IF(CMAKE_SYSTEM_NAME MATCHES "SunOS")
    # On unix-like platforms the library is almost always called libz
    SET_TARGET_PROPERTIES(zlib PROPERTIES OUTPUT_NAME z)
  ELSEIF(UNIX)
    # On unix-like platforms the library is almost always called libz
    SET_TARGET_PROPERTIES(zlib PROPERTIES OUTPUT_NAME z)
    IF(NOT APPLE)
      SET_TARGET_PROPERTIES(zlib PROPERTIES LINK_FLAGS "-Wl,--version-script,\"${ZLIB_SRC}/zlib.map\"")
    ENDIF()
  ENDIF()
ENDFUNCTION()
