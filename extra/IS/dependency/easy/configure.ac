/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(t_libeasy, 2.0, duolong@taobao.com)
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/io/easy_io.h])
AC_PREFIX_DEFAULT(/usr)

# Checks for programs.
AC_PROG_CXX
AC_PROG_LN_S
AC_PROG_LIBTOOL
AC_PROG_RANLIB

if test "x$prefix" = "xNONE" ; then
    prefix="/usr/local"
fi
includedir=${prefix}/include/easy
PRESET_CFLAGS="$CFLAGS"
PRESET_CPPFLAGS="$CPPFLAGS"
PRESET_LDFLAGS="$LDFLAGS"
CFLAGS="-g $PRESET_CFLAGS -Wall -Werror -fPIC -fno-strict-aliasing"
CPPFLAGS="$PRESET_CPPFLAGS -D_GNU_SOURCE -D__STDC_LIMIT_MACROS"

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([sys/inotify.h])
# Checks for typedefs, structures, and compiler characteristics.
# Checks for library functions.
AC_CHECK_LIB([pthread], [main], [], exit 1)
AC_CHECK_LIB([ssl], [SSL_CTX_new], [], exit 1)
AC_CHECK_FUNCS([accept4], [AC_DEFINE(HAVE_ACCEPT4, 1, [Define to 1 if support accept4])])
AC_CHECK_FUNCS([recvmmsg])

MOSTLYCLEANFILES=
AC_SUBST(MOSTLYCLEANFILES)
AC_ARG_WITH([gcov], AS_HELP_STRING([--with-gcov], [enable code coverage(default is NO)]), [
    if test "$withval" = "yes"; then
        CFLAGS="${PRESET_CFLAGS} -Wall -Werror -fPIC -ggdb3 -fprofile-arcs -ftest-coverage"
	LDFLAGS="-lgcov ${PRESET_LDFLAGS} -fprofile-arcs -ftest-coverage"
    fi
])
MOSTLYCLEANFILES="*.gcno *.gcda"
DEFAULT_INCLUDES="-I`pwd`/src/include/ -I`pwd`/src/util/"
DEFAULT_INCLUDES="$DEFAULT_INCLUDES -I`pwd`/src/thread/ -I`pwd`/src/io/ -I`pwd`/src/packet/ -I`pwd`/src/packet/http -I."
VERSION_MAP="`pwd`/src/libeasy.map"
AC_SUBST(DEFAULT_INCLUDES)
AC_SUBST(VERSION_MAP)
PRESET_LDADD="`pwd`/src/.libs/libeasy.a"
AC_SUBST(PRESET_LDADD)

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/include/Makefile
                 src/util/Makefile
                 src/thread/Makefile
                 src/io/Makefile
                 src/packet/Makefile
                 test/Makefile
                 test/include/Makefile
                 test/util/Makefile
                 test/thread/Makefile
                 test/io/Makefile
                 test/packet/Makefile
                 sample/Makefile
		])
AC_OUTPUT

if test -f libtool; then
  sed -i -e '/^soname_spec=/ s/major/{major}.\\${age}/g' libtool
  sed -i -e '/^library_names_spec=/ s/major/{major}.\\${age}/g' libtool
fi
