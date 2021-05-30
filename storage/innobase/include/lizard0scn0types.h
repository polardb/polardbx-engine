/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0scn0types.h
 Lizard scn number type declaration.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0scn0types_h
#define lizard0scn0types_h

#include "univ.i"

/** Scn number type was defined unsigned long long */
typedef uint64_t scn_t;

/** Scn time was defined 64 bits size (microsecond) */
typedef uint64_t utc_t;

/** Commit scn structure {SCN, UTC} */
typedef std::pair<scn_t, utc_t> commit_scn_t;

/** Commit scn state */
enum scn_state_t {
  SCN_STATE_INITIAL,   /** {SCN_NULL, UTC_NULL}*/
  SCN_STATE_ALLOCATED, /** 0 < scn < SCN_MAX, 0 < utc < UTC_MAX */
  SCN_STATE_INVALID    /** NONE of initial or allocated */
};

#endif
