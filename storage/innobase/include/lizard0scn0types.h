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

struct MyGCN;

/** Scn number type was defined unsigned long long */
typedef uint64_t scn_t;

/** Scn time was defined 64 bits size (microsecond) */
typedef uint64_t utc_t;

/** Global commit number */
typedef uint64_t gcn_t;

/** Commit number source */
enum csr_t {
  /** Automatic generated commit number. like scn, utc or local trx gcn */
  /** Defaultly, all commit numbers are automatical */
  CSR_AUTOMATIC = 0,

  /** Assigned commit number. like global trx gcn */
  CSR_ASSIGNED = 1,
};

/** Commit undo structure {SCN, UTC, GCN} */
struct commit_mark_t {
  scn_t scn;
  utc_t us;
  gcn_t gcn;
  /** Current only represent gcn source. since utc and scn only be allowed to
   * generate automatically */
  csr_t csr;
  /** Copy gcn state from owned_commit_gcn. */
  void copy_from_my_gcn(const MyGCN *);
  void copy_to_my_gcn(MyGCN *);
};

/** Compare function */
inline bool operator==(const commit_mark_t &lhs, const commit_mark_t &rhs) {
  if (lhs.scn == rhs.scn && lhs.us == rhs.us && lhs.gcn == rhs.gcn) return true;

  return false;
}

/** Commit scn state */
enum scn_state_t {
  SCN_STATE_INITIAL,   /** {SCN_NULL, US_NULL}*/
  SCN_STATE_ALLOCATED, /** 0 < scn < SCN_MAX, 0 < utc < US_MAX */
  SCN_STATE_INVALID    /** NONE of initial or allocated */
};

#endif
