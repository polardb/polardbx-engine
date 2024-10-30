/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0gcs0service.h
 Lizard scn number type declaration.

 Created 2024-06-05 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0gcs0service_h
#define lizard0gcs0service_h

#include "my_inttypes.h"
#include "assert.h"
#include <limits>
#include <tuple>

/** Scn number type was defined unsigned long long */
typedef uint64_t scn_t;

/** Scn time was defined 64 bits size (microsecond) */
typedef uint64_t utc_t;

/** Global commit number */
typedef uint64_t gcn_t;

/**------------------------------------------------------------------------*/
/** Predefined */
/**------------------------------------------------------------------------*/
/** Invalid scn number was defined as the max value of ulint */
constexpr scn_t SCN_NULL = std::numeric_limits<scn_t>::max();

/** Invalid time 1970-01-01 00:00:00 +0000 (UTC) */
constexpr utc_t US_NULL = std::numeric_limits<utc_t>::min();

/** Invalid gcn number was defined as the max value of ulint */
constexpr gcn_t GCN_NULL = std::numeric_limits<gcn_t>::max();

/** The initial global commit number value after initialize db */
constexpr gcn_t GCN_INITIAL = 1024;

/** Commit number source */
enum cn_source_t {
  /** Automatic generated commit number. like scn, utc or local trx gcn */
  /** Defaultly, all commit numbers are automatical */
  CSR_AUTOMATIC = 0,

  /** Assigned commit number. like global trx gcn */
  CSR_ASSIGNED = 1,
};
typedef enum cn_source_t csr_t;

/** Category of commit number combination. */
enum cn_category_t {
  /** Didn't have any kind of commit number */
  CCR_NONE = 0,
  /** Represent system commit number. */
  CCR_SCN,
  /** Represent global commit number. */
  CCR_GCN,
  /** Represent scn and gcn both */
  CCR_ALL,
};
typedef enum cn_category_t ccr_t;

/** GCN tuple structure, it will be used by:
 *
 *  1) Commit policy, whichi is used to decide GCN when commit.
 *
 *  2) MyXAinfo, which is used to search gcn info.
 *
 *  2) Proposal gcn, which is proposed by CN customized XA.
 *
 *  3) Commit gcn, which is submit by local or global trx.
 * */
struct gcn_tuple_t {
 public:
  gcn_tuple_t() : gcn(GCN_NULL), csr(csr_t::CSR_AUTOMATIC) {}

  /** constructor */
  gcn_tuple_t(gcn_t gcn_arg, csr_t csr_arg) : gcn(gcn_arg), csr(csr_arg) {}

  /** reset */
  void reset() {
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
  }

  /** Whether it's null gcn tuple */
  bool is_null() const { return gcn == GCN_NULL; }

  /** global commit number */
  gcn_t gcn;
  /** global commit source */
  csr_t csr;
};

const gcn_tuple_t GTUPLE_NULL(GCN_NULL, CSR_AUTOMATIC);

/** Validate gcn tuple */
inline bool gcn_tuple_validation(const gcn_tuple_t &tuple) {
  return tuple.csr == csr_t::CSR_ASSIGNED ? tuple.gcn != GCN_NULL : true;
}

/** Convert number to csr */
inline csr_t uint2csr(unsigned int csr) {
  return csr == 0 ? CSR_AUTOMATIC : CSR_ASSIGNED;
}

#endif
