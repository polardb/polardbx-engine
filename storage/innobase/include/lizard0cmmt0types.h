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

/** @file include/lizard0cmmt0types.h
 Lizard commit number type declaration.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0cmmt0types_h
#define lizard0cmmt0types_h

#include <utility>

#include "univ.i"

#include "lizard0gcs0service.h"
#include "lizard0ut.h"

/**------------------------------------------------------------------------*/
/** Predefined SCN */
/**------------------------------------------------------------------------*/

/**------------------------------------------------------------------------*/
/** For troubleshooting and readability, we use mutiple SCN FAKE in different
scenarios */
/**------------------------------------------------------------------------*/

/** Initialized prev scn number in txn header. See the case:
1. If txn undos are unexpectedly removed
2. the mysql run with cleanout_safe_mode again
some prev UBAs might point at such a txn header: in uncommitted status
but if not really the prev UBAs try to find. And lookup by these UBAs
might get a initialized prev scn/utc. We should set them small enough for
visibility. */

/** The max of scn number, crash direct if more than SCN_MAX */
constexpr scn_t SCN_MAX = std::numeric_limits<scn_t>::max() - 1;

/**------------------------------------------------------------------------*/
/** Predefined UTC */
/**------------------------------------------------------------------------*/

/** The max local time is less than 2038 year */
constexpr utc_t US_MAX = std::numeric_limits<std::int32_t>::max() * 1000000ULL;

/**------------------------------------------------------------------------*/
/** Predefined GCN */
/**------------------------------------------------------------------------*/
/** The max of gcn number, crash direct if more than GCN_MAX */
constexpr gcn_t GCN_MAX = std::numeric_limits<gcn_t>::max() - 1;

/** Commit undo structure {SCN, UTC, GCN} */
struct commit_mark_t {
 private:
  /** SCN special for undo corrupted */
  static constexpr scn_t SCN_UNDO_INVALID = 1;

  /** SCN special for undo lost */
  static constexpr scn_t SCN_UNDO_LOST = 2;

  /** SCN special for temporary table record */
  static constexpr scn_t SCN_TEMP_TAB_REC = 3;

  /** SCN special for index */
  static constexpr scn_t SCN_DICT_REC = 4;

  /** SCN special for index upgraded from old version. */
  static constexpr scn_t SCN_INDEX_UPGRADE = 5;

  /** The scn number for innodb dynamic metadata */
  static constexpr scn_t SCN_DYNAMIC_METADATA = SCN_MAX;

  /** The scn number for innodb log ddl */
  static constexpr scn_t SCN_LOG_DDL = SCN_MAX;

  /**------------------------------------------------------------------------*/
  /** utc for undo corrupted:  {2020/1/1 00:00:01} */
  static constexpr utc_t US_UNDO_INVALID = 1577808000 * 1000000ULL + 1;

  /** Initialized utc in txn header */
  static constexpr utc_t US_UNDO_LOST = 1577808000 * 1000000ULL + 2;

  /** Temporary table utc {2020/1/1 00:00:00} */
  static constexpr utc_t US_TEMP_TAB_REC = 1577808000 * 1000000ULL + 3;

  /** The utc for innodb dynamic metadata */
  static constexpr utc_t US_DYNAMIC_METADATA = US_MAX;

  /** The utc for innodb log ddl */
  static constexpr utc_t US_LOG_DDL = US_MAX;

  /** The utc for dd index for dd table. */
  static constexpr utc_t US_DICT_REC = US_MAX;

  /** The utc for dd index for dd table upgrade. */
  static constexpr utc_t US_INDEX_UPGRADE = US_MAX;

  /**------------------------------------------------------------------------*/
  /** Initialized prev gcn in txn header */
  static constexpr gcn_t GCN_UNDO_INVALID = 1;

  /** GCN special for undo lost */
  static constexpr gcn_t GCN_UNDO_LOST = 2;

  /** GCN special for temporary table record */
  static constexpr gcn_t GCN_TEMP_TAB_REC = 3;

  /** GCN special for index */
  static constexpr gcn_t GCN_DICT_REC = 4;

  /** GCN special for index upgraded from old version. */
  static constexpr gcn_t GCN_INDEX_UPGRADE = 5;

  /** The gcn for innodb dynamic metadata */
  static constexpr gcn_t GCN_DYNAMIC_METADATA = GCN_MAX;

  /** The gcn for innodb log ddl */
  static constexpr gcn_t GCN_LOG_DDL = GCN_MAX;

 public:
  commit_mark_t()
      : scn(SCN_NULL), us(US_NULL), gcn(GCN_NULL), csr(CSR_AUTOMATIC) {}

  commit_mark_t(scn_t scn_arg, utc_t us_arg, gcn_t gcn_arg, csr_t csr_arg)
      : scn(scn_arg), us(us_arg), gcn(gcn_arg), csr(csr_arg) {}

  bool is_null() const {
    return scn == SCN_NULL && us == US_NULL && gcn == GCN_NULL;
  }

  void reset() {
    scn = SCN_NULL;
    us = US_NULL;
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
  }

  bool is_whole_committed() const {
    return scn != SCN_NULL && us != US_NULL && gcn != GCN_NULL;
  }

  bool is_zero() const { return scn == 0 && us == 0 && gcn == 0; }

  bool is_uninitial() const { return scn == 0 && us == 0 && gcn == 0; }

  bool is_lost() const {
    return scn == SCN_UNDO_LOST && us == US_UNDO_LOST && gcn == GCN_UNDO_LOST;
  }

  bool is_alloced() const {
    if (scn > 0 && scn <= SCN_MAX && us > 0 && us <= US_MAX && gcn > 0 &&
        gcn <= GCN_MAX) {
      return true;
    }
    return false;
  }

  bool is_initial() const {
    if (scn == SCN_NULL && us == US_NULL && gcn == GCN_NULL) {
      return true;
    }
    return false;
  }

  scn_t scn;
  utc_t us;
  gcn_t gcn;
  /** Current only represent gcn source. since utc and scn only be allowed to
   * generate automatically */
  csr_t csr;

  /** Copy gcn state to gcn_tuple_t */
  void copy_to_gcn(gcn_tuple_t &tuple) const { tuple = {gcn, csr}; }

  /** Predefined */
  static const commit_mark_t temporary() {
    return {SCN_TEMP_TAB_REC, US_TEMP_TAB_REC, GCN_TEMP_TAB_REC, CSR_AUTOMATIC};
  }

  static const commit_mark_t dynamic_metadata() {
    return {SCN_DYNAMIC_METADATA, US_DYNAMIC_METADATA, GCN_DYNAMIC_METADATA,
            CSR_AUTOMATIC};
  }
  static const commit_mark_t log_ddl() {
    return {SCN_LOG_DDL, US_LOG_DDL, GCN_LOG_DDL, CSR_AUTOMATIC};
  }
  static const commit_mark_t dd_dict() {
    return {SCN_DICT_REC, US_DICT_REC, GCN_DICT_REC, CSR_AUTOMATIC};
  }
  static const commit_mark_t index_upgrade() {
    return {SCN_INDEX_UPGRADE, US_INDEX_UPGRADE, GCN_INDEX_UPGRADE,
            CSR_AUTOMATIC};
  }
  static const commit_mark_t lost() {
    return {SCN_UNDO_LOST, US_UNDO_LOST, GCN_UNDO_LOST, CSR_AUTOMATIC};
  }

  static const commit_mark_t invalid() {
    return {SCN_UNDO_INVALID, US_UNDO_INVALID, GCN_UNDO_INVALID, CSR_AUTOMATIC};
  }
  static const commit_mark_t null() {
    return {SCN_NULL, US_NULL, GCN_NULL, CSR_AUTOMATIC};
  }
};

/** Compare function */
inline bool operator==(const commit_mark_t &lhs, const commit_mark_t &rhs) {
  if (lhs.scn == rhs.scn && lhs.us == rhs.us && lhs.gcn == rhs.gcn &&
      lhs.csr == rhs.csr)
    return true;

  return false;
}

/** Commit order status of a rollback segment {SCN, UTC, GCN}, and also of
purge_sys, erase sys, free sys whose undo header iters are come from
commit_order_t of rollback segment. */
struct commit_order_t {
  scn_t scn;
  utc_t us;
  gcn_t gcn;

  commit_order_t() : scn(0), us(0), gcn(0) {}

  commit_order_t(scn_t scn_arg, utc_t us_arg, gcn_t gcn_arg)
      : scn(scn_arg), us(us_arg), gcn(gcn_arg) {}

  void set_null() {
    scn = 0;
    us = 0;
    gcn = 0;
  }

  bool is_null() const { return scn == 0 && us == 0 && gcn == 0; }

  commit_order_t &operator=(const commit_mark_t &cmmt) {
    ut_a(!cmmt.is_null());

    scn = cmmt.scn;
    us = cmmt.us;
    gcn = cmmt.gcn;

    return *this;
  }
};

/** Min commit order, return null if both null. */
static inline commit_order_t ut_min(const commit_order_t &lhs,
                                    const commit_order_t &rhs) {
  if (lhs.is_null()) return rhs;

  if (rhs.is_null()) return lhs;

  return lhs.scn < rhs.scn ? lhs : rhs;
}

/**
  Proposal gcn. It only makes sense when the ac transaction is in prepare state.
  At other times, it is in SCN_STATE_INITIAL state.
*/
struct proposal_mark_t {
 private:
  /** Initialized gcn invalid */
  static constexpr gcn_t GCN_UNDO_INVALID = 1;

 public:
  proposal_mark_t() : gcn(GCN_NULL), csr(csr_t::CSR_AUTOMATIC) {}

  proposal_mark_t(gcn_t gcn_arg, csr_t csr_arg) : gcn(gcn_arg), csr(csr_arg) {}

  proposal_mark_t(const gcn_tuple_t &tuple) : gcn(tuple.gcn), csr(tuple.csr) {}

  /** Copy gcn state to gcn_tuple_t */
  void copy_to_gcn(gcn_tuple_t &tuple) const { tuple = {gcn, csr}; }

  bool is_null() const { return gcn == GCN_NULL; }

  bool is_initial() const { return gcn == GCN_NULL; }
  bool is_alloced() const { return gcn > 0 && gcn <= GCN_MAX; }

  void reset() {
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
  }

  gcn_t gcn;
  csr_t csr;

  /** Predefined */
  static const proposal_mark_t invalid() {
    return {GCN_UNDO_INVALID, CSR_AUTOMATIC};
  }
};

inline bool operator==(const proposal_mark_t &lhs, const proposal_mark_t &rhs) {
  return lhs.gcn == rhs.gcn && lhs.csr == rhs.csr;
}

#endif
