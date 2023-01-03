/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file include/lizard0read0types.h
  Lizard Vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#ifndef lizard0read0types_h
#define lizard0read0types_h

/* #define TURN_MVCC_SEARCH_TO_AS_OF */

#include <algorithm>

#include "ut0lst.h"
#include "ut0mutex.h"

#include "lizard0scn0types.h"
#include "lizard0undo0types.h"
#include "lizard0scn.h"

#include "trx0types.h"
#include "dict0mem.h"

#include "lizard0xa0types.h"

struct row_prebuilt_t;

namespace lizard {

/** SCN print format */
#define TRX_SCN_FMT IB_ID_FMT

/** Vision list uninfinite size */
constexpr ulint VISION_LIST_IDX_NULL = std::numeric_limits<ulint>::max();

/** Vision list mutex type */
typedef ib_mutex_t VisionListMutex;

/** Whether equal gcn is visible */
extern bool srv_equal_gcn_visible;

class Vision {
 public:
  explicit Vision();
  virtual ~Vision() {}

 private:
  /* Disable copying */
  Vision(const Vision &) = delete;
  Vision(const Vision &&) = delete;
  Vision &operator=(const Vision &) = delete;

 public:
  /**
    Return scn of the vision
    @retval   scn of the vision
  */
  scn_t snapshot_scn() const { return m_snapshot_scn; }

  /**
    Write the limits to the file.

    @param     file		file to write to */
  void print_limits(FILE *file) const {
    fprintf(file,
            "Trx vision will not see trx with"
            " scn >= " TRX_SCN_FMT "\n",
            m_snapshot_scn);
  }

  bool modifications_visible_mvcc(txn_rec_t *txn_rec,
                                  const table_name_t &name,
                                  bool check_consistent) const;

  /**
    Check whether the changes by id are visible. Only used in as-of query.

    @param[in]  txn_rec           txn related information of record.

    @retval     whether the vision sees the modifications of id
                True if visible.
  */
  bool modifications_visible_asof_scn(txn_rec_t *txn_rec) const;

  /**
    Check whether the changes by id are visible. Only used in global query.

    @param[in]  txn_rec           txn related information of record.

    @retval     whether the vision sees the modifications of id
                True if visible.
  */
  bool modifications_visible_asof_gcn(txn_rec_t *txn_rec) const;
  /**
    Check whether the changes by id are visible.

    @param[in]  txn_rec           txn related information.
    @param[in]  name	            table name
    @param[in]  check_consistent  check the consistent between SCN and UBA

    @retval       whether the vision sees the modifications of id.
                  True if visible
  */
  bool modifications_visible(txn_rec_t *txn_rec, const table_name_t &name,
                             bool check_consistent = true) const
      MY_ATTRIBUTE((warn_unused_result));

  /**
    Check whether transaction id is valid.

    @param[in]	id	transaction id to check
    @param[in]	name	table name */
  void check_trx_id_sanity(trx_id_t id, const table_name_t &name) const;

  /**
    Whether Vision can see the target trx id,
    if the target trx id is less than the least
    active trx, then it will see.

    @param       id		transaction to check
    @retval      true  if view sees transaction id
  */
  bool sees(trx_id_t id) const {
    ut_ad(id < TRX_ID_MAX && m_creator_trx_id < TRX_ID_MAX);
    ut_ad(m_list_idx != VISION_LIST_IDX_NULL);
    /** If it's a as of scn snapshot query, we always force using pk */
    if (m_is_asof_scn) return false;

    /** Simliar with as of scn */
    if (m_is_asof_gcn) return false;

    return id < m_up_limit_id;
  }

  /**
    Set the view creator transaction id. This should be set only
    for views created by RW transactions. */
  void set_vision_creator_trx_id(trx_id_t id) {
    ut_ad(id > 0);
    ut_ad(m_creator_trx_id == 0);
    m_creator_trx_id = id;
  }

  /**
    Return active state of the vision
    @retval   active state of the vision
  */
  bool is_active() const { return m_active; }

  /** Reset as initialzed values */
  void reset();

  /**
    Set m_snapshot_scn, m_is_as_of

    @param[in]    scn           m_snapshot_scn
    @param[in]    is_as_of      true if it's a as-of query
  */
  void set_asof_scn(scn_t scn);

  /** reset m_as_of_scn, m_is_as_of as initialized values */
  void reset_asof_scn();

  bool is_asof_scn() const { return m_is_asof_scn; }

  void set_asof_gcn(gcn_t gcn);

  void reset_asof_gcn();

  bool is_asof_gcn() const { return m_is_asof_gcn; }

  gcn_t get_asof_gcn() const { return m_asof_gcn; }

  bool is_asof() const { return m_is_asof_scn || m_is_asof_gcn; }

#ifdef UNIV_DEBUG
  /**
    Less or equal compare
    @param    rhs		  view to compare with
    @retval   true    if this view is less than or equal rhs
  */
  bool le(const Vision *rhs) const {
    return (m_snapshot_scn <= rhs->m_snapshot_scn);
  }
#endif /* UNIV_DEBUG */

 private:
  /** All RW transactions that SCN number is less than m_snapshot_scn
  have been commited */
  scn_t m_snapshot_scn;

  /** The index of the list that it belongs to, used to find which
  mutex should be holded. */
  ulint m_list_idx;

  /** The creator of the view, and is used to determine visibility */
  trx_id_t m_creator_trx_id;

  /** All trx whose trx_id is smaller than m_up_limit_id is seen. Only used
  by secondary index */
  trx_id_t m_up_limit_id;

  /** Whether the vision is active, the active vision must be in list. */
  bool m_active;

  /** true if it's a as-of query. An as-of query is that searches rows only
  condiering scn. The as-of query context also uses m_snapshot_scn. */
  bool m_is_asof_scn;

  /** SCN_NULL if it's not a as-of query. Only used in
  modifications_visible_as_of */
  scn_t m_asof_scn;

  /** true if it's global query */
  bool m_is_asof_gcn;
  /** Global commit number for query */
  gcn_t m_asof_gcn;

  typedef UT_LIST_NODE_T(Vision) node_t;

  node_t m_vision_list;

  friend class VisionContainer;

 public:
  /** The trx id container that belong to the same trx group */
  trx_group_ids group_ids;
};

/**
  A helper class: backup trx->vision.m_snapshot_scn and restores it.
*/
class AsofVisonWrapper {
 public:
  AsofVisonWrapper() : m_vision(nullptr) {}

  ~AsofVisonWrapper() { reset(); }

  void set_as_of_vision(row_prebuilt_t *prebuilt);

  void reset();

 private:
  Vision *m_vision;
};

}  // namespace lizard

#endif
