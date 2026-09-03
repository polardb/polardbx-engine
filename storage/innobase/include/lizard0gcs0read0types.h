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

/** @file include/lizard0gcs0read0types.h
  Lizard Vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#ifndef lizard0gcs0read0types_h
#define lizard0gcs0read0types_h

#include "univ.i"

#include "db0err.h"
#include "ut0lst.h"
#include "ut0mutex.h"

#include "lizard0cmmt0types.h"
#include "lizard0txn0rec0types.h"

#include "dict0mem.h"
#include "trx0types.h"

#include "lizard0read0xa.h"

struct row_prebuilt_t;

namespace lizard {

/** SCN print format */
#define TRX_SCN_FMT IB_ID_FMT

/** Vision list uninfinite size */
constexpr ulint VISION_LIST_IDX_NULL = std::numeric_limits<ulint>::max();

/** Vision list mutex type */
typedef ib_mutex_t VisionListMutex;

class Vision {
 public:
  Vision();
  Vision(komodo::Snapshot_vision *);
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

  void set_snapshot_scn(scn_t value) { m_snapshot_scn = value; }

  trx_id_t up_limit_tid() const { return m_up_limit_id; }

  void set_up_limit_tid(trx_id_t value) { m_up_limit_id = value; }

  /**
    Write the limits to the file.

    @param     file		file to write to */
  void print_limits(FILE *file) const {
    fprintf(file,
            "Trx vision will not see trx with"
            " scn >= " TRX_SCN_FMT "\n",
            m_snapshot_scn);
  }

  bool modifications_visible_mvcc(txn_rec_t *txn_rec, const table_name_t &name,
                                  bool check_consistent) const;

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
  bool sees(trx_id_t id) const;

  /**
    Set the view creator transaction id. This should be set only
    for views created by RW transactions. */
  void set_vision_creator_trx_id(trx_id_t id) {
    ut_ad(id > 0);
    ut_ad(m_creator_trx_id == 0);
    m_creator_trx_id = id;
  }

  trx_id_t creator_trx_id() const { return m_creator_trx_id; }

  /** What kind of commit number that was used to check visible . */
  ccr_t visible_by() const {
    if (m_snapshot_vision) return m_snapshot_vision->visible_by();

    return CCR_SCN;
  }

  bool valid_txn_rec_check(txn_rec_t *txn_rec) const {
    if (txn_rec->is_committed()) {
      switch (visible_by()) {
        case CCR_SCN:
          ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
          break;
        case CCR_GCN:
          ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);
          break;
        case CCR_ALL:
          ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
          ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);
          break;
        case CCR_NONE:
        default:
          ut_ad(0);
          break;
      }
      return true;
    } else {
      return false;
    }
  }

  /**
    Return active state of the vision
    @retval   active state of the vision
  */
  bool is_active() const { return m_active; }

  /** Reset as initialzed values */
  void reset();

  void store_snapshot_vision(const komodo::Snapshot_vision *v) {
    ut_ad(v->is_vision());
    m_snapshot_vision = v;
  }

  void release_snapshot_vision() { m_snapshot_vision = nullptr; }

  const komodo::Snapshot_vision *snapshot_vision() const { return m_snapshot_vision; }

  void xa_refresh(trx_t *trx);

  bool is_asof() const { return m_snapshot_vision != nullptr; }

  bool is_asof_gcn() const { return is_asof() && m_snapshot_vision->is_gcn(); }

  bool is_asof_flashback_area() const {
    return is_asof() && m_snapshot_vision->is_flashback_area();
  }

  bool is_asof_cross_ddl() const {
    return is_asof() && m_snapshot_vision->is_cross_ddl();
  }

  /** Return the error code for a previous-version lookup result.
    @param[in]  purge_sees      purge sees the trx
    @param[in]  table_rebuilt   whether undo traversal crossed a rebuild
                                boundary
    @retval     dberr_t         error code */
  dberr_t prev_version_error(const bool purge_sees,
                             const bool table_rebuilt) const;

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

  /** Snapshot vision used for asof query. */
  const komodo::Snapshot_vision *m_snapshot_vision;

  /** Xa vision used for transaction group. */
  Xa_vision m_xa_vision;

  UT_LIST_NODE_T(Vision) list;

  friend class MVCC;
};

class Shadow_view {
 public:
  Shadow_view() { reset(); }

  bool is_active() const { return m_vision.is_active(); }

  void activate(komodo::Snapshot_automatic_gcn_vision *);

  void reset() {
    m_vision.reset();
    m_gcn = GCN_NULL;
    m_up_limit_id = 0;
  }

  const Vision &vision() const { return m_vision; }

  Vision *vision() { return &m_vision; }

  scn_t snapshot_scn() const { return m_vision.snapshot_scn(); }

  gcn_t gcn() const { return m_gcn; }

  trx_id_t up_limit_tid() const { return m_up_limit_id; }

  void set_up_limit_tid(trx_id_t value) { m_up_limit_id = value; }

  void set_gcn(gcn_t value) { m_gcn = value; }

 private:
  Vision m_vision{};
  gcn_t m_gcn{GCN_NULL};
  trx_id_t m_up_limit_id;

  friend class MVCC;
};

}  // namespace lizard

#endif
