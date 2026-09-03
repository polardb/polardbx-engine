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

/** @file read/lizard0gcs0read.cc
  Lizard vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#include "row0mysql.h"
#include "univ.i"

#include "lizard0gcs.h"
#include "lizard0gcs0read.h"
#include "lizard0gcs0read0types.h"

#include "sql/transaction/komodo0snapshot.h"
#include "lizard0txn0rec.h"

#ifdef UNIV_PFS_MUTEX
/* Vision container list mutex key */
mysql_pfs_key_t mvcc_list_mutex_key;
#endif

namespace lizard {

/** Whether to use commit snapshot history to search
 * a suitable up_limit_id to decide sees on secondary index */

/** Only verify commit snapshot correct. */
bool srv_vision_use_commit_snapshot_debug = false;

Vision::Vision()
    : m_snapshot_scn(SCN_NULL),
      m_list_idx(VISION_LIST_IDX_NULL),
      m_creator_trx_id(TRX_ID_MAX),
      m_up_limit_id(TRX_ID_MAX),
      m_active(false),
      m_snapshot_vision(nullptr),
      m_xa_vision() {}

Vision::Vision(komodo::Snapshot_vision *sv)
    : m_snapshot_scn(SCN_NULL),
      m_list_idx(VISION_LIST_IDX_NULL),
      m_creator_trx_id(TRX_ID_MAX),
      m_up_limit_id(TRX_ID_MAX),
      m_active(false),
      m_snapshot_vision(sv),
      m_xa_vision() {}

/** Reset as initialzed values */
void Vision::reset() {
  m_snapshot_scn = SCN_NULL;
  m_list_idx = VISION_LIST_IDX_NULL;
  m_creator_trx_id = TRX_ID_MAX;
  m_up_limit_id = TRX_ID_MAX;
  m_active = false;
  m_snapshot_vision = nullptr;
  m_xa_vision.reset();
}

MVCC::VisionList::VisionList() {
  mutex_create(LATCH_ID_MVCC_LIST, &m_mutex);
  UT_LIST_INIT(m_vision_list);
}

MVCC::VisionList::~VisionList() {
  mutex_free(&m_mutex);
  ut_ad(UT_LIST_GET_LEN(m_vision_list) == 0);
}

/**
  Add an element

  @param[in]  the element will be added
*/
void MVCC::VisionList::add_element(Vision *vision) {
  ut_ad(!m_mutex.is_owned());
  ut_ad(!vision->m_active);

  mutex_enter(&m_mutex);
  UT_LIST_ADD_LAST(m_vision_list, vision);
  vision->m_snapshot_scn = gcs_load_scn();
  vision->m_active = true;
  mutex_exit(&m_mutex);
}

/**
  Remove an element

  @param[in]  the element will be released
*/
void MVCC::VisionList::remove_element(Vision *vision) {
  ut_ad(!m_mutex.is_owned());
  ut_ad(vision->m_list_idx != VISION_LIST_IDX_NULL);
  ut_ad(vision->m_active);

  mutex_enter(&m_mutex);
  vision->m_active = false;
  UT_LIST_REMOVE(m_vision_list, vision);
  mutex_exit(&m_mutex);
}

/**
  Get the first element scn of the list. Must hold the mutex latch.

  @retval     the first element of the list,
              SCN_NULL if the list is empty
*/
scn_t MVCC::VisionList::first_element_scn() {
  Vision *element = nullptr;
  scn_t scn;

  mutex_enter(&m_mutex);
  element = UT_LIST_GET_FIRST(m_vision_list);
  scn = element ? element->m_snapshot_scn : SCN_NULL;
  mutex_exit(&m_mutex);

  return scn;
}

MVCC::MVCC(ulint _n_lists)
    : m_n_lists(_n_lists),
      m_counter(0),
      m_size(0),
      m_lists(_n_lists),
      m_inited(false) {
  m_inited = true;
}

/**
  Add a element.

  @param[in]		the transaction to assign vision
*/
void MVCC::vision_open(Vision *vision, trx_id_t creator_trx_id) {
  ut_ad(inited());
  ut_ad(creator_trx_id != TRX_ID_MAX);

  vision->m_creator_trx_id = creator_trx_id;
  vision->m_up_limit_id = gcs_load_min_active_tid();

  ulint idx = m_counter.fetch_add(1);
  idx %= m_n_lists;
  m_lists[idx].add_element(vision);

  m_size.fetch_add(1);

  vision->m_list_idx = idx;
}

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
void MVCC::vision_release(Vision *vision) {
  ut_ad(vision != nullptr);
  ut_ad(vision->m_list_idx < m_n_lists);

  m_lists[vision->m_list_idx].remove_element(vision);

  m_size.fetch_sub(1);

  /** vision will be cached */
  vision->reset();
}

/**
  Get the earliest vision, return newest scn if not exist.

  @retval	earliest vision snapshot scn.
*/
scn_t MVCC::clone_oldest_vision() {
  scn_t oldest_scn = gcs_load_scn();

  for (ulint i = 0; i < m_n_lists; i++) {
    scn_t first_scn = m_lists[i].first_element_scn();
    if (first_scn != SCN_NULL) {
      oldest_scn = std::min(oldest_scn, first_scn);
    }
  }
  return oldest_scn;
}

/**
  Get total size of all active vision

  @retval       total size of all active vision
*/
ulint MVCC::size() const {
  /**
    We don't want to hold all mutexs to acquire the accurate values,
    becase there all three locathions calling the function:
    1. perf_agent
    2. innodb monitor
    3. trx_sys_close
  */
  return m_size;
}

bool Vision::modifications_visible_mvcc(txn_rec_t *txn_rec,
                                        const table_name_t &name,
                                        bool check_consistent) const {
  /** purge view will use m_snapshot_scn straightway */
  ut_ad(txn_rec);
  ut_ad(txn_rec->trx_id > 0 && txn_rec->trx_id < TRX_ID_MAX);

  check_trx_id_sanity(txn_rec->trx_id, name);

  /** purge view will use m_snapshot_scn straightway */
  if (txn_rec->trx_id == m_creator_trx_id) {
    if (check_consistent) {
      /** If modification from myself, then they should be seen,
      unless it's a temp table */
      ut_ad(txn_sys_t::instance()->is_temporary(txn_rec->scn,
                                                txn_rec->undo_ptr) ||
            txn_rec->is_active());
    }
    return true;
  } else if (txn_rec->scn == SCN_NULL) {
    if (m_xa_vision.modification_visible(txn_rec->trx_id)) return true;
    /** If transaction still active,  not seen */
    ut_ad(!check_consistent || txn_rec->is_active());
    return false;
  } else {
    if (m_xa_vision.modification_visible(txn_rec->trx_id)) return true;
    /**
      Modification scn is less than snapshot mean that
      the trx commit is prior the query lanuch.
    */
    ut_ad(!check_consistent || valid_txn_rec_check(txn_rec));

    /** Use snapshot vision first when committed txn and not myself. */
    if (m_snapshot_vision) {
      return m_snapshot_vision->modification_visible(txn_rec);
    } else {
      return txn_rec->scn <= m_snapshot_scn;
    }
  }
}

/**
  Check whether the changes by id are visible.

  @param[in]  txn_rec           txn related information.
  @param[in]  name	            table name
  @param[in]  check_consistent  check the consistent between SCN and UBA

  @retval     whether the vision sees the modifications of id.
              True if visible
*/
bool Vision::modifications_visible(txn_rec_t *txn_rec, const table_name_t &name,
                                   bool check_consistent) const {
  ut_ad(txn_rec);
  ut_ad(txn_rec->trx_id > 0 && txn_rec->trx_id < TRX_ID_MAX);

  return modifications_visible_mvcc(txn_rec, name, check_consistent);
}
/**
  Whether Vision can see the target trx id,
  if the target trx id is less than the least
  active trx, then it will see.

  @param       id		transaction to check
  @retval      true  if view sees transaction id
*/
bool Vision::sees(trx_id_t id) const {
  ut_ad(id < TRX_ID_MAX && m_creator_trx_id < TRX_ID_MAX);
  ut_ad(m_list_idx != VISION_LIST_IDX_NULL);
  /** If it's a as of scn snapshot query, we always force using pk */
  /** Simliar with as of scn */

  /** Revision:  Support snapshot vision on secondary index. */
  if (m_snapshot_vision) {
    return id < m_snapshot_vision->up_limit_tid();
  }

  return id < m_up_limit_id;
}

/** Update xa related vision from xa transaction group context. */
void Vision::xa_refresh(trx_t *trx) {
  Xa_group *group = nullptr;
  ut_ad(this == &trx->vision);

  group = trx->xa_desc.group();
  if (group) {
    m_xa_vision.refresh(group);
  }
}

/** Return the error code for a previous-version lookup result.
  @param[in]  purge_sees      purge sees the trx
  @param[in]  table_rebuilt   whether undo traversal crossed a rebuild boundary
  @retval     dberr_t         error code */
dberr_t Vision::prev_version_error(const bool purge_sees,
                                   const bool table_rebuilt) const {
  if (is_asof()) {
    if (is_asof_cross_ddl() && table_rebuilt) {
      ut_ad(purge_sees);
      return DB_AS_OF_TABLE_DEF_CHANGED;
    }

    return purge_sees ? DB_SUCCESS : DB_SNAPSHOT_TOO_OLD;
  }

  return purge_sees ? DB_SUCCESS : DB_MISSING_HISTORY;
}

void Shadow_view::activate(
    komodo::Snapshot_automatic_gcn_vision *snapshot_vision) {
  snapshot_vision->m_scn = m_vision.snapshot_scn();
  snapshot_vision->m_gcn = m_gcn;
  snapshot_vision->m_up_limit_tid = m_up_limit_id;
}

}  // namespace lizard

namespace komodo {

/**
  Judge visible by txn relation info.

  Attention:
   asof
    1) Will see myself modification
    2) Not see uncommitted modification

  @retval     whether the vision sees the modifications of id.
              True if visible
*/
bool Snapshot_scn_vision::modification_visible(void *txn_rec) const {
  txn_rec_t *rec = static_cast<txn_rec_t *>(txn_rec);
  /** Promise committed trx and not myself. */
  ut_ad(rec->is_committed());

  return rec->scn <= m_scn;
}

/**
  Judge visible by txn relation info.

  Attention:
   asof
    1) Will see myself modification
    2) Not see uncommitted modification

  @retval     whether the vision sees the modifications of id.
              True if visible
*/
bool Snapshot_assigned_gcn_vision::modification_visible(void *txn_rec) const {
  txn_rec_t *rec = static_cast<txn_rec_t *>(txn_rec);
  /** Promise committed trx and not myself. */
  ut_ad(rec->is_committed());

  if (rec->gcn == m_gcn) {
    if (rec->csr() == CSR_ASSIGNED) {
      /** Case 1: Usually, it is impossible for distributed writing and
      distributed reading to have the same GCN. However, for the flashback
      query, it might happen because the AS OF GCN might be converted by
      timestamp.

      In such a case, we think is's read-after-write. */
      return true;
    } else {
      /** Case 2: If the record is generate by local trx, then it must happen
      after the distribute reading. */
      return false;
    }
  } else {
    return rec->gcn < m_gcn;
  }
}

/**
  Judge visible by txn relation info.

  Attention:
   asof
    1) Will see myself modification
    2) Not see uncommitted modification

  @retval     whether the vision sees the modifications of id.
              True if visible
*/
bool Snapshot_automatic_gcn_vision::modification_visible(void *txn_rec) const {
  txn_rec_t *rec = static_cast<txn_rec_t *>(txn_rec);
  /** Promise committed trx and not myself. */
  ut_ad(rec->is_committed());

  if (rec->gcn == m_gcn) {
    if (rec->csr() == CSR_ASSIGNED) {
      /** Case 3: If the record is generate by distributed trx, then it must
      happen before the local reading opened. */
      return true;
    } else {
      if (!rec->is_slave()) {
        /** Case 4: If the record is generate by local trx, the the visibility
        judgment of the local read depends entirely on the local commit
        number (SCN).*/
        return rec->scn <= m_scn;
      } else {
        /** Case 5: If a single-shard read query two branchs of a global
        transaction, then the two branchs shoud share a commit state. */
        return modification_visible_by_share_cn(rec);
      }
    }
  } else {
    return rec->gcn < m_gcn;
  }
}

bool Snapshot_automatic_gcn_vision::modification_visible_by_share_cn(
    void *rec) const {
  txn_rec_t *txn_rec = static_cast<txn_rec_t *>(rec);
  txn_rec_t ref_txn_rec;

  /** Must be Single Shard Transaction, and the master branch must be distribute
  transation. So never: creator_trx_id == txn_rec->trx_id. */
  bool active = lizard::txn_rec_get_master_by_lookup(txn_rec, &ref_txn_rec);

  /**
    For normal GCN based XA transaction, the external commit number is still
    not yet knonwn after XA PREPARE. So query must wait until the transaction
    committing.

    For async commt XA transaction, the external commit number of a master branch
    should be same as its slave branchs. So no need to wait if the master branch
    is still active.
  */
  if (active) {
    /** Still active. Assume that the transaction will get a COMMIT_GCN >= my
    snapshot_gcn. */
    ut_ad(ref_txn_rec.scn == SCN_NULL);
    return false;
  } else {
    /** Skip infinite recursion */
    ut_a(!ref_txn_rec.is_slave());
    ut_a(ref_txn_rec.is_committed());
    return modification_visible(&ref_txn_rec);
  }
}

void Snapshot_automatic_gcn_vision::delay_activate(trx_t *trx) {
  lizard::Shadow_view *shadow = &trx->shadow_view;

  ut_ad(!srv_read_only_mode);

  if (!shadow->is_active()) {
    trx_assign_shadow_read_view(trx);

    ut_ad(m_scn == SCN_NULL && m_gcn == GCN_NULL);
    shadow->activate(this);
  } else if (m_gcn == GCN_NULL) {
    /** inherit */
    shadow->activate(this);
  } else {
    ut_ad(m_scn == shadow->snapshot_scn());
    ut_ad(m_up_limit_tid == shadow->up_limit_tid());
    ut_ad(m_gcn == shadow->gcn());
  }

  ut_ad(m_scn != SCN_NULL && m_gcn != GCN_NULL);
}

void Snapshot_automatic_gcn_vision::inherit(lizard::Shadow_view *shadow) {
  /**
    We ran a full MTR test but still didn't find any cases where the vision
    information wasn't set correctly. However, the SQL scenario is very flexible,
    to be on the safe side, the assertions in debug mode and the assignments in
    release mode will be retained for a period of time until we are certain that
    this scenario does not exist.
  */
  ut_ad(m_gcn != GCN_NULL);
  if (m_gcn == GCN_NULL) {
    ut_ad(shadow->is_active());
    shadow->activate(this);
  }
}

}  // namespace komodo
