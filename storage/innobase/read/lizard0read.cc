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

/** @file read/lizard0read.cc
  Lizard vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#include "clone0clone.h"
#include "row0mysql.h"
#include "trx0sys.h"
#include "univ.i"

#include "lizard0gcs.h"
#include "lizard0read0read.h"
#include "lizard0read0types.h"
#include "lizard0scn.h"
#include "lizard0undo.h"

#include "sql/lizard/lizard_snapshot.h"

#ifdef UNIV_PFS_MUTEX
/* Vision container list mutex key */
mysql_pfs_key_t lizard_vision_list_mutex_key;
#endif

namespace lizard {

/** Whether to use commit snapshot history to search
 * a suitable up_limit_id to decide sees on secondary index */

/** Only verify commit snapshot correct. */
bool srv_vision_use_commit_snapshot_debug = false;

/** Global visions */
VisionContainer *vision_container;

Vision::Vision()
    : m_snapshot_scn(SCN_NULL),
      m_list_idx(VISION_LIST_IDX_NULL),
      m_creator_trx_id(TRX_ID_MAX),
      m_up_limit_id(TRX_ID_MAX),
      m_active(false),
      m_snapshot_vision(nullptr),
      group_ids() {}

/** Reset as initialzed values */
void Vision::reset() {
  m_snapshot_scn = SCN_NULL;
  m_list_idx = VISION_LIST_IDX_NULL;
  m_creator_trx_id = TRX_ID_MAX;
  m_up_limit_id = TRX_ID_MAX;
  m_active = false;
  m_snapshot_vision = nullptr;
  group_ids.clear();
}

VisionContainer::VisionList::VisionList() {
  mutex_create(LATCH_ID_LIZARD_VISION_LIST, &m_mutex);
  UT_LIST_INIT(m_vision_list);
}

VisionContainer::VisionList::~VisionList() {
  mutex_free(&m_mutex);
  ut_ad(UT_LIST_GET_LEN(m_vision_list) == 0);
}

/**
  Add a element

  @retval		  a new empty vision obj
  @deprecated    use add_element instead
*/
Vision *VisionContainer::VisionList::new_element() {
  Vision *vision = nullptr;
  vision = ut::new_<Vision>();

  if (vision == nullptr) {
    lizard_error(ER_LIZARD) << "Failed to allocate vision";
    return nullptr;
  }

  vision->m_up_limit_id = gcs_load_min_active_trx_id();

  ut_ad(!m_mutex.is_owned());

  mutex_enter(&m_mutex);
  UT_LIST_ADD_LAST(m_vision_list, vision);
  vision->m_snapshot_scn = gcs_load_scn();
  vision->m_active = true;
  mutex_exit(&m_mutex);

  return vision;
}

/**
  Add an element

  @param[in]  the element will be added
*/
void VisionContainer::VisionList::add_element(Vision *vision) {
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
void VisionContainer::VisionList::remove_element(Vision *vision) {
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
scn_t VisionContainer::VisionList::first_element_scn() {
  Vision *element = nullptr;
  scn_t scn;

  mutex_enter(&m_mutex);
  element = UT_LIST_GET_FIRST(m_vision_list);
  scn = element ? element->m_snapshot_scn : SCN_NULL;
  mutex_exit(&m_mutex);

  return scn;
}

VisionContainer::VisionContainer(ulint _n_lists)
    : m_n_lists(_n_lists),
      m_counter(0),
      m_size(0),
      m_lists(_n_lists),
      m_inited(false) {}

/**
  Add a element.

  @param[in]		the transaction to assign vision
*/
void VisionContainer::vision_open(trx_t *trx) {
  ut_ad(trx->id != TRX_ID_MAX);

  auto vision = &trx->vision;
  vision->m_creator_trx_id = trx->id;
  vision->m_up_limit_id = gcs_load_min_active_trx_id();

  ulint idx = m_counter.fetch_add(1);
  idx %= m_n_lists;
  m_lists[idx].add_element(vision);

  m_size.fetch_add(1);

  vision->m_list_idx = idx;

  /** Verify commit snapshot module correctness. */
  if (srv_vision_use_commit_snapshot_debug) {
    Snapshot_scn_vision v(vision->snapshot_scn(), 0);
    vision->m_up_limit_id = gcs_search_up_limit_tid<Snapshot_scn_vision>(v);
  }
}

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
void VisionContainer::vision_release(Vision *vision) {
  ut_ad(vision != nullptr);
  ut_ad(vision->m_list_idx < m_n_lists);

  m_lists[vision->m_list_idx].remove_element(vision);

  m_size.fetch_sub(1);

  /** vision will be cached */
  vision->reset();
}

/**
  Get the earliest undestructed element. A exclusive lock will be holded.

  @param[out]		the earliest entry undestructed element
*/
void VisionContainer::clone_oldest_vision(Vision *vision) {
  ut_ad(vision && vision->m_list_idx == VISION_LIST_IDX_NULL);
  ut_ad(vision->m_active == false);

  scn_t oldest_scn = gcs_load_min_safe_scn();

  for (ulint i = 0; i < m_n_lists; i++) {
    scn_t first_scn = m_lists[i].first_element_scn();
    if (first_scn != SCN_NULL) {
      oldest_scn = std::min(oldest_scn, first_scn);
    }
  }

  /* Update to block purging transaction till GTID is persisted. */
  auto &gtid_persistor = clone_sys->get_gtid_persistor();
  auto gtid_oldest_trxscn = gtid_persistor.get_oldest_trx_scn();
  oldest_scn = std::min(oldest_scn, gtid_oldest_trxscn);

  vision->m_snapshot_scn = oldest_scn;
  ut_ad(vision->m_snapshot_scn <= gcs_load_scn());

  /** This vision didn't put into list */
  vision->m_list_idx = VISION_LIST_IDX_NULL;
  vision->m_active = false;
}

/**
  Get total size of all active vision

  @retval       total size of all active vision
*/
ulint VisionContainer::size() const {
  /**
    We don't want to hold all mutexs to acquire the accurate values,
    becase there all three locathions calling the function:
    1. perf_agent
    2. innodb monitor
    3. trx_sys_close
  */
  return m_size;
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
bool Snapshot_scn_vision::modification_visible(void *txn_rec) const {
  txn_rec_t *rec = static_cast<txn_rec_t *>(txn_rec);
  /** Promise committed trx and not myself. */
  ut_ad(rec->scn != SCN_NULL && rec->gcn != GCN_NULL);
  ut_ad(!undo_ptr_is_active(rec->undo_ptr));
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
bool Snapshot_gcn_vision::modification_visible(void *txn_rec) const {
  csr_t rec_csr, vision_csr;
  txn_rec_t *rec = static_cast<txn_rec_t *>(txn_rec);

  /** Promise committed trx and not myself. */
  ut_ad(rec->scn != SCN_NULL && rec->gcn != GCN_NULL);
  ut_ad(!undo_ptr_is_active(rec->undo_ptr));

  switch (m_csr) {
    case MYSQL_CSR_ASSIGNED:
      vision_csr = csr_t::CSR_ASSIGNED;
      break;
    case MYSQL_CSR_AUTOMATIC:
      vision_csr = csr_t::CSR_AUTOMATIC;
      break;
    default:
      ut_error;
  }

  rec_csr = undo_ptr_get_csr(rec->undo_ptr);

  if (rec->gcn == m_gcn) {
    if (vision_csr == CSR_ASSIGNED) {
      if (rec_csr == CSR_ASSIGNED) {
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
      if (rec_csr == CSR_ASSIGNED) {
        /** Case 3: If the record is generate by distributed trx, then it must
        happen before the local reading opened. */
        return true;
      } else {
        /** Case 4: If the record is generate by local trx, the the visibility
        judgment of the local read depends entirely on the local commit
        number (SCN).*/
        return rec->scn <= m_current_scn;
      }
    }
  } else {
    return rec->gcn < m_gcn;
  }
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
      lizard_ut_ad(txn_sys_t::instance()->is_temporary(txn_rec->scn,
                                                       txn_rec->undo_ptr) ||
                   undo_ptr_is_active(txn_rec->undo_ptr));
    }
    return true;
  } else if (txn_rec->scn == SCN_NULL) {
    if (group_ids.has(txn_rec->trx_id)) return true;
    /** If transaction still active,  not seen */
    ut_ad(!check_consistent || undo_ptr_is_active(txn_rec->undo_ptr));
    return false;
  } else {
    if (group_ids.has(txn_rec->trx_id)) return true;
    /**
      Modification scn is less than snapshot mean that
      the trx commit is prior the query lanuch.
    */
    ut_ad(!check_consistent || !undo_ptr_is_active(txn_rec->undo_ptr));

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

/**
  Assign vision for an active trancaction.

  @param[in]		the transaction to assign vision
*/
void trx_vision_open(trx_t *trx) {
  ut_ad(vision_container->inited());
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());
  vision_container->vision_open(trx);
}

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
void trx_vision_release(Vision *vision) {
  ut_ad(vision_container->inited());

  vision_container->vision_release(vision);
}

/**
  Get the earliest undestructed element.

  @param[out]		the earliest entry undestructed element
*/
void trx_clone_oldest_vision(Vision *vision) {
  ut_ad(vision_container->inited());
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  vision_container->clone_oldest_vision(vision);
}

/**
  Get total size of all active vision

  @retval       total size of all active vision
*/
ulint trx_vision_container_size() {
  ut_ad(vision_container->inited());
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  return vision_container->size();
}

/** New and init the vision_container */
void trx_vision_container_init() {
  ut_ad(vision_container == nullptr);
  void *ptr = (void *)(ut::zalloc(sizeof(VisionContainer)));
  vision_container = new (ptr) VisionContainer(VISION_CONTAINER_N_LISTS);
  vision_container->init();
}

/** Destroy the vision_container */
void trx_vision_container_destroy() {
  ut_ad(vision_container != nullptr);
  vision_container->~VisionContainer();
  ut::free(vision_container);
  vision_container = nullptr;
}

void AsofVisonWrapper::trx_store_snapshot_vision(row_prebuilt_t *prebuilt) {
  if (!prebuilt || !prebuilt->m_asof_query.is_asof_query()) return;

  ut_ad(prebuilt->trx);

  ut_ad(prebuilt->trx->vision.is_active());

  if (!prebuilt->trx->vision.is_active()) return;

  m_vision = &prebuilt->trx->vision;

  m_vision->store_snapshot_vision(prebuilt->m_asof_query.snapshot_vision());
}

void AsofVisonWrapper::release_snapshot_vision() {
  if (m_vision) {
    m_vision->release_snapshot_vision();
    m_vision = nullptr;
  }
}

}  // namespace lizard
