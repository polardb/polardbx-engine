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


/** @file read/lizard0read.cc
  Lizard vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#include "univ.i"
#include "trx0sys.h"
#include "clone0clone.h"
#include "row0mysql.h"

#include "lizard0read0read.h"
#include "lizard0read0types.h"
#include "lizard0scn.h"
#include "lizard0sys.h"
#include "lizard0undo.h"

#ifdef UNIV_PFS_MUTEX
/* Vision container list mutex key */
mysql_pfs_key_t lizard_vision_list_mutex_key;
#endif

namespace lizard {

bool srv_equal_gcn_visible = false;

/** Global visions */
VisionContainer *vision_container;

Vision::Vision()
    : m_snapshot_scn(SCN_NULL),
      m_list_idx(VISION_LIST_IDX_NULL),
      m_creator_trx_id(TRX_ID_MAX),
      m_up_limit_id(TRX_ID_MAX),
      m_active(false),
      m_is_asof_scn(false),
      m_asof_scn(SCN_NULL),
      m_is_asof_gcn(false),
      m_asof_gcn(GCN_NULL),
      group_ids() {}

/** Reset as initialzed values */
void Vision::reset() {
  m_snapshot_scn = SCN_NULL;
  m_list_idx = VISION_LIST_IDX_NULL;
  m_creator_trx_id = TRX_ID_MAX;
  m_up_limit_id = TRX_ID_MAX;
  m_active = false;
  m_is_asof_scn = false;
  m_asof_scn = SCN_NULL;
  m_is_asof_gcn = false;
  m_asof_gcn = GCN_NULL;
  group_ids.clear();
}

VisionContainer::VisionList::VisionList() {
  mutex_create(LATCH_ID_LIZARD_VISION_LIST, &m_mutex);
  UT_LIST_INIT(m_vision_list, &Vision::m_vision_list);
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
  vision = UT_NEW_NOKEY(Vision());

  if (vision == nullptr) {
    lizard_error(ER_LIZARD) << "Failed to allocate vision";
    return nullptr;
  }

  vision->m_up_limit_id = lizard_sys_get_min_active_trx_id();

  ut_ad(!m_mutex.is_owned());

  mutex_enter(&m_mutex);
  UT_LIST_ADD_LAST(m_vision_list, vision);
  vision->m_snapshot_scn = lizard_sys_get_scn();
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
  vision->m_snapshot_scn = lizard_sys_get_scn();
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
  vision->m_up_limit_id = lizard_sys_get_min_active_trx_id();

  ulint idx = os_atomic_increment_ulint(&m_counter, 1);
  idx %= m_n_lists;
  m_lists[idx].add_element(vision);

  os_atomic_increment_ulint(&m_size, 1);

  vision->m_list_idx = idx;

  vision_collect_trx_group_ids(trx, vision);
}

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
void VisionContainer::vision_release(Vision *vision) {
  ut_ad(vision != nullptr);
  ut_ad(vision->m_list_idx < m_n_lists);

  m_lists[vision->m_list_idx].remove_element(vision);

  os_atomic_decrement_ulint(&m_size, 1);

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

  scn_t oldest_scn = lizard_sys_get_min_safe_scn();

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
  ut_ad(vision->m_snapshot_scn <= lizard_sys_get_scn());

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
      lizard_ut_ad((txn_rec->scn == SCN_TEMP_TAB_REC &&
                    txn_rec->undo_ptr == UNDO_PTR_TEMP_TAB_REC) ||
                   lizard_undo_ptr_is_active(txn_rec->undo_ptr));
    }
    return true;
  } else if (txn_rec->scn == SCN_NULL) {
    if (group_ids.has(txn_rec->trx_id)) return true;
    /** If transaction still active,  not seen */
    ut_ad(!check_consistent || lizard_undo_ptr_is_active(txn_rec->undo_ptr));
    return false;
  } else {
    if (group_ids.has(txn_rec->trx_id)) return true;
    /**
      Modification scn is less than snapshot mean that
      the trx commit is prior the query lanuch.
    */
    ut_ad(!check_consistent || !lizard_undo_ptr_is_active(txn_rec->undo_ptr));
    return txn_rec->scn <= m_snapshot_scn;
  }
}

/**
  Check whether the changes by id are visible. Only used in as-of query.

  @param[in]  txn_rec           txn related information of record.

  @retval     whether the vision sees the modifications of id
              True if visible.
*/
bool Vision::modifications_visible_asof_scn(txn_rec_t *txn_rec) const {
  ut_ad(m_is_asof_scn);

#if defined UNIV_DEBUG && defined TURN_MVCC_SEARCH_TO_AS_OF
  if (txn_rec->trx_id == m_creator_trx_id) {
    return true;
  }
#endif

  if (group_ids.has(txn_rec->trx_id)) return true;

  if (txn_rec->scn == SCN_NULL) {
    /* flash back query can never see the un-committed modifications */
    return false;
  }

  return txn_rec->scn <= m_asof_scn;
}

/**
  Check whether the changes by id are visible. Only used in global query.

  @param[in]  txn_rec           txn related information of record.

  @retval     whether the vision sees the modifications of id
              True if visible.
*/
bool Vision::modifications_visible_asof_gcn(txn_rec_t *txn_rec) const {
  ut_ad(!m_is_asof_scn);
  ut_ad(m_is_asof_gcn);

  /** Promise that caller has used txn_undo_hdr_lookup() correctly.
      It means that gcn number is not null if the trx has committed.
  */
#ifndef DBUG_OFF
  if (txn_rec->scn != SCN_NULL) {
    ut_a(txn_rec->gcn != GCN_NULL);
  }
#endif

  /** Global query should see myself */
  if (txn_rec->trx_id == m_creator_trx_id) {
    return true;
  }

  if (group_ids.has(txn_rec->trx_id)) return true;

  if (txn_rec->gcn == GCN_NULL) {
    ut_ad(txn_rec->scn == SCN_NULL);
    /* global query can never see the un-committed modifications */
    return false;
  }

  ut_ad(txn_rec->gcn != GCN_NULL);

  if (srv_equal_gcn_visible)
    return txn_rec->gcn <= m_asof_gcn;
  else
    return txn_rec->gcn < m_asof_gcn;
}

/**
  Check whether the changes by id are visible.

  @param[in]  txn_rec           txn related information.
  @param[in]  name	            table name
  @param[in]  check_consistent  check the consistent between SCN and UBA

  @retval     whether the vision sees the modifications of id.
              True if visible
*/
bool Vision::modifications_visible(txn_rec_t *txn_rec,
                                   const table_name_t &name,
                                   bool check_consistent) const {
  ut_ad(txn_rec);
  ut_ad(txn_rec->trx_id > 0 && txn_rec->trx_id < TRX_ID_MAX);

  if (m_is_asof_scn) {
    return modifications_visible_asof_scn(txn_rec);
  } else if (m_is_asof_gcn) {
    return modifications_visible_asof_gcn(txn_rec);
  } else {
    return modifications_visible_mvcc(txn_rec, name, check_consistent);
  }
}

/**
  Set m_snapshot_scn, m_is_as_of

  @param[in]    scn           m_snapshot_scn
  @param[in]    is_as_of      true if it's a as-of query
*/
void Vision::set_asof_scn(scn_t scn) {
  if (scn != SCN_NULL) {
    m_asof_scn = scn;
    m_is_asof_scn = true;
  }
}

/** reset m_as_of_scn, m_is_as_of as initialized values */
void Vision::reset_asof_scn() {
  m_is_asof_scn = false;
  m_asof_scn = SCN_NULL;
}

void Vision::set_asof_gcn(gcn_t gcn) {
  if (gcn != GCN_NULL) {
    m_asof_gcn = gcn;
    m_is_asof_gcn = true;
  }
}

void Vision::reset_asof_gcn() {
  m_is_asof_gcn = false;
  m_asof_gcn = GCN_NULL;
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
  void *ptr = (void *)(ut_zalloc_nokey(sizeof(VisionContainer)));
  vision_container = new (ptr) VisionContainer(VISION_CONTAINER_N_LISTS);
  vision_container->init();
}

/** Destroy the vision_container */
void trx_vision_container_destroy() {
  ut_ad(vision_container != nullptr);
  vision_container->~VisionContainer();
  ut_free(vision_container);
  vision_container = nullptr;
}

void AsofVisonWrapper::set_as_of_vision(row_prebuilt_t *prebuilt) {
  if (!prebuilt || !prebuilt->m_asof_query.is_asof_query()) return;

  ut_ad(prebuilt->trx);

  ut_ad(prebuilt->trx->vision.is_active());

  if (!prebuilt->trx->vision.is_active()) return;

  m_vision = &prebuilt->trx->vision;

  if (prebuilt->m_asof_query.is_asof_scn()) {
    m_vision->set_asof_scn(prebuilt->m_asof_query.m_scn);
  } else if (prebuilt->m_asof_query.is_asof_gcn()) {
    m_vision->set_asof_gcn(prebuilt->m_asof_query.m_gcn);
  }
}

void AsofVisonWrapper::reset() {
  if (m_vision) {
    m_vision->reset_asof_scn();
    m_vision->reset_asof_gcn();
    m_vision = nullptr;
  }
}

}  // namespace lizard
