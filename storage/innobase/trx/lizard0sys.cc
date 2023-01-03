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


/** @file trx/lizard0sys.cc
 Lizard system implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#include "fsp0fsp.h"
#include "mtr0mtr.h"
#include "trx0trx.h"
#include "trx0sys.h"

#include "lizard0dict.h"
#include "lizard0fil.h"
#include "lizard0fsp.h"
#include "lizard0read0read.h"
#include "lizard0scn.h"
#include "lizard0sys.h"
#include "lizard0dbg.h"


#ifdef UNIV_PFS_MUTEX
/* min active trx_id mutex PFS key */
mysql_pfs_key_t lizard_sys_mtx_id_mutex_key;
#endif

namespace lizard {

/** The global lizard system memory structure */
lizard_sys_t *lizard_sys = nullptr;

/** Create lizard system structure. */
void lizard_sys_create() {
  ut_ad(lizard_sys == nullptr);

  lizard_sys =
      static_cast<lizard_sys_t *>(ut_zalloc_nokey(sizeof(*lizard_sys)));

  /** Placement new SCN object  */
  new (&(lizard_sys->scn)) SCN();

  lizard_sys->min_active_trx_id.store(LIZARD_SYS_MTX_ID_NULL);

  lizard_sys->mtx_inited = true;

  /** Promise here didn't have any active trx */
  ut_ad(trx_sys == nullptr || UT_LIST_GET_LEN(trx_sys->rw_trx_list) == 0);

  /** Attention: it's monitor metrics, didn't promise accuration */
  lizard_sys->txn_undo_log_free_list_len = 0;
  lizard_sys->txn_undo_log_cached = 0;

  UT_LIST_INIT(lizard_sys->serialisation_list_scn, &trx_t::scn_list);

  return;
}

/** Erase trx in serialisation_list_scn, and update min_safe_scn
@param[in]      trx      trx to be removed */
void lizard_sys_erase_lists(trx_t *trx) {
  assert_lizard_min_safe_scn_valid();

  lizard_sys_scn_mutex_enter();

  lizard_ut_ad(UT_LIST_GET_LEN(lizard_sys->serialisation_list_scn) > 0);

  lizard_ut_ad(trx != NULL && trx->state == TRX_STATE_COMMITTED_IN_MEMORY);

  auto oldest_trx = UT_LIST_GET_FIRST(lizard_sys->serialisation_list_scn);
  UT_LIST_REMOVE(lizard_sys->serialisation_list_scn, trx);

  /** Considering the case:
  serialisation_list_scn [100, 101], lizard_sys->scn = 101
  if **101** erased, and then **100** erased, serialisation_list_scn []
  we also set min_safe_scn as 100 for safety */
  if (oldest_trx == trx) {
    lizard_ut_ad(lizard_sys->min_safe_scn.load() < trx->txn_desc.cmmt.scn);
    lizard_sys->min_safe_scn.store(oldest_trx->txn_desc.cmmt.scn);
  }

  lizard_sys_scn_mutex_exit();

  assert_lizard_min_safe_scn_valid();
}

/** Close lizard system structure. */
void lizard_sys_close() {
  if (lizard_sys != nullptr) {
    lizard_ut_ad(UT_LIST_GET_LEN(lizard_sys->serialisation_list_scn) == 0);

    lizard_sys->scn.~SCN();
    lizard_sys->mtx_inited = false;

    ut_free(lizard_sys);
    lizard_sys = nullptr;
  }

  trx_vision_container_destroy();
}

/** Get the address of lizard system header */
lizard_sysf_t *lizard_sysf_get(mtr_t *mtr) {
  buf_block_t *block = nullptr;
  lizard_sysf_t *hdr;
  ut_ad(mtr);

  block = buf_page_get(
      page_id_t(dict_lizard::s_lizard_space_id, LIZARD_SYS_PAGE_NO),
      univ_page_size, RW_X_LATCH, mtr);

  hdr = LIZARD_SYS + buf_block_get_frame(block);
  return hdr;
}

/** Init the elements of lizard system */
void lizard_sys_init() {
  ut_ad(lizard_sys);
  lizard_sys->scn.init();

  /** Init vision system */
  trx_vision_container_init();
}

/** Create a new file segment special for lizard system
@param[in]      mtr */
static void lizard_create_sysf(mtr_t *mtr) {
  lizard_sysf_t *lzd_hdr;
  buf_block_t *block;
  page_t *page;
  byte *ptr;
  ut_ad(mtr);

  mtr_x_lock_space(fil_space_get_lizard_space(), mtr);

  block = fseg_create(dict_lizard::s_lizard_space_id, 0,
                      LIZARD_SYS + LIZARD_SYS_FSEG_HEADER, mtr);

  ut_a(block->page.id.page_no() == LIZARD_SYS_PAGE_NO);
  page = buf_block_get_frame(block);

  lzd_hdr = lizard_sysf_get(mtr);

  /* The scan number is counting from SCN_RESERVERD_MAX */
  mach_write_to_8(lzd_hdr + LIZARD_SYS_SCN, SCN_RESERVERD_MAX);

  /** The global commit number initial value */
  mach_write_to_8(lzd_hdr + LIZARD_SYS_GCN, GCN_INITIAL);

  /** The max GCN number whose transaction has been purged */
  mach_write_to_8(lzd_hdr + LIZARD_SYS_PURGE_GCN, GCN_INITIAL);

  ptr = lzd_hdr + LIZARD_SYS_NOT_USED;

  memset(ptr, 0, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - ptr);

  mlog_log_string(lzd_hdr, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - lzd_hdr,
                  mtr);
  lizard_info(ER_LIZARD) << "Initialize lizard system";
}

/** Create lizard system pages within system tablespace */
void lizard_create_sys_pages() {
  mtr_t mtr;

  mtr_start(&mtr);

  /* Create a new file segment special for lizard system */
  lizard_create_sysf(&mtr);

  mtr_commit(&mtr);
}

/** Get current SCN number */
scn_t lizard_sys_get_scn() {
  ut_a(lizard_sys);

  return lizard_sys->scn.get_scn();
}

/** Get max persisted GCN number */
gcn_t lizard_sys_get_gcn() {
  ut_a(lizard_sys);

  return lizard_sys->scn.get_gcn();
}

/** Get max snapthot GCN number */
gcn_t lizard_sys_get_snapshot_gcn() {
  ut_a(lizard_sys);

  return lizard_sys->scn.get_snapshot_gcn();
}

/**
  Modify the min active trx id

  @param[in]      the removed trx */
void lizard_sys_mod_min_active_trx_id(trx_t *trx) {
  trx_t *min_active_trx = NULL;

  ut_ad(trx != NULL);
  ut_ad(lizard_sys->mtx_inited);
  /* Must hold the trx sys mutex */
  ut_ad(trx_sys_mutex_own());

  /** Called after remove */
  ut_ad(!trx->in_rw_trx_list);

  /** Read only didn't put into rw list even through it allocated new trx id
     for temporary table update. */
  ut_ad((trx->read_only ||
         (trx->rsegs.m_redo.rseg == NULL && trx->rsegs.m_txn.rseg == NULL)) ||
        lizard_sys->min_active_trx_id.load() <= trx->id);

#ifdef UNIV_DEBUG
  /** Only myself modify mtx id, so delay to hold mtx mutex */
  trx_id_t old_min_active_id = lizard_sys->min_active_trx_id.load();
#endif

  min_active_trx = UT_LIST_GET_LAST(trx_sys->rw_trx_list);

  trx_id_t min_id =
      min_active_trx == nullptr ? trx_sys->max_trx_id : min_active_trx->id;

  lizard_sys->min_active_trx_id.store(min_id);

  ut_ad(old_min_active_id <= lizard_sys->min_active_trx_id.load());
}

/**
  Get the min active trx id

  @retval         the min active id in trx_sys. */
trx_id_t lizard_sys_get_min_active_trx_id() {
  trx_id_t ret;
  ret = lizard_sys->min_active_trx_id.load();
  return ret;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check if min_safe_scn is valid
@return         true if min_safe_scn is valid */
void min_safe_scn_valid() {
  scn_t sys_scn;
  trx_t *trx = nullptr;
  trx_t *prev_trx = nullptr;

  ut_ad(!lizard_sys_scn_mutex_own());

  lizard_sys_scn_mutex_enter();

  sys_scn = lizard_sys->scn.acquire_scn(true);
  if (UT_LIST_GET_LEN(lizard_sys->serialisation_list_scn) == 0) {
    ut_a(lizard_sys->min_safe_scn <= sys_scn);
  } else {
    trx = UT_LIST_GET_FIRST(lizard_sys->serialisation_list_scn);
    ut_a(trx);

    ut_a(lizard_sys->min_safe_scn < trx->txn_desc.cmmt.scn);

    ut_a(lizard_sys->min_safe_scn < sys_scn);

    /** trx in serialisation_list_scn are ordered by scn */
    prev_trx = trx;
    for (trx = UT_LIST_GET_NEXT(scn_list, trx); trx != nullptr;
         trx = UT_LIST_GET_NEXT(scn_list, trx)) {
      ut_a(prev_trx->txn_desc.cmmt.scn < trx->txn_desc.cmmt.scn);
      prev_trx = trx;
    }
  }

  lizard_sys_scn_mutex_exit();
}
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

/**
  In MySQL 8.0:
  * Hold trx_sys::mutex, generate trx->no, add trx to trx_sys->serialisation_list
  * Hold purge_sys::pq_mutex, add undo rseg to purge_queue. All undo records are ordered.
  * Erase above mutexs, commit in undo header
  * Hold trx_sys::mutex, erase serialisation_list, rw_trx_ids, rw_trx_list,
    the modifications from the committed trx can be seen.

  In Lizard:
  * Hold trx_sys::mutex, generate trx->txn_desc.scn
  * Hold SCN::mutex, add trx to trx_sys->serialisation_list_scn
  * Erase trx_sys::mutex, hold purge_sys::pq_mutex and rseg::mutex, and add
    undo rseg to purge_queue. So undo records in the same rseg are ordered, but
    undo records from different rsegs are not ordered in purge_heap.
  * Erase above mutexs, commit in undo header
  * Hold SCN::mutex, remove trx in trx_sys->serialisation_list_scn,
    and update min_safe_scn.

  To ensure purge_sys purge in order, min_safe_scn is used for purge sys.
  min_safe_scn is the current smallest scn of committing transactions (both
  prepared state and committed in memory state).

  This function might be used in the following scenarios:
  * purge sys should get a safe scn for purging
  * clone
  * PolarDB

  @retval         the min safe commited scn in current lizard sys
*/
scn_t lizard_sys_get_min_safe_scn() {
  ut_ad(!lizard_sys_scn_mutex_own());
  assert_lizard_min_safe_scn_valid();
  /* Get the oldest transaction from serialisation list. */
  return lizard_sys->min_safe_scn.load();
}

/**
  Get the max gcn between snapshot gcn and m_gcn

  @retval         the valid gcn. */
gcn_t lizard_sys_acquire_gcn() {
  ut_ad(!lizard_sys_scn_mutex_own());
  return lizard_sys->scn.acquire_gcn(false);
}

}  // namespace lizard
