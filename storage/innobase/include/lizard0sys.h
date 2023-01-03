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


/** @file include/lizard0sys.h
 Lizard system implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0sys_h
#define lizard0sys_h

#include "fsp0types.h"
#include "lizard0scn.h"
#include "trx0types.h"
#include "trx0sys.h"

struct mtr_t;

/** Lizard system file segment header */
typedef byte lizard_sys_fseg_t;

/** Lizard system header:
    it begins from LIZARD_SYS, and includes file segment header. */
typedef byte lizard_sysf_t;

/** Lizard system header */
/**-----------------------------------------------------------------------*/

/** The offset of lizard system header on the file segment page */
#define LIZARD_SYS FSEG_PAGE_DATA

/** The scn number which is stored here, it occupied 8 bytes */
#define LIZARD_SYS_SCN 0

/** The global commit number which is stored here. */
#define LIZARD_SYS_GCN (LIZARD_SYS_SCN + 8)

/** The purge scn number which is stored here, it occupied 8 bytes */
#define LIZARD_SYS_PURGE_SCN (LIZARD_SYS_GCN + 8)

/** The offset of file segment header */
#define LIZARD_SYS_FSEG_HEADER (LIZARD_SYS_PURGE_SCN + 8)

/**
   Revision history:
   ------------------------------------------------------
   1. Add purged_gcn
*/

#define LIZARD_SYS_PURGE_GCN (LIZARD_SYS_FSEG_HEADER + FSEG_HEADER_SIZE)

/** The start of not used */
#define LIZARD_SYS_NOT_USED (LIZARD_SYS_PURGE_GCN + 8)
/**-----------------------------------------------------------------------*/

/** The page number of lizard system header in lizard tablespace */
#define LIZARD_SYS_PAGE_NO 3

/** Initial value of min_active_trx_id */
#define LIZARD_SYS_MTX_ID_NULL 0

#ifdef UNIV_PFS_MUTEX
/* min active trx_id mutex PFS key */
extern mysql_pfs_key_t lizard_sys_mtx_id_mutex_key;
#endif

namespace lizard {

/** The memory structure of lizard system */
struct lizard_sys_t {
  /** The global scn number which is total order. */
  SCN scn;

  /** The min active trx id */
  std::atomic<trx_id_t> min_active_trx_id;

  /** min_active_trx has been inited */
  bool mtx_inited;

  /** Length of txn undo log segment free list */
  ulint txn_undo_log_free_list_len;

  /** Count of txn undo log which is cached */
  ulint txn_undo_log_cached;

  /** A min safe SCN, only used for purge */
  std::atomic<scn_t> min_safe_scn;

  /*!< Ordered on commited scn of trx_t of all the
  currenrtly active RW transactions */
  trx_ut_list_t serialisation_list_scn;
};

/** Create lizard system structure. */
void lizard_sys_create();

/** Close lizard system structure. */
void lizard_sys_close();

/** Init the elements of lizard system */
void lizard_sys_init();

/** Get the address of lizard system header */
extern lizard_sysf_t *lizard_sysf_get(mtr_t *mtr);

/** Create lizard system pages within lizard tablespace */
extern void lizard_create_sys_pages();

/** GLobal lizard system */
extern lizard_sys_t *lizard_sys;

/** Get current SCN number */
extern scn_t lizard_sys_get_scn();

/** Get current persisted GCN number */
extern gcn_t lizard_sys_get_gcn();

/** Get max snapshot GCN number */
extern gcn_t lizard_sys_get_snapshot_gcn();

/**
  Modify the min active trx id

  @param[in]      the add/removed trx */
void lizard_sys_mod_min_active_trx_id(trx_t *trx);

/**
  Get the min active trx id

  @retval         the min active id in trx_sys. */
trx_id_t lizard_sys_get_min_active_trx_id();

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
scn_t lizard_sys_get_min_safe_scn();

/** Erase trx in serialisation_list_scn, and update min_safe_scn
@param[in]      trx      trx to be removed */
void lizard_sys_erase_lists(trx_t *trx);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check if min_safe_scn is valid */
void min_safe_scn_valid();
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

/**
  Get the max gcn between snapshot gcn and m_gcn

  @retval         the valid gcn. */
gcn_t lizard_sys_acquire_gcn();

}  // namespace lizard

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
#define assert_lizard_min_safe_scn_valid() \
  do {                                     \
    lizard::min_safe_scn_valid();          \
  } while (0)

#else

#define assert_lizard_min_safe_scn_valid()

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

#endif  // lizard0sys_h define
