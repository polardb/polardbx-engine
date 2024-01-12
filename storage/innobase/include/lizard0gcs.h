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

/** @file include/lizard0gcs.h
 Global Change System implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0gcs_h
#define lizard0gcs_h

#include "fsp0types.h"
#include "lizard0gcs0hist.h"
#include "lizard0scn.h"
#include "trx0sys.h"
#include "trx0types.h"

struct mtr_t;

/** Global Change System file segment header */
typedef byte gcs_sysf_t;

/** GCS system header */
/**-----------------------------------------------------------------------*/

/** The offset of GCS system header on the file segment page */
#define GCS_DATA FSEG_PAGE_DATA

/** The scn number which is stored here, it occupied 8 bytes */
#define GCS_DATA_SCN 0

/** The global commit number which is stored here. */
#define GCS_DATA_GCN (GCS_DATA_SCN + 8)

/** The purge scn number which is stored here, it occupied 8 bytes */
#define GCS_DATA_PURGE_SCN (GCS_DATA_GCN + 8)

/** The offset of file segment header */
#define GCS_DATA_FSEG_HEADER (GCS_DATA_PURGE_SCN + 8)

/**
   Revision history:
   ------------------------------------------------------
   1. Add purged_gcn
*/

#define GCS_DATA_PURGE_GCN (GCS_DATA_FSEG_HEADER + FSEG_HEADER_SIZE)

/** The start of not used */
#define GCS_DATA_NOT_USED (GCS_DATA_PURGE_GCN + 8)
/**-----------------------------------------------------------------------*/

/** The page number of GCS system header in lizard tablespace */
#define GCS_DATA_PAGE_NO 3

/** Initial value of min_active_trx_id */
#define GCS_DATA_MTX_ID_NULL 0

#ifdef UNIV_PFS_MUTEX
/* GCS scn list mutex PFS key */
extern mysql_pfs_key_t scn_list_mutex_key;
/* GCS gcn order mutex PFS key */
extern mysql_pfs_key_t gcn_order_mutex_key;
/* GCS gcn persist mutex PFS key */
extern mysql_pfs_key_t gcn_persist_mutex_key;
#endif

namespace lizard {

class CSnapshot_mgr;

/** GCS Persister container. */
struct Persisters {
 public:
  Persisters() : m_scn_persister(), m_gcn_persister() {}

  virtual ~Persisters() {}

  Persister *scn_persister() { return &m_scn_persister; }
  Persister *gcn_persister() { return &m_gcn_persister; }

 private:
  ScnPersister m_scn_persister;
  GcnPersister m_gcn_persister;
};

class CRecover {
 public:
  CRecover() : m_need_recovery(false), m_metadata() {}
  virtual ~CRecover() {}

  /**
    Raise gcn metadata when parse gcn redo log record.

    @param[in]	gcn	Parsed gcn
  */
  void recover_gcn(const gcn_t gcn);

  /**
    Recover the max parsed gcn to current gcn.
  */
  void apply_gcn();

  void need_recovery(bool value) { m_need_recovery = value; }

  bool is_need_recovery() { return m_need_recovery; }

 private:
  bool m_need_recovery;
  PersistentGcsData m_metadata;
};

/** The memory structure of GCS system */
struct gcs_t {
  /** The global scn number which is total order. */
  SCN scn;

  /** The global gcn number which come from TSO or Inner. */
  GCN gcn;

  /** The min active trx id */
  std::atomic<trx_id_t> min_active_trx_id;

  /** min_active_trx has been inited */
  bool mtx_inited;

  /** Length of txn undo log segment free list */
  std::atomic<uint64_t> txn_undo_log_free_list_len;

  /** Count of txn undo log which is cached */
  std::atomic<uint64_t> txn_undo_log_cached;

  /** A min safe SCN, only used for purge */
  std::atomic<scn_t> min_safe_scn;

  /*!< Ordered on commited scn of trx_t of all the
  currenrtly active RW transactions */
  UT_LIST_BASE_NODE_T(trx_t, scn_list) serialisation_list_scn;
  /** Persister for gcs metadata. */
  Persisters persisters;

  /** Protect serialisation list scn */
  ib_mutex_t m_scn_list_mutex;

  /** Protect m_gcn order */
  ib_mutex_t m_gcn_order_mutex;

  /** Serialize gcn persist */
  ib_mutex_t m_gcn_persist_mutex;

  /** New trx commit. */
  commit_mark_t new_commit(trx_t *trx, mtr_t *mtr);

  /** Persisted gcn value. */
  std::atomic<gcn_t> m_persisted_gcn;

  /**
    Persist gcn if current gcn > persisted gcn.

    @retval	true	if written
   */
  bool persist_gcn();

  /** Commit number snapshot. */
  CSnapshot_mgr csnapshot_mgr;
  void new_snapshot(const commit_snap_t &snap);

  template <typename T>
  trx_id_t search_up_limit_tid(const T &lhs);
};

/** Initialize GCS system memory structure. */
void gcs_init();

/** Close GCS system structure. */
void gcs_close();

/** Boot GCS system */
void gcs_boot();

/** Get the address of GCS system header */
extern gcs_sysf_t *gcs_sysf_get(mtr_t *mtr);

/** Create GCS system pages within lizard tablespace */
extern void gcs_create_sys_pages();

/** GLobal GCS system */
extern gcs_t *gcs;

/** Get current SCN number */
extern scn_t gcs_load_scn();

/** Get current persisted GCN number */
extern gcn_t gcs_load_gcn();

extern bool gcs_persist_gcn();

/**
  Modify the min active trx id

  @param[in]      the add/removed trx */
extern void gcs_mod_min_active_trx_id(trx_t *trx);

/**
  Get the min active trx id

  @retval         the min active id in trx_sys. */
extern trx_id_t gcs_load_min_active_trx_id();

/**
  In MySQL 8.0:
  * Hold trx_sys::mutex, generate trx->no, add trx to
  trx_sys->serialisation_list
  * Hold purge_sys::pq_mutex, add undo rseg to purge_queue. All undo records are
  ordered.
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
extern scn_t gcs_load_min_safe_scn();

/** Erase trx in serialisation_list_scn, and update min_safe_scn
@param[in]      trx      trx to be removed */
void gcs_erase_lists(trx_t *trx);

template <typename T>
extern trx_id_t gcs_search_up_limit_tid(const T &lhs);

extern void gcs_set_gcn_if_bigger(gcn_t gcn);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check if min_safe_scn is valid */
void min_safe_scn_valid();
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

}  // namespace lizard

#define scn_list_mutex_enter()                   \
  do {                                           \
    ut_ad(lizard::gcs != nullptr);               \
    mutex_enter(&lizard::gcs->m_scn_list_mutex); \
  } while (0)

#define scn_list_mutex_exit()                   \
  do {                                          \
    ut_ad(lizard::gcs != nullptr);              \
    mutex_exit(&lizard::gcs->m_scn_list_mutex); \
  } while (0)

#ifdef UNIV_DEBUG
#define scn_list_mutex_own() mutex_own(&lizard::gcs->m_scn_list_mutex)
#endif

#define gcn_order_mutex_enter()                   \
  do {                                            \
    ut_ad(lizard::gcs != nullptr);                \
    mutex_enter(&lizard::gcs->m_gcn_order_mutex); \
  } while (0)

#define gcn_order_mutex_exit()                   \
  do {                                           \
    ut_ad(lizard::gcs != nullptr);               \
    mutex_exit(&lizard::gcs->m_gcn_order_mutex); \
  } while (0)

#ifdef UNIV_DEBUG
#define gcn_order_mutex_own() mutex_own(&lizard::gcs->m_gcn_order_mutex)
#endif

#define gcn_persist_mutex_enter()                   \
  do {                                              \
    ut_ad(lizard::gcs != nullptr);                  \
    mutex_enter(&lizard::gcs->m_gcn_persist_mutex); \
  } while (0)

#define gcn_persist_mutex_exit()                   \
  do {                                             \
    ut_ad(lizard::gcs != nullptr);                 \
    mutex_exit(&lizard::gcs->m_gcn_persist_mutex); \
  } while (0)

#ifdef UNIV_DEBUG
#define gcn_persist_mutex_own() mutex_own(&lizard::gcs->m_gcn_persist_mutex)
#endif

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
#define assert_lizard_min_safe_scn_valid() \
  do {                                     \
    lizard::min_safe_scn_valid();          \
  } while (0)

#else

#define assert_lizard_min_safe_scn_valid()

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

#endif  // lizard0gcs_h define
