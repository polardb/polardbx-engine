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

/** @file include/lizard0gcs0hist.cc
 Transaction Commit Number Snapshot

 Created 2020-09-22 by Jianwei.zhao
 *******************************************************/

#include "sql_thd_internal_api.h"

#include "lizard0gcs0hist.h"
#include "lizard0undo.h"
#include "sql/dd/types/object_table.h"
#include "sql/dd/types/object_table_definition.h"

#include "dict0dd.h"
#include "lizard0gcs.h"
#include "lizard0scn.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0sel.h"
#include "srv0srv.h"
#include "trx0trx.h"

#ifdef UNIV_PFS_THREAD
mysql_pfs_key_t scn_history_thread_key;
#endif

#ifdef UNIV_PFS_RWLOCK
mysql_pfs_key_t commit_snapshot_rw_lock_key;
#endif

#ifdef UNIV_PFS_MEMORY
PSI_memory_key commit_snapshot_mem_key;
#endif

namespace lizard {

/** How many partition of commit snapshot buffer. */
ulint srv_commit_snapshot_partition = 8;

/** Commit snapshot buffer size. */
ulint srv_commit_snapshot_capacity = 60;

/** Whether enable commit snapshot search to
 * find a suitable up_limit_tid for Vision::sees on
 * secondary index.
 */
bool srv_commit_snapshot_search_enabled = true;

static bool scn_history_start_shutdown = false;

os_event_t scn_history_event = nullptr;

/** SCN rolling forward interval */
ulint srv_scn_history_interval = 3;

/** SCN rolling forward task */
bool srv_scn_history_task_enabled = true;

/** The max time of scn history record keeped */
ulint srv_scn_history_keep_days = 7;

/** Create INNODB_FLASHBACK_SNAPSHOT table */
dd::Object_table *create_innodb_scn_hist_table() {
  dd::Object_table *tbl = nullptr;
  dd::Object_table_definition *def = nullptr;

  tbl = dd::Object_table::create_object_table();
  ut_ad(tbl);

  tbl->set_hidden(false);
  def = tbl->target_table_definition();
  def->set_table_name(SCN_HISTORY_TABLE_NAME);
  def->add_field(0, "scn", "scn BIGINT UNSIGNED NOT NULL");
  def->add_field(1, "utc", "utc BIGINT UNSIGNED NOT NULL");
  def->add_field(2, "memo", "memo TEXT");
  /** SCN as primary key */
  def->add_index(0, "index_pk", "PRIMARY KEY(scn)");
  /** UTC index to speed lookup*/
  def->add_index(1, "index_utc", "index(utc)");

  return tbl;
}

/**
  Interpret the SCN and UTC from select node every record.

  @param[in]      node      SEL_NODE_T
  @param[in/out]  result    SCN_TRANFORM_RESULT_T

  @retval         true      Unused
*/
static bool srv_fetch_scn_history_step(void *node_void, void *result_void) {
  sel_node_t *node = (sel_node_t *)node_void;
  scn_transform_result_t *result = (scn_transform_result_t *)(result_void);
  que_common_t *cnode;
  int i;
  ut_ad(result->state == SCN_TRANSFORM_STATE::NOT_FOUND);

  result->state = SCN_TRANSFORM_STATE::SUCCESS;

  /* this should loop exactly 2 times for scn and utc  */
  for (cnode = static_cast<que_common_t *>(node->select_list), i = 0;
       cnode != NULL;
       cnode = static_cast<que_common_t *>(que_node_get_next(cnode)), i++) {
    const byte *data;
    dfield_t *dfield = que_node_get_val(cnode);
    dtype_t *type = dfield_get_type(dfield);
    ulint len = dfield_get_len(dfield);

    data = static_cast<const byte *>(dfield_get_data(dfield));

    switch (i) {
      case 0:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->scn = mach_read_from_8(data);
        break;
      case 1:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->utc = mach_read_from_8(data);
        break;
      default:
        ut_error;
    }
  }

  return true;
}

/**
  Try the best to transform UTC to SCN,
  it will search the first record which is more than the utc.
  Pls confirm the result state to judge whether the tranformation is success,
  and also it can report the error to client user if required through
  result.err.

  @param[in]      utc       Measure by second.
  @retval         result
*/
scn_transform_result_t try_scn_transform_by_utc(const ulint utc) {
  scn_transform_result_t result;
  trx_t *trx;
  pars_info_t *pinfo;

  trx = trx_allocate_for_background();

  trx->isolation_level = TRX_ISO_READ_UNCOMMITTED;

  if (srv_read_only_mode) {
    trx_start_internal_read_only(trx, UT_LOCATION_HERE);
  } else {
    trx_start_internal(trx, UT_LOCATION_HERE);
  }

  pinfo = pars_info_create();

  pars_info_add_ull_literal(pinfo, "utc", utc);

  pars_info_bind_function(pinfo, "fetch_scn_history_step",
                          srv_fetch_scn_history_step, &result);

  result.err = que_eval_sql(pinfo,
                            "PROCEDURE FETCH_SCN_HISTORY () IS\n"
                            "DECLARE FUNCTION fetch_scn_history_step;\n"
                            "DECLARE CURSOR scn_history_cur IS\n"
                            "  SELECT\n"
                            "  scn,\n"
                            "  utc \n"
                            "  FROM \"" SCN_HISTORY_TABLE_FULL_NAME
                            "\"\n"
                            "  WHERE\n"
                            "  utc >= :utc\n"
                            "  ORDER BY utc ASC;\n"

                            "BEGIN\n"

                            "OPEN scn_history_cur;\n"
                            "FETCH scn_history_cur INTO\n"
                            "  fetch_scn_history_step();\n"
                            "IF (SQL % NOTFOUND) THEN\n"
                            "  CLOSE scn_history_cur;\n"
                            "  RETURN;\n"
                            "END IF;\n"
                            "CLOSE scn_history_cur;\n"
                            "END;\n",
                            trx);

  /* pinfo is freed by que_eval_sql() */

  trx_commit_for_mysql(trx);

  trx_free_for_background(trx);

  return result;
}

/**
 * Make a commit snapshot.
 *
 * @retval	commit snapshot
 */
static commit_snap_t make_commit_snapshot() {
  commit_snap_t snap;
  /** Step 1: Get least active tid. */
  snap.up_limit_tid = gcs_load_min_active_trx_id();
  /** Step 2: Get utc. */
  snap.utc_sec = ut_time_system_us() / 1000000;

  /** Step 3. Generate new scn. */
  scn_list_mutex_enter();
  snap.scn = gcs->scn.new_scn();
  scn_list_mutex_exit();

  /** Step 4. Get gcn. */
  snap.gcn = gcs_load_gcn();

  return snap;
}

/**
  Rolling forward the SCN every SRV_SCN_HISTORY_INTERVAL.
*/
static dberr_t roll_forward_scn(commit_snap_t &snap) {
  pars_info_t *pinfo;
  dberr_t ret;
  ulint keep;
  trx_t *trx;

  keep = snap.utc_sec - (srv_scn_history_keep_days * 24 * 60 * 60);

  trx = trx_allocate_for_background();
  if (srv_read_only_mode) {
    trx_start_internal_read_only(trx, UT_LOCATION_HERE);
  } else {
    trx_start_internal(trx, UT_LOCATION_HERE);
  }

  pinfo = pars_info_create();

  pars_info_add_ull_literal(pinfo, "keep", keep);
  pars_info_add_ull_literal(pinfo, "scn", snap.scn);
  pars_info_add_ull_literal(pinfo, "utc", snap.utc_sec);
  pars_info_add_str_literal(pinfo, "memo", "");

  ret = que_eval_sql(pinfo,
                     "PROCEDURE ROLL_FORWARD_SCN() IS\n"
                     "BEGIN\n"

                     "DELETE FROM \"" SCN_HISTORY_TABLE_FULL_NAME
                     "\"\n"
                     "WHERE\n"
                     "utc < :keep; \n"

                     "INSERT INTO \"" SCN_HISTORY_TABLE_FULL_NAME
                     "\"\n"
                     "VALUES\n"
                     "(\n"
                     ":scn,\n"
                     ":utc,\n"
                     ":memo\n"
                     ");\n"
                     "END;",
                     trx);

  if (ret == DB_SUCCESS) {
    trx_commit_for_mysql(trx);
  } else {
    trx->op_info = "rollback of internal trx on rolling forward SCN";
    trx_rollback_to_savepoint(trx, NULL);
    trx->op_info = "";
    ut_a(trx->error_state == DB_SUCCESS);
  }

  trx_free_for_background(trx);

  return ret;
}

/** Start the background thread of scn rolling forward */
void srv_scn_history_thread(void) {
  dberr_t err;
  ulint sec_counter = 0;
  commit_snap_t snap;
  THD *thd = create_internal_thd();

  if (dict_sys->scn_hist == nullptr) {
    dict_sys->scn_hist = dd_table_open_on_name(
        thd, NULL, SCN_HISTORY_TABLE_FULL_NAME, false, DICT_ERR_IGNORE_NONE);
  }

  ut_a(dict_sys->scn_hist != nullptr);

  while (!scn_history_start_shutdown) {
    os_event_wait_time(scn_history_event, std::chrono::seconds{1});

    snap = make_commit_snapshot();

    gcs->new_snapshot(snap);

    Undo_retention::instance()->refresh_stat_data();

    if (srv_scn_history_task_enabled &&
        sec_counter++ % srv_scn_history_interval == 0)
      err = roll_forward_scn(snap);
    else
      err = DB_SUCCESS;

    if (err != DB_SUCCESS) {
      ib::error(ER_ROLL_FORWARD_SCN)
          << "Cannot rolling forward scn and save into"
          << SCN_HISTORY_TABLE_FULL_NAME << ": " << ut_strerr(err);
    }

    if (scn_history_start_shutdown) break;

    os_event_reset(scn_history_event);
  }

  destroy_internal_thd(thd);
}

/** Init the background thread attributes */
void srv_scn_history_thread_init() { scn_history_event = os_event_create(); }

/** Deinit the background thread attributes */
void srv_scn_history_thread_deinit() {
  ut_a(!srv_read_only_mode);
  ut_ad(!srv_thread_is_active(srv_threads.m_scn_hist));

  os_event_destroy(scn_history_event);
  scn_history_event = nullptr;
  scn_history_start_shutdown = false;
}

/** Shutdown the background thread of scn rolling forward */
void srv_scn_history_shutdown() {
  scn_history_start_shutdown = true;
  os_event_set(scn_history_event);
  srv_threads.m_scn_hist.join();
}

template <typename Item>
CRing<Item>::CRing(PSI_memory_key key, size_t size)
    : m_key(key), m_capacity(size), m_items(nullptr), m_head(0), m_tail(0) {
  m_items = ut::new_arr_withkey<Item>(ut::make_psi_memory_key(key),
                                      ut::Count{m_capacity});
  m_head = -1;
  m_tail = -1;
}

template <typename Item>
CRing<Item>::~CRing() {
  ut::delete_arr(m_items);
  m_items = nullptr;
  m_capacity = 0;
  m_head = -1;
  m_tail = -1;
}

/**
 * rebuild array of ring according to new size.
 *
 * @retval	size	new array size
 *
 * Do nothing if new size is equal to old size.
 */
template <typename Item>
void CRing<Item>::rebuild(size_t size) {
  ut_ad(m_items != nullptr);
  if (size == m_capacity) return;

  ut::delete_arr(m_items);

  m_items = ut::new_arr_withkey<Item>(ut::make_psi_memory_key(m_key),
                                      ut::Count{size});
  m_capacity = size;
  m_head = -1;
  m_tail = -1;
}

/**
 * Add new item into the ring buffer. overwrite last item
 * if new key value is equal with last item.
 */
template <typename Item>
void CRing<Item>::add(Item &item) {
  Item *ptr = nullptr;
  ut_ad(m_head >= m_tail);

  if (empty()) {
    m_head++;
    m_tail++;
    ptr = header();
  } else {
    ptr = header();
    /** If scn or gcn didn't change, only update up_limit_tid. */
    if (ptr->val_int() != item.val_int()) {
      m_head++;
      ptr = header();
    }
  }
  /** Assign new value. */
  *ptr = item;

  /** If full, push tail. */
  if (full()) {
    m_tail++;
  }
}

/**
 * Find the biggest item which is less than argument.
 *
 * @retval	item pointer
 */
template <typename Item>
Item *CRing<Item>::biggest_less_equal_than(const Item &lhs) {
  Item *ptr = nullptr;
  int upper, lower, mid;

  /** If ring buffer is empty, return nullptr. */
  if (empty()) return nullptr;

  /** head and tail pre-check. */
  if (tailer()->val_int() > lhs.val_int()) {
    return nullptr;
  } else if (header()->val_int() <= lhs.val_int()) {
    return header();
  } else {
    /** Binary search */
    upper = m_head;
    lower = m_tail;
    ut_a(upper > lower);
    mid = 0;

    while (upper >= lower) {
      mid = (upper + lower) / 2;
      if (at(mid)->val_int() > lhs.val_int()) {
        upper = mid - 1;
      } else {
        lower = mid + 1;
        /** Next lower will larger than argument, break loop. */
        if (lower >= m_tail && lower <= m_head &&
            at(lower)->val_int() > lhs.val_int()) {
          break;
        }
      }
    }
    ptr = at(mid);
    if (ptr->val_int() <= lhs.val_int()) {
      return ptr;
    } else {
      return nullptr;
    }
  }

  return nullptr;
}

template <typename Item>
Item *CBuffer<Item>::biggest_less_equal_than(const Item &lhs) {
  Item *ptr = nullptr;

  if ((ptr = m_ring_sec.biggest_less_equal_than(lhs)) != nullptr) {
    return ptr;
  } else {
    return m_ring_min.biggest_less_equal_than(lhs);
  }
}

template <typename Item>
Item *CBuffer<Item>::biggest_less_than(const Item &lhs) {
  Item *ptr = nullptr;
  if ((ptr = m_ring_sec.biggest_less_than(lhs)) != nullptr) {
    return ptr;
  } else {
    return m_ring_min.biggest_less_than(lhs);
  }
}

/**
 * Find the biggest item which is less than or equal with argument.
 *
 * @retval	item pointer
 */
template <typename Item>
Item *CRing<Item>::biggest_less_than(const Item &lhs) {
  Item *ptr = nullptr;
  int upper, lower, mid = 0;

  /** If ring buffer is empty, return nullptr. */
  if (empty()) return nullptr;

  /** head and tail pre-check. */
  if (tailer()->val_int() >= lhs.val_int()) {
    return nullptr;
  } else if (header()->val_int() < lhs.val_int()) {
    return header();
  } else {
    /** Binary search */
    upper = m_head;
    lower = m_tail;
    ut_a(upper > lower);

    while (upper >= lower) {
      mid = (upper + lower) / 2;
      if (at(mid)->val_int() >= lhs.val_int()) {
        upper = mid - 1;
      } else {
        lower = mid + 1;
        /** Next lower will larger than argument, break loop. */
        if (lower >= m_tail && lower <= m_head &&
            at(lower)->val_int() >= lhs.val_int()) {
          break;
        }
      }
    }
    ptr = at(mid);
    if (ptr->val_int() < lhs.val_int()) {
      return ptr;
    } else {
      return nullptr;
    }
  }
  return nullptr;
}

/** Constructor of commit snapshot management. */
CSnapshot_mgr::CSnapshot_mgr(PSI_memory_key key, size_t part, size_t buff_size)
    : m_part(part), m_buff_size(buff_size) {
  m_csnapshot_buffers = static_cast<CSnapshot_buffer *>(ut::malloc_withkey(
      ut::make_psi_memory_key(key), sizeof(CSnapshot_buffer) * m_part));
  m_latches = static_cast<rw_lock_t *>(ut::malloc_withkey(
      ut::make_psi_memory_key(key), sizeof(rw_lock_t) * m_part));

  for (size_t i = 0; i < m_part; i++) {
    ::new (m_csnapshot_buffers + i) CSnapshot_buffer(key, buff_size);
    rw_lock_create(commit_snapshot_rw_lock_key, m_latches + i,
                   LATCH_ID_COMMIT_SNAPSHOT_RW_LOCK);
  }
}

/** Destructor of commit snapshot management. */
CSnapshot_mgr::~CSnapshot_mgr() {
  for (size_t i = 0; i < m_part; i++) {
    (m_csnapshot_buffers + i)->~CSnapshot_buffer();
    rw_lock_free(m_latches + i);
  }

  ut::free(m_csnapshot_buffers);
  ut::free(m_latches);

  m_part = 0;
  m_buff_size = 0;
  m_csnapshot_buffers = nullptr;
  m_latches = nullptr;
}

template <typename Item>
void CBuffer<Item>::add(Item &item, utc_t utc_sec) {
  m_ring_sec.add(item);
  if (utc_sec % 60 == 0) {
    m_ring_min.add(item);
  }
  return;
}

void CSnapshot_buffer::push(const commit_snap_t &snap) {
  /** Add scn snapshot. */
  Snapshot_scn_vision scn_vision(snap.scn, snap.up_limit_tid);
  m_scn_buffer.add(scn_vision, snap.utc_sec);

  /** Add gcn snapshot. */
  Snapshot_gcn_vision gcn_vision(snap.gcn, snap.scn, snap.up_limit_tid);
  m_gcn_buffer.add(gcn_vision, snap.utc_sec);
}

/** Push a new commit snapshot */
void CSnapshot_mgr::push(const commit_snap_t &snap) {
  for (size_t i = 0; i < m_part; i++) {
    rw_lock_x_lock(m_latches + i, UT_LOCATION_HERE);
    (m_csnapshot_buffers + i)->push(snap);
    rw_lock_x_unlock(m_latches + i);
  }
}

template <typename Item>
trx_id_t CSnapshot_mgr::search_up_limit_tid(const Item &lhs) {
  Item *ptr = nullptr;
  trx_id_t tid = 0;
  ut_ad(lhs.is_vision());

  /** If didn't enable, return 0 instead. */
  if (!srv_commit_snapshot_search_enabled) {
    return tid;
  }

  /** Generate a random number to partition contention. */
  size_t partition_num = pthread_self() % m_part;

  rw_lock_s_lock(m_latches + partition_num, UT_LOCATION_HERE);
  ptr = m_csnapshot_buffers[partition_num].buffer<Item>()->search(lhs);
  if (ptr != nullptr) {
    ut_a(ptr->val_int() <= lhs.val_int());
    tid = ptr->up_limit_tid();
  }
  rw_lock_s_unlock(m_latches + partition_num);

  return tid;
}

template trx_id_t CSnapshot_mgr::search_up_limit_tid<Snapshot_gcn_vision>(
    const Snapshot_gcn_vision &lhs);

template trx_id_t CSnapshot_mgr::search_up_limit_tid<Snapshot_scn_vision>(
    const Snapshot_scn_vision &lhs);

}  // namespace lizard
