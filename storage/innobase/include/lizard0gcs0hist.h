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

/** @file include/lizard0gcs0hist.h
 Transaction Commit Number Snapshot

 Created 2020-09-22 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gcs0hist_h
#define lizard0gcs0hist_h

#include "univ.i"

#include "db0err.h"
#include "os0event.h"
#include "sync0rw.h"
#include "trx0types.h"

#include "lizard0mon.h"
#include "lizard0scn0types.h"

#include "sql/lizard/lizard_snapshot.h"

#ifdef UNIV_PFS_THREAD
extern mysql_pfs_key_t scn_history_thread_key;
#endif

#ifdef UNIV_PFS_RWLOCK
extern mysql_pfs_key_t commit_snapshot_rw_lock_key;
#endif

#ifdef UNIV_PFS_MEMORY
extern PSI_memory_key commit_snapshot_mem_key;
#endif

namespace dd {
class Object_table;
}  // namespace dd

/** SCN history table named "INNODB_FLASHBACK_SNAPSHOT" */
#define SCN_HISTORY_TABLE_NAME "innodb_flashback_snapshot"
#define SCN_HISTORY_TABLE_FULL_NAME "mysql/innodb_flashback_snapshot"

/** Transformation state between scn and utc */
enum class SCN_TRANSFORM_STATE { NOT_FOUND, SUCCESS };

/** Transformation result between scn and utc */
struct scn_transform_result_t {
  scn_t scn;
  ulint utc;
  SCN_TRANSFORM_STATE state;
  dberr_t err;

  scn_transform_result_t() {
    scn = 0;
    utc = 0;
    state = SCN_TRANSFORM_STATE::NOT_FOUND;
    err = DB_SUCCESS;
  }
};

namespace lizard {

/** How many partition of commit snapshot buffer. */
extern ulint srv_commit_snapshot_partition;

/** Commit snapshot buffer size. */
extern ulint srv_commit_snapshot_capacity;

/** Whether enable commit snapshot search to
 * find a suitable up_limit_tid for Vision::sees on
 * secondary index.
 */
extern bool srv_commit_snapshot_search_enabled;

extern os_event_t scn_history_event;

/** SCN rolling forward interval */
extern ulint srv_scn_history_interval;

/** Max gap (seconds) allowed scn snapshot. */
constexpr ulint SRV_SCN_HISTORY_INTERVAL_MAX = 10;

/** SCN rolling forward task */
extern bool srv_scn_history_task_enabled;

/** The max time of scn history record keeped */
extern ulint srv_scn_history_keep_days;

/** Create INNODB_FLASHBACK_SNAPSHOT table */
dd::Object_table *create_innodb_scn_hist_table();

/** Start the background thread of scn rolling forward */
extern void srv_scn_history_thread(void);

/** Shutdown the background thread of scn rolling forward */
extern void srv_scn_history_shutdown();

/** Init the background thread attributes */
extern void srv_scn_history_thread_init();

/** Deinit the background thread attributes */
extern void srv_scn_history_thread_deinit();

/**
  Try the best to transform UTC to SCN,
  it will search the first record which is more than the utc.
  Pls confirm the result state to judge whether the tranformation is success,
  and also it can report the error to client user if required through
  result.err.

  @param[in]      utc       Measure by second.
  @retval         result
*/
extern scn_transform_result_t try_scn_transform_by_utc(const ulint utc);

/**------------------------------------------------------------------------*/
/** Transaction Commit Number Snapshot */
/**------------------------------------------------------------------------*/
/**
 * Transaction Commit Number Snapshot:
 *
 * Snapshot system commit numbers include :
 *   0) utc
 *   1) up_limit_tid
 *   2) scn
 *   3) gcn
 *
 *  to fill Commit Snapshot Buffer and Persist
 *  into innodb_flashback_snapshot.
 */

struct commit_snap_t {
  trx_id_t up_limit_tid;
  utc_t utc_sec;
  scn_t scn;
  gcn_t gcn;
};

template <typename Item>
class CRing {
 public:
  CRing(PSI_memory_key key, size_t size);

  virtual ~CRing();

  /**
   * rebuild array of ring according to new size.
   *
   * @retval	size	new array size
   *
   * Do nothing if new size is equal to old size.
   */
  void rebuild(size_t size);

  void add(Item &item);

  bool empty() { return m_head == -1; }

  bool full() {
    ut_ad(m_head >= m_tail);
    return size_t(m_head - m_tail) == m_capacity;
  }

  /**
   * Head item, require the ring buffer is not empty.
   *
   * @retval	head item pointer
   */
  Item *header() {
    ut_ad(!empty());
    return m_items + m_head % m_capacity;
  }

  /**
   * Tail item, require the ring buffer is not empty.
   *
   * @retval	tail item pointer
   */
  Item *tailer() {
    ut_ad(!empty());
    return m_items + m_tail % m_capacity;
  }

  Item *at(int seq) {
    ut_ad(seq >= 0);
    return m_items + seq % m_capacity;
  }

  /**
   * Find the biggest item which is less than argument.
   *
   * @retval	item pointer
   */
  Item *biggest_less_equal_than(const Item &lhs);

  /**
   * Find the biggest item which is less than or equal with argument.
   *
   * @retval	item pointer
   */
  Item *biggest_less_than(const Item &lhs);

 private:
  /** Memory allocation metrics. */
  PSI_memory_key m_key;
  /** Array size. */
  size_t m_capacity;
  /** Commit snapshot item array. */
  Item *m_items;
  /** Header of array. */
  int m_head;
  /** Tailer of array. */
  int m_tail;
};

/** Commit number buffer divided by time level. */
template <typename Item>
class CBuffer {
 public:
  CBuffer(PSI_memory_key key, size_t size)
      : m_ring_sec(key, size), m_ring_min(key, size) {}

  virtual ~CBuffer() {}

  void add(Item &item, utc_t utc_sec);

  /**
   * Find the biggest item which is less than argument.
   *
   * @retval	item pointer
   */
  Item *biggest_less_equal_than(const Item &lhs);

  /**
   * Find the biggest item which is less than or equal with argument.
   *
   * @retval	item pointer
   */
  Item *biggest_less_than(const Item &lhs);

  template <typename T>
  struct Type_selector {};

  Item *search(const Item &lhs, Type_selector<Snapshot_gcn_vision>) {
    Item *ptr = biggest_less_than(lhs);
#ifdef UNIV_DEBUG
    if (ptr != nullptr) {
      lizard_stats.commit_snapshot_gcn_search_hit.inc();
    }
#endif
    return ptr;
  }

  Item *search(const Item &lhs, Type_selector<Snapshot_scn_vision>) {
    Item *ptr = biggest_less_equal_than(lhs);
#ifdef UNIV_DEBUG
    if (ptr != nullptr) {
      lizard_stats.commit_snapshot_scn_search_hit.inc();
    }
#endif
    return ptr;
  }

  Item *search(const Item &lhs) { return search(lhs, Type_selector<Item>()); }

 private:
  /** Fill item by second */
  CRing<Item> m_ring_sec;
  /** Fill item by minute */
  CRing<Item> m_ring_min;
};

using Scn_buffer = CBuffer<Snapshot_scn_vision>;
using Gcn_buffer = CBuffer<Snapshot_gcn_vision>;

/** Commit snapshot buffer include all commit number */
class CSnapshot_buffer {
 public:
  CSnapshot_buffer(PSI_memory_key key, size_t size)
      : m_scn_buffer(key, size), m_gcn_buffer(key, size) {}

  template <typename T>
  struct Type_selector {};

  virtual ~CSnapshot_buffer() {}

  void push(const commit_snap_t &snap);

  Scn_buffer *get_buffer(Type_selector<Snapshot_scn_vision>) {
    return &m_scn_buffer;
  }

  Gcn_buffer *get_buffer(Type_selector<Snapshot_gcn_vision>) {
    return &m_gcn_buffer;
  }

  template <typename T>
  CBuffer<T> *buffer() {
    return get_buffer(Type_selector<T>());
  }

 private:
  Scn_buffer m_scn_buffer;
  Gcn_buffer m_gcn_buffer;
};

class CSnapshot_mgr {
 public:
  CSnapshot_mgr(PSI_memory_key key, size_t part, size_t buff_size);
  virtual ~CSnapshot_mgr();

  void push(const commit_snap_t &snap);

  template <typename Item>
  trx_id_t search_up_limit_tid(const Item &lhs);

 private:
  CSnapshot_buffer *m_csnapshot_buffers;
  rw_lock_t *m_latches;
  size_t m_part;
  size_t m_buff_size;
};

}  // namespace lizard

#endif  // lizard0gcs0hist_h
