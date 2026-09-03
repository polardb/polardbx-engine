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

/** @file trx/lizard0gcs0snapshot.cc
 Transaction Commit Number Snapshot

 Created 2020-09-22 by Jianwei.zhao
 *******************************************************/
#include "sync0rw.h"

#include "lizard0gcs0snapshot.h"

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
Item *CRing<Item>::biggest_less_equal_than(const Item *lhs) {
  Item *ptr = nullptr;
  int upper, lower, mid;

  /** If ring buffer is empty, return nullptr. */
  if (empty()) return nullptr;

  /** head and tail pre-check. */
  if (tailer()->val_int() > lhs->val_int()) {
    return nullptr;
  } else if (header()->val_int() <= lhs->val_int()) {
    return header();
  } else {
    /** Binary search */
    upper = m_head;
    lower = m_tail;
    ut_a(upper > lower);
    mid = 0;

    while (upper >= lower) {
      mid = (upper + lower) / 2;
      if (at(mid)->val_int() > lhs->val_int()) {
        upper = mid - 1;
      } else {
        lower = mid + 1;
        /** Next lower will larger than argument, break loop. */
        if (lower >= m_tail && lower <= m_head &&
            at(lower)->val_int() > lhs->val_int()) {
          break;
        }
      }
    }
    ptr = at(mid);
    if (ptr->val_int() <= lhs->val_int()) {
      return ptr;
    } else {
      return nullptr;
    }
  }

  return nullptr;
}

template <typename Item>
Item *CBuffer<Item>::biggest_less_equal_than(const Item *lhs) {
  Item *ptr = nullptr;

  if ((ptr = m_ring_sec.biggest_less_equal_than(lhs)) != nullptr) {
    return ptr;
  } else {
    return m_ring_min.biggest_less_equal_than(lhs);
  }
}

template <typename Item>
Item *CBuffer<Item>::biggest_less_than(const Item *lhs) {
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
Item *CRing<Item>::biggest_less_than(const Item *lhs) {
  Item *ptr = nullptr;
  int upper, lower, mid = 0;

  /** If ring buffer is empty, return nullptr. */
  if (empty()) return nullptr;

  /** head and tail pre-check. */
  if (tailer()->val_int() >= lhs->val_int()) {
    return nullptr;
  } else if (header()->val_int() < lhs->val_int()) {
    return header();
  } else {
    /** Binary search */
    upper = m_head;
    lower = m_tail;
    ut_a(upper > lower);

    while (upper >= lower) {
      mid = (upper + lower) / 2;
      if (at(mid)->val_int() >= lhs->val_int()) {
        upper = mid - 1;
      } else {
        lower = mid + 1;
        /** Next lower will larger than argument, break loop. */
        if (lower >= m_tail && lower <= m_head &&
            at(lower)->val_int() >= lhs->val_int()) {
          break;
        }
      }
    }
    ptr = at(mid);
    if (ptr->val_int() < lhs->val_int()) {
      return ptr;
    } else {
      return nullptr;
    }
  }
  return nullptr;
}

/** Constructor of commit snapshot management. */
CSnapshot::CSnapshot(PSI_memory_key key, size_t part, size_t buff_size)
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
CSnapshot::~CSnapshot() {
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
  komodo::Snapshot_scn_vision scn_vision(snap.scn, snap.up_limit_tid);
  m_scn_buffer.add(scn_vision, snap.utc_sec);

  /** Add gcn snapshot. */
  komodo::Snapshot_gcn_vision gcn_vision(snap.gcn, snap.up_limit_tid);
  m_gcn_buffer.add(gcn_vision, snap.utc_sec);
}

/** Push a new commit snapshot */
void CSnapshot::push(const commit_snap_t &snap) {
  for (size_t i = 0; i < m_part; i++) {
    rw_lock_x_lock(m_latches + i, UT_LOCATION_HERE);
    (m_csnapshot_buffers + i)->push(snap);
    rw_lock_x_unlock(m_latches + i);
  }
}

template <typename Item>
trx_id_t CSnapshot::search_up_limit_tid(const Item *lhs) {
  Item *ptr = nullptr;
  trx_id_t tid = 0;
  ut_ad(lhs->is_vision());

  /** If didn't enable, return 0 instead. */
  if (!srv_commit_snapshot_search_enabled) {
    return tid;
  }

  /** Generate a random number to partition contention. */
  size_t partition_num = pthread_self() % m_part;

  rw_lock_s_lock(m_latches + partition_num, UT_LOCATION_HERE);
  ptr = m_csnapshot_buffers[partition_num].buffer<Item>()->search(lhs);
  if (ptr != nullptr) {
    ut_a(ptr->val_int() <= lhs->val_int());
    tid = ptr->up_limit_tid();
  }
  rw_lock_s_unlock(m_latches + partition_num);

  return tid;
}

template trx_id_t CSnapshot::search_up_limit_tid<komodo::Snapshot_gcn_vision>(
    const komodo::Snapshot_gcn_vision *lhs);

template trx_id_t CSnapshot::search_up_limit_tid<komodo::Snapshot_scn_vision>(
    const komodo::Snapshot_scn_vision *lhs);

}  // namespace lizard
