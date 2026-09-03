/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/consensus/komodo0fifo0cache.cc
  Komodo fifo cache.

 Created 2025-05-16 by Jiyang.zhang
 *******************************************************/
#include "sql/komodo0xlog.h"
#include "sql/komodo0assert.h"

#include "sql/komodo0mon.h"
#include "sql/consensus/komodo0fifo0cache.h"
#include "sql/consensus/komodo0mem0pool.h"
#include "sql/komodo0consensus0xlog.h"
#include "sql/consensus_log_manager.h"

namespace komodo {

PSI_memory_key key_memory_FIFO_CACHE;

PSI_rwlock_key key_rwlock_FIFO_RESIZE;

PSI_mutex_key key_mutex_FIFO_CACHE;

/**
  Deque base on an array.
  Ability to search for the corresponding element at O(1) cost
*/
template <typename Slot>
class Deque_by_array {
 public:
  Deque_by_array(PSI_memory_key key_memory_arg,
                 const size_t preallocated_size_arg)
      : m_front(nullptr),
        m_back(nullptr),
        m_slots_array(nullptr),
        m_slots_total(0) {
    create(key_memory_arg, preallocated_size_arg);
  }

  ~Deque_by_array() { destroy(); }

  /** Holds raw slot array freed in destroy(); copying would double-free. */
  Deque_by_array(const Deque_by_array &) = delete;
  Deque_by_array &operator=(const Deque_by_array &) = delete;
  Deque_by_array(Deque_by_array &&) = delete;
  Deque_by_array &operator=(Deque_by_array &&) = delete;

  void create(PSI_memory_key key_memory_arg,
              const size_t preallocated_size_arg) {
    m_slots_total = (preallocated_size_arg + sizeof(Slot) - 1) / sizeof(Slot);
    kut_a(m_slots_total > 0);

    m_slots_array = (Slot *)my_malloc(
        key_memory_arg, m_slots_total * sizeof(Slot), MYF(MY_FAE));

    for (size_t i = 0; i < m_slots_total; ++i) {
      new (&m_slots_array[i]) Slot();
    }

    m_front = m_back = nullptr;
  }

  void destroy() {
    if (m_slots_array) {
      kut_a(is_empty());

      for (size_t i = 0; i < m_slots_total; ++i) {
        m_slots_array[i].destroy();
      }

      my_free(m_slots_array);
      m_slots_array = nullptr;
    }
  }

  bool is_empty() const { return m_back == nullptr; }

  bool is_full() const {
    return !is_empty() && fetch_slot_by_relative_pos(m_back, 1) == m_front;
  }

  Slot *front() { return m_front; }

  Slot *back() { return m_back; }

  Slot *next(const Slot *slot) const {
    kut_a(check_in_queue(slot));

    if (slot == m_back) {
      return nullptr;
    }

    return fetch_slot_by_relative_pos(slot, 1);
  }

  Slot *prev(const Slot *slot) const {
    kut_a(check_in_queue(slot));

    if (slot == m_front) {
      return nullptr;
    }

    return fetch_slot_by_relative_pos(slot, -1);
  }

  void push_front() = delete;

  Slot *push_back(uint64 index) {
    Slot *slot;

    if (is_full()) {
      return nullptr;
    }

    slot = fetch_slot_by_index(index);
    if (is_empty()) {
      m_back = m_front = slot;
    } else {
      kut_a(fetch_slot_by_relative_pos(m_back, 1) == slot);
      m_back = slot;
    }

    return slot;
  }

  void pop_back() {
    if (prev(m_back) == nullptr) {
      m_back = m_front = nullptr;
    } else {
      m_back = prev(m_back);
    }
  }

  void pop_front() {
    if (next(m_front) == nullptr) {
      m_back = m_front = nullptr;
    } else {
      m_front = next(m_front);
    }
  }

  Slot *fetch_slot_by_index(uint64 index) {
    uint64 slot_pos = index % m_slots_total;
    return m_slots_array + slot_pos;
  }

  bool check_in_queue(const Slot *slot) const {
    if (!slot) {
      return false;
    } else if (m_front < m_back) {
      return (slot >= m_front && slot <= m_back);
    } else {
      return (slot >= m_front && slot < m_slots_array + m_slots_total) ||
             (slot <= m_back && slot >= m_slots_array);
    }
  }

  size_t capacity() const { return m_slots_total; }

  bool almost_full(unsigned int percentage) const {
    return free() * 100 <= capacity() * percentage;
  }

 private:
  Slot *fetch_slot_by_relative_pos(const Slot *slot,
                                   int64_t relative_pos) const {
    return m_slots_array +
           (slot + m_slots_total + relative_pos - m_slots_array) %
               m_slots_total;
  }

  size_t free() const {
    if (is_empty()) return m_slots_total;
    if (m_front <= m_back) return m_slots_total - (m_back - m_front + 1);
    return (m_front - m_back - 1);
  }

 private:
  Slot *m_front;
  Slot *m_back;
  Slot *m_slots_array;
  size_t m_slots_total;
};

/*********************************************************************/
/* Fifo base on the preallocate memory pool. */
/*********************************************************************/
template <typename MEM_POOL>
class Mem_pool_fifo : public Fifo_cache {
 public:
  /** Minmum size of the preallocated memory. */
  static constexpr size_t PREALLOC_MINIMUM_SIZE = 2 * 1024 * 1024;

  static constexpr size_t PREALLOC_SLOT_ARRAY_PERCENTAGE = 50;

 public:
  typedef typename MEM_POOL::BUFFER_TYPE Buffer_t;

 public:
  struct Log_slot {
    mysql_rwlock_t rw_lock;
    uint64 index;
    uint64 term;
    uint flag;
    bool outer;
    Buffer_t buffer;

    Log_slot() { create(); }

    void create() {
      /** No need to use init PFS rw_lock */
      mysql_rwlock_init(0, &rw_lock);
      index = INVALID_INDEX;
      term = 0;
      flag = 0;
      outer = false;
      buffer.reset();
    }

    void destroy() { mysql_rwlock_destroy(&rw_lock); }

    void mark_used(uint64 use_index) { index = use_index; }
    void mark_unused() { index = INVALID_INDEX; }
    bool is_used() const { return index != INVALID_INDEX; }
    bool is_expect(uint64 expect_index) const { return index == expect_index; }
  };

  typedef Deque_by_array<Log_slot> SLOT_QUEUE;

 public:
  Mem_pool_fifo(const size_t preallocated_size_arg);

  virtual ~Mem_pool_fifo() override;

  Buffer *open_buffer(uint64 term, uint64 index, size_t payload_len, bool outer,
                      uint flag, bool malloc_if_not_enough = false,
                      bool mutex_held = false) override;

  void close_buffer(uint64 index, bool error) override;

  void open_and_write_buffer(uint64 term, uint64 index, size_t payload_len,
                             const uchar *payload, bool outer,
                             uint flag) override;

  [[nodiscard]] int get(uint64 index, uint64 *term, std::string &log_content,
                        bool *outer, uint *flag) override;

  void evict_newest_until(uint64 low_index) override;

  void evict_oldest_until(uint64 up_index, bool error) override;

  void evict_oldest_try(uint64 hint_index, uint64 hint_count) override;

  /**
    Try to get total log size between [begin_index, end_index] which in cache.
  */
  uint64 get_log_size(uint64 begin_index, uint64 end_index,
                      uint64 max_packet_size) override;

  uint64 get_log_size();

  uint64 get_oldest_index();

  uint64 get_log_count();

  /**
    Export cache_status.
  */
  void export_status(Fifo_cache_status *cache_status) override;

  void resize(const size_t new_size_in_bytes) override;

  bool has_space_for(const size_t index_count,
                     const size_t space_bytes) const override {
    /** Might use malloc if not enough space. */
    (void)(space_bytes);

    if (m_slot_queue->capacity() > index_count) {
      return true;
    }

    return false;
  }

  void lock() override { mysql_mutex_lock(&m_mutex); }

  void unlock() override { mysql_mutex_unlock(&m_mutex); }

 private:
  void get_by_slot(Log_slot *slot, uint64 *term, uint64 *index,
                   std::string &log_content, bool *outer, uint *flag);

  void evict_oldest_one();

  void mark_unused(Log_slot *slot, bool error = false);

  uint64 get_oldest_index_no_lock();

  void swap(MEM_POOL *&mem_pool, SLOT_QUEUE *&slot_queue);

  bool almost_full() const {
    constexpr unsigned int FULL_THRESHOLD = 10;
    return m_mem_pool->almost_full(FULL_THRESHOLD) ||
           m_slot_queue->almost_full(FULL_THRESHOLD);
  }

 private:
  /** Memory pool */
  MEM_POOL *m_mem_pool;

  /** Slot deque */
  SLOT_QUEUE *m_slot_queue;

  /**
    Mutex that protect m_slot_queue and m_mem_pool.

    In write path:
    1. Hold m_mutex to allocate memory and push_back a new slot
    2. Hold the write lock of the new slot.
    3. Release m_mutex
    4. Modify the new slot
    5. Release the write lock of the new slot
  */
  mysql_mutex_t m_mutex;

  /**
    rwlock that to protect resize.

    In read path:
    1. Hold m_resize_rwlock (read lock)
    2. Hold read lock of the slot
    3. Release read lock of the slot
    4. Release m_resize_rwlock

    In resize path:
    1. Hold m_mutex
    2. Hold m_resize_rwlock (write lock)
    3. Hold every read_lock of the slot and copy
    4. Release m_resize_rwlock
    5. Release m_mutex
  */
  mysql_rwlock_t m_resize_rwlock;
};

template <typename MEM_POOL>
Mem_pool_fifo<MEM_POOL>::Mem_pool_fifo(const size_t preallocated_size_arg) {
  size_t preallocated_size;

  preallocated_size = std::max(preallocated_size_arg, PREALLOC_MINIMUM_SIZE);

  m_mem_pool = new MEM_POOL(key_memory_FIFO_CACHE, preallocated_size);

  m_slot_queue = new SLOT_QUEUE(
      key_memory_FIFO_CACHE,
      (preallocated_size * PREALLOC_SLOT_ARRAY_PERCENTAGE) / 100);

  mysql_mutex_init(key_mutex_FIFO_CACHE, &m_mutex, MY_MUTEX_INIT_FAST);

  mysql_rwlock_init(key_rwlock_FIFO_RESIZE, &m_resize_rwlock);
}

template <typename MEM_POOL>
Mem_pool_fifo<MEM_POOL>::~Mem_pool_fifo() {
  evict_newest_until(0);

  delete m_slot_queue;
  m_slot_queue = nullptr;

  delete m_mem_pool;
  m_mem_pool = nullptr;

  mysql_mutex_destroy(&m_mutex);

  mysql_rwlock_destroy(&m_resize_rwlock);
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::open_and_write_buffer(uint64 term, uint64 index,
                                                    size_t payload_len,
                                                    const uchar *payload,
                                                    bool outer, uint flag) {
  Log_slot *slot;
  mysql_rwlock_t *rwlock;
  Buffer_t buffer;
  size_t mem_evict_count = 0;
  size_t slot_evict_count = 0;

  /** 1. Hold Fifo cache mutex */
  mysql_mutex_lock(&m_mutex);

  /** 2.1 Reserve (or alloc) buffer */
  while (!m_mem_pool->allocate(payload_len, &buffer)) {
    evict_oldest_one();
    mem_evict_count++;
  }

  /** 2.2 Reserve slot */
  while (!(slot = m_slot_queue->push_back(index))) {
    evict_oldest_one();
    slot_evict_count++;
  }

  /** 3. Hold the write lock of the slot. */
  rwlock = &slot->rw_lock;
  mysql_rwlock_wrlock(rwlock);

  /** 4. Update monitor status. */
  fifo_cache_status.open_buffer++;
  fifo_cache_status.open_buffer_memory_evict += mem_evict_count;
  fifo_cache_status.open_buffer_slot_evict += slot_evict_count;

  /** 5. Release Fifo cache mutex */
  mysql_mutex_unlock(&m_mutex);

  /** 6. Fill the payload and the slot. */
  buffer.append(payload, payload_len);
  slot->buffer = std::move(buffer);
  slot->flag = flag;
  slot->outer = outer;
  slot->term = term;
  slot->mark_used(index);

  /** 7. Release the write lock of the slot. */
  mysql_rwlock_unlock(rwlock);
}

template <typename MEM_POOL>
Buffer *Mem_pool_fifo<MEM_POOL>::open_buffer(uint64 term, uint64 index,
                                             size_t payload_len, bool outer,
                                             uint flag,
                                             bool malloc_if_not_enough,
                                             bool mutex_held) {
  Log_slot *slot;
  mysql_rwlock_t *rwlock;
  Buffer_t buffer;
  size_t mem_evict_count = 0;
  size_t slot_evict_count = 0;

  /** 1. Ensure the FIFO cache mutex is held. */
  if (mutex_held) {
    /** Caller claims to hold the mutex already. */
    mysql_mutex_assert_owner(&m_mutex);
  } else {
    mysql_mutex_lock(&m_mutex);
  }

  /** 2.1 Reserve (or alloc) buffer */
  while (!m_mem_pool->allocate(payload_len, &buffer, malloc_if_not_enough)) {
    evict_oldest_one();
    mem_evict_count++;
  }

  /** 2.2 Reserve slot */
  while (!(slot = m_slot_queue->push_back(index))) {
    evict_oldest_one();
    slot_evict_count++;
  }

  /** 3. Hold the write lock of the slot. */
  rwlock = &slot->rw_lock;
  mysql_rwlock_wrlock(rwlock);

  /** 4. Update monitor status. */
  fifo_cache_status.open_buffer++;
  fifo_cache_status.open_buffer_memory_evict += mem_evict_count;
  fifo_cache_status.open_buffer_slot_evict += slot_evict_count;

  /** 5. Release FIFO cache mutex unless ownership is retained by caller. */
  if (!mutex_held) mysql_mutex_unlock(&m_mutex);

  slot->buffer = std::move(buffer);
  slot->flag = flag;
  slot->outer = outer;
  slot->term = term;

  return &slot->buffer;
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::close_buffer(uint64 index, bool error) {
  Log_slot *slot;

  slot = m_slot_queue->fetch_slot_by_index(index);
  if (!error) {
    slot->mark_used(index);
    kut_a(slot->buffer.capacity() >= slot->buffer.size());
    mysql_rwlock_unlock(&slot->rw_lock);
    xlog_engine_get_log_manager().set_cache_index(index);
  } else {
    mysql_mutex_assert_owner(mysql_bin_log.get_log_lock());
    mysql_rwlock_unlock(&slot->rw_lock);

    /**
      Holding another Mutex (LOCK_log) to ensure a slot with bigger index is
      impossible. So all slots will actually evict.

      Consensus Cache index is not advanced, so the slot never be read.
    */
    evict_oldest_until(index, error);
  }
}

template <typename MEM_POOL>
int Mem_pool_fifo<MEM_POOL>::get(uint64 index, uint64 *term,
                                 std::string &log_content, bool *outer,
                                 uint *flag) {
  Log_slot *slot;
  mysql_rwlock_t *rwlock;
  int error = 0;

  if (index == INVALID_INDEX) return CLC_ALREADY_SWAP_OUT;

  mysql_rwlock_rdlock(&m_resize_rwlock);

  slot = m_slot_queue->fetch_slot_by_index(index);
  rwlock = &slot->rw_lock;

  mysql_rwlock_rdlock(rwlock);
  fifo_cache_status.read_requests++;
  if (!slot->is_expect(index) ||
      DBUG_EVALUATE_IF("skip_consensus_fifo_cache", 1, 0)) {
    fifo_cache_status.read_miss++;
    error = CLC_ALREADY_SWAP_OUT;
    goto end;
  }

  *term = slot->term;
  *outer = slot->outer;
  *flag = slot->flag;
  slot->buffer.deep_copy(log_content);

end:
  mysql_rwlock_unlock(rwlock);
  mysql_rwlock_unlock(&m_resize_rwlock);
  return error;
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::get_by_slot(Log_slot *slot, uint64 *term,
                                          uint64 *index,
                                          std::string &log_content, bool *outer,
                                          uint *flag) {
  mysql_rwlock_t *rwlock;

  rwlock = &slot->rw_lock;

  /** Must already owned m_resize_rwlock because slot was used. */
  mysql_rwlock_rdlock(rwlock);
  fifo_cache_status.read_requests++;

  kut_a(slot->is_used());
  kut_ad(m_slot_queue->check_in_queue(slot));

  *term = slot->term;
  *index = slot->index;
  *outer = slot->outer;
  *flag = slot->flag;
  slot->buffer.deep_copy(log_content);

  mysql_rwlock_unlock(rwlock);
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::mark_unused(Log_slot *slot, bool error) {
  assert(slot);
  kut_a(slot->buffer.capacity() > 0);
  m_mem_pool->deallocate(&slot->buffer, error);
  slot->mark_unused();
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::evict_oldest_one() {
  Log_slot *slot;
  mysql_mutex_assert_owner(&m_mutex);

  slot = m_slot_queue->front();
  kut_a(slot);

  mysql_rwlock_wrlock(&slot->rw_lock);

  mark_unused(slot);
  m_slot_queue->pop_front();

  mysql_rwlock_unlock(&slot->rw_lock);
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::evict_newest_until(uint64 low_index) {
  Log_slot *slot;

  mysql_mutex_lock(&m_mutex);

  while ((slot = m_slot_queue->back())) {
    mysql_rwlock_wrlock(&slot->rw_lock);
    raii::Sentry<> lock_guard{
        [&]() -> void { mysql_rwlock_unlock(&slot->rw_lock); }};

    if (!slot->is_used() || slot->index >= low_index) {
      mark_unused(slot);
      m_slot_queue->pop_back();
    } else {
      break;
    }
  }

  mysql_mutex_unlock(&m_mutex);
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::evict_oldest_until(uint64 up_index, bool error) {
  Log_slot *slot;

  mysql_mutex_lock(&m_mutex);

  while ((slot = m_slot_queue->front())) {
    mysql_rwlock_wrlock(&slot->rw_lock);
    raii::Sentry<> lock_guard{
        [&]() -> void { mysql_rwlock_unlock(&slot->rw_lock); }};

    if (!slot->is_used() || slot->index <= up_index) {
      mark_unused(slot, error);
      m_slot_queue->pop_front();
    } else {
      break;
    }
  }

  mysql_mutex_unlock(&m_mutex);
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::evict_oldest_try(uint64 hint_index,
                                               uint64 hint_count) {
  constexpr uint64 INDEX_SAFE_MARGIN = 100;
  bool lock_failed;

  Log_slot *slot;
  uint64 evict_count = 0;
  hint_index =
      hint_index > INDEX_SAFE_MARGIN ? hint_index - INDEX_SAFE_MARGIN : 0;

  DBUG_EXECUTE_IF("fifo_cache_disable_evicit_oldest_try", { return; });

  fifo_cache_status.try_evict++;
  lock_failed = mysql_mutex_trylock(&m_mutex);
  if (lock_failed) {
    fifo_cache_status.try_evict_lock_failed++;
    return;
  }

  if (!almost_full()) {
    goto end;
  }

  while ((slot = m_slot_queue->front())) {
    mysql_rwlock_wrlock(&slot->rw_lock);
    raii::Sentry<> lock_guard{
        [&]() -> void { mysql_rwlock_unlock(&slot->rw_lock); }};

    if ((!slot->is_used() || slot->index <= hint_index) &&
        evict_count < hint_count && almost_full()) {
      mark_unused(slot);
      m_slot_queue->pop_front();
      evict_count++;
    } else {
      break;
    }
  }

end:
  mysql_mutex_unlock(&m_mutex);
}

template <typename MEM_POOL>
uint64 Mem_pool_fifo<MEM_POOL>::get_log_size(uint64 begin_index,
                                             uint64 end_index,
                                             uint64 max_packet_size) {
  Log_slot *slot;
  Log_slot *cur_slot;
  uint64 total_size = 0;

  mysql_mutex_lock(&m_mutex);

  slot = m_slot_queue->back();
  while (slot) {
    cur_slot = slot;
    mysql_rwlock_rdlock(&cur_slot->rw_lock);
    raii::Sentry<> lock_guard{
        [&]() -> void { mysql_rwlock_unlock(&cur_slot->rw_lock); }};

    kut_ad(cur_slot->is_used());
    if (cur_slot->index < begin_index) {
      break;
    }

    if (cur_slot->index >= begin_index && cur_slot->index <= end_index) {
      total_size += cur_slot->buffer.size();
    }

    if (total_size > max_packet_size) {
      break;
    }

    slot = m_slot_queue->prev(cur_slot);
  }

  mysql_mutex_unlock(&m_mutex);

  return total_size;
}

template <typename MEM_POOL>
uint64 Mem_pool_fifo<MEM_POOL>::get_log_size() {
  uint64 total_size = 0;

  mysql_mutex_lock(&m_mutex);

  total_size = m_mem_pool->used_buffer();

  mysql_mutex_unlock(&m_mutex);

  return total_size;
}

template <typename MEM_POOL>
uint64 Mem_pool_fifo<MEM_POOL>::get_log_count() {
  Log_slot *slot;
  uint64 log_count = 0;

  mysql_mutex_lock(&m_mutex);

  for (slot = m_slot_queue->front(); slot; slot = m_slot_queue->next(slot)) {
#ifndef NDEBUG
    mysql_rwlock_rdlock(&slot->rw_lock);
    raii::Sentry<> lock_guard{
        [&]() -> void { mysql_rwlock_unlock(&slot->rw_lock); }};

    kut_a(slot->is_used());
#endif
    log_count++;
  }

  mysql_mutex_unlock(&m_mutex);
  return log_count;
}

template <typename MEM_POOL>
uint64 Mem_pool_fifo<MEM_POOL>::get_oldest_index() {
  uint64 oldest_index;

  mysql_mutex_lock(&m_mutex);
  oldest_index = get_oldest_index_no_lock();
  mysql_mutex_unlock(&m_mutex);

  return oldest_index;
}

template <typename MEM_POOL>
uint64 Mem_pool_fifo<MEM_POOL>::get_oldest_index_no_lock() {
  Log_slot *slot;
  uint64 oldest_index;

  mysql_mutex_assert_owner(&m_mutex);

  slot = m_slot_queue->front();
  if (slot) {
    mysql_rwlock_rdlock(&slot->rw_lock);
    oldest_index = slot->index;
    mysql_rwlock_unlock(&slot->rw_lock);
  } else {
    oldest_index = INVALID_INDEX;
  }

  return oldest_index;
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::export_status(Fifo_cache_status *cache_status) {
  cache_status->used_size = get_log_size();
  cache_status->first_index = get_oldest_index();
  cache_status->log_count = get_log_count();
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::resize(const size_t new_size_in_bytes) {
  Log_slot *slot;
  Mem_pool_fifo<MEM_POOL> *tmp_fifo;

  std::string log_content;
  uint64 index;
  uint64 term;
  bool outer;
  uint flag;

  tmp_fifo = new Mem_pool_fifo<MEM_POOL>(new_size_in_bytes);

  mysql_mutex_lock(&m_mutex);
  mysql_rwlock_wrlock(&m_resize_rwlock);

  for (slot = m_slot_queue->front(); slot; slot = m_slot_queue->next(slot)) {
    get_by_slot(slot, &term, &index, log_content, &outer, &flag);
    tmp_fifo->open_and_write_buffer(term, index, log_content.size(),
                                    (const uchar *)(log_content.c_str()), outer,
                                    flag);
    log_content.clear();
  }

  tmp_fifo->swap(m_mem_pool, m_slot_queue);

  delete tmp_fifo;

  mysql_rwlock_unlock(&m_resize_rwlock);
  mysql_mutex_unlock(&m_mutex);
}

template <typename MEM_POOL>
void Mem_pool_fifo<MEM_POOL>::swap(MEM_POOL *&mem_pool,
                                   SLOT_QUEUE *&slot_queue) {
  std::swap(m_mem_pool, mem_pool);
  std::swap(m_slot_queue, slot_queue);
}

typedef Mem_pool_fifo<Redundant_mem_pool> Redundant_mem_fifo;

Fifo_cache *create_fifo_cache(size_t max_fifo_cache_size_arg) {
  switch (opt_fifo_mem_pool_type) {
    case MEM_POOL_REDUNDANT:
      return new Redundant_mem_fifo(max_fifo_cache_size_arg);
    default:
      return nullptr;
  }
}

}  // namespace komodo
