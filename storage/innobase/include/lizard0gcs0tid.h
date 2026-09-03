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

/** @file include/lizard0gcs0tid.h
 Transaction number implementation.

 Created 2025-03-17 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gcs0tid_h
#define lizard0gcs0tid_h

#include "trx0sys.h"
#include "trx0trx.h"
#include "univ.i"

#include "lizard0trx0service.h"
#include "lizard0txn.h"

#ifdef UNIV_PFS_MUTEX
/* GCS tid list mutex PFS key */
extern mysql_pfs_key_t tid_list_mutex_key;
/* GCS tid persist mutex PFS key */
extern mysql_pfs_key_t tid_persist_mutex_key;

#endif

namespace lizard {

/** How many shard of tid list. */
extern uint32_t srv_gcs_tid_list_shards;

class TID {
 public:
  typedef struct meta_t {
    trx_id_t tid{0};
    /** Set tid if bigger */
    void set_tid_if_bigger(const meta_t &meta) {
      ut_ad(meta.valid());
      if (meta.tid > tid) {
        tid = meta.tid;
      }
    }

    bool valid() const { return tid > 0 && tid < TRX_ID_MAX; }
  } meta_t;

  /** tid persister */
  class TidPersister {
    /** The number gap of persist tid number into system tablespace */
    constexpr static size_t TID_NUMBER_MAGIN = TRX_SYS_TRX_ID_WRITE_MARGIN;

   public:
    /** Write tid value into tablespace.

        @param[in]	metadata   tid metadata
     */
    void write(const meta_t *metadata);
    /** Read tid value from tablespace and increase GCS_tid_NUMBER_MAGIN
     *  to promise unique.

        @param[in/out]	metadata   tid metadata

     */
    void read(meta_t *metadata);
  };

  /** list of trx;*/
  class tid_list_t {
   public:
    using List = PURE_LIST_BASE_NODE_T(trx_t, trx_list);
    using Mutex = ib_mutex_t;

   public:
    tid_list_t() {}

    virtual ~tid_list_t() {
      ut_ad(length() == 0);
      ut_ad(tid_mgr == nullptr);
    }

    /* init length */
    void init(TID *tid_arg) {
      UT_LIST_INIT(m_list);
      m_min_active_tid.store(0, std::memory_order_relaxed);
      mutex_create(LATCH_ID_TID_LIST, &m_mutex);

      tid_mgr = tid_arg;
    }

    void destroy() {
      ut_ad(length() == 0);
      m_min_active_tid.store(0, std::memory_order_relaxed);
      mutex_free(&m_mutex);
      tid_mgr = nullptr;
    }

    // Getter
    trx_id_t load_min_active_tid() const {
      ut_ad(m_min_active_tid != 0);
      return m_min_active_tid.load(std::memory_order_relaxed);
    }
    List *get_list() { return &m_list; }
    Mutex *get_mutex() { return &m_mutex; }

    /** Init min active tid after gcs boot and recovery. */
    void start_active_tid();

    /** Add into the tailer of list. */
    void add_list(trx_t *trx) {
      ut_ad(lock_own());
      UT_LIST_ADD_FIRST(m_list, trx);

      ut_ad(check_min_active_tid());
    }

    /** Remove trx from list and modify min active tid if necessary. */
    void remove_list(trx_t *trx);

    /** Check the trx list ordered by tid and min_active_tid was maintained
     * correctly.
     *
     * @retval	true	valid */
    bool check_min_active_tid() const;

    // Iterate all trx in list.
    template <typename F>
    bool iterate(F &&f) {
      for (auto trx : m_list) {
        if (std::forward<F>(f)(trx)) return true;
      }
      return false;
    }

    void lock() { mutex_enter(&m_mutex); }
    void unlock() { mutex_exit(&m_mutex); }

#if defined UNIV_DEBUG
    bool lock_own() const { return mutex_own(&m_mutex); }
#endif

   private:
    /** list length */
    ulint length() const { return UT_LIST_GET_LEN(m_list); }

   private:
    /** trx list order by tid.*/
    List m_list;

    /** min tid within trx list.*/
    std::atomic<trx_id_t> m_min_active_tid;

    /* mutex protect list.*/
    Mutex m_mutex;

    TID *tid_mgr;
  };

  using tid_rw_t = std::pair<trx_t *, tid_list_t *>;
  /**-----------------------------------------------------------------------*/
  /**  Tid list iterator */
  /**-----------------------------------------------------------------------*/
  class tid_list_iterator_t {
   public:
    tid_list_iterator_t(TID *tid)
        : m_index(0), m_shard(0), m_tid_list(nullptr), m_tid(tid) {
      m_tid_list = m_tid->get_list(m_shard);
      m_tid_list->lock();
    }

    virtual ~tid_list_iterator_t() {
      if (m_tid_list) {
        release_shard();
      }

      m_shard = 0;
      m_tid = nullptr;
    }

    /** Current position trx object if has
     *
     * @param[in/out]	tid rw struct
     *
     * @retval		true if has.*/
    bool current(tid_rw_t &tid_rw) { return reposition(tid_rw); }

    /** Iterate to next trx within the same tid list. */
    void next() { m_index++; }

    /** Rewind to first trx within current tid list. */
    void rewind() { m_index = 0; }

    /** Breakoff iteration, can't resume it again. */
    void interrupt() { release_shard(); }

   private:
    void release_shard() {
      ut_ad(m_tid_list);
      ut_ad(m_tid_list->lock_own());

      /** Clean current list. */
      m_tid_list->unlock();
      m_tid_list = nullptr;
      m_index = 0;
    }

    /** Switch to next tid list. return false if end. */
    bool next_shard() {
      /** Clean current list. */
      release_shard();

      m_shard++;
      if ((m_tid_list = m_tid->get_list(m_shard)) != nullptr) {
        m_tid_list->lock();
        return true;
      }
      return false;
    }

    /** Reposition trx object, switch to next shard if end until last shard.
     *
     * @param[in/out]	tid rw struct
     *
     * @retval		true if exist.*/
    bool reposition(tid_rw_t &tid_rw) {
      ulint i = 0;

    next_list:
      i = 0;
      /* Make the transaction at the ordinal value of m_index
      the current transaction. ie. reposition/restore */

      for (auto trx : *m_tid_list->get_list()) {
        if (i++ == m_index) {
          tid_rw = {trx, m_tid_list};
          return true;
        }
        check_trx_state(trx);
      }

      if (next_shard()) {
        goto next_list;
      }

      tid_rw = {nullptr, nullptr};
      return false;
    }

   private:
    /** Ordinal value of the transaction in the current transaction list */
    ulint m_index;
    /** Tid list shard. */
    size_t m_shard;
    /** Current tid list. */
    tid_list_t *m_tid_list;
    /** tid */
    TID *m_tid;
  };
  /**-----------------------------------------------------------------------*/

 public:
  TID();

  virtual ~TID();

  /** Assign the init value by reading from tablespace */
  void boot();

  /**Allocate a new transaction id number and all into
   * ordered list.
   *
   * @param[in]	trx
   *
   * @retval	new tid */
  template <typename F>
  trx_id_t new_tid(trx_t *trx, F &&f) {
    ut_ad(m_inited);
    ut_ad(!trx->in_rw_trx_list);
    ut_ad(trx->id == 0);
    ut_ad(trx->tid_shard_no == -1);
    ut_ad(trx_is_txn_rseg_assigned(trx));

    trx->tid_shard_no = map2shard(trx_get_txn_rseg(trx)->space_id);

    // Generate new tid and add into list.
    latch_and_execute_tid_list(trx->tid_shard_no, [&](tid_list_t *list) -> bool {
      trx->id = m_next_tid.fetch_add(1);
      list->add_list(trx);

      ut_ad(trx->id != 0);
      ut_d(trx->in_rw_trx_list = true);

      std::forward<F>(f)(trx);
      return false;
    });

    return trx->id;
  }

  /** Allocate a new tid. */
  trx_id_t new_tid() { return m_next_tid.fetch_add(1); }

  /** Resurrect recovered trx which is active/prepared/committed_in_memory. */
  void resurrect_trx(trx_t *trx);

  void erase(trx_t *trx);

  /**
   * Persist tid if next tid > persisted tid.
   *
   * @retval	true	if written */
  bool persist_tid();

  trx_id_t persisted_next_tid() const { return m_persisted_next_tid.load(); }

  void recover_tid_if_bigger(const meta_t &meta);

  bool validate_trx_list() {
    for (size_t i = 0; i < m_shards; i++) {
      latch_and_execute_tid_list(i, [](tid_list_t *list) -> bool {
        ut_a(list->check_min_active_tid());
        return false;
      });
    }
    return true;
  }

  /** Assign min active tid according to list element. */
  void start_active_tid() {
    for (size_t i = 0; i < m_shards; i++) {
      latch_and_execute_tid_list(i, [](tid_list_t *list) -> bool {
        list->start_active_tid();
        return false;
      });
    }

    /** Boot tid value. */
    m_boot_tid = m_next_tid.load();
  }

  trx_id_t boot_tid() const { return m_boot_tid; }

  trx_id_t load_next_tid() const { return m_next_tid.load(); }

  /** Load the min active tid from all lists. */
  trx_id_t load_min_active_tid() const {
    trx_id_t min_tid = TRX_ID_MAX;
    for (size_t i = 0; i < m_shards; i++) {
      min_tid = std::min(min_tid, m_lists[i].load_min_active_tid());
    }
    ut_a(min_tid != TRX_ID_MAX);
    return min_tid;
  }

  /* List getter */
  tid_list_t *get_list(size_t shard_no) {
    if (shard_no >= srv_gcs_tid_list_shards) return nullptr;
    return &m_lists[shard_no];
  }

  /** Peek min active tid within one shard through slot address. */
  trx_id_t peek_min_active_tid(const slot_addr_t &slot_addr) const {
    return m_lists[map2shard(slot_addr.space_id)].load_min_active_tid();
  }

  /**latch and iterate all trx from list. */
  template <typename F>
  bool latch_and_iterate_trx(F &&f) {
    bool res = false;
    for (size_t i = 0; i < m_shards; i++) {
      m_lists[i].lock();
      res = m_lists[i].iterate(std::forward<F>(f));
      m_lists[i].unlock();
      if (res) break;
    }
    return res;
  }

  /** latch and iterate all list. */
  template <typename F>
  bool latch_and_iterate_trx_list(F &&f) {
    bool res = false;
    for (size_t i = 0; i < m_shards; i++) {
      m_lists[i].lock();
      res = std::forward<F>(f)(m_lists[i].get_list());
      m_lists[i].unlock();
      if (res) break;
    }
    return res;
  }

 private:
  /** RW active tid lists were partitioned by txn slot space id. */
  int32_t map2shard(space_id_t txn_space_id) const {
    return txn_space_id % m_shards;
  }

  /**latch and execute list lamda. */
  template <typename F>
  bool latch_and_execute_tid_list(size_t shard_no, F &&f) {
    bool res;
    tid_list_t *list = &m_lists[shard_no];
    list->lock();
    res = std::forward<F>(f)(list);
    list->unlock();

    return res;
  }

 private:
  std::atomic<trx_id_t> m_next_tid;

  /** List of active and committed in memory read-write transactions, sorted
  on trx id, biggest first. Recovered transactions are always on this list. */
  ut::Cacheline_padded<tid_list_t> *m_lists;

  TidPersister m_persister;

  /** Persisted next tid value. */
  std::atomic<trx_id_t> m_persisted_next_tid;

  /** Serialize tid persist */
  ib_mutex_t m_persist_mutex;

  uint32_t m_shards;

  /** Boot tid value. */
  trx_id_t m_boot_tid;

  bool m_inited;
};

}  // namespace lizard

using tid_meta_t = lizard::TID::meta_t;
using Tid_list_t = lizard::TID::tid_list_t;
using Tid_rw_t = lizard::TID::tid_rw_t;
using Tid_list_iterator = lizard::TID::tid_list_iterator_t;

using Trx_rw_list_t = lizard::TID::tid_list_t::List;

#endif
