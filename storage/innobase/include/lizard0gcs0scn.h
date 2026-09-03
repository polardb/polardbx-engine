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

/** @file include/lizard0gcs0scn.h
 Lizard scn number implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gcs0scn_h
#define lizard0gcs0scn_h

#include <utility>

#include "mtr0types.h"
#include "trx0trx.h"
#include "ut0mutex.h"

#include "lizard0cmmt0types.h"

struct mtr_t;

#ifdef UNIV_PFS_MUTEX
/* GCS scn list mutex PFS key */
extern mysql_pfs_key_t scn_list_mutex_key;
/* GCS scn persist mutex PFS key */
extern mysql_pfs_key_t scn_persist_mutex_key;

#endif

namespace lizard {

/** Whether to print gcs process info. */
extern bool srv_gcs_print_process_debug;

extern uint32_t srv_gcs_scn_list_shards;

extern bool dbug_gcs_enable_traverse_scn_list;

/* The structure of scn number generation */
class SCN {
 public:
  /** MAX reserved scn NUMBER  */
  static constexpr scn_t SCN_INSTALL_VALUE = 1024;

 public:
  typedef struct meta_t {
    scn_t scn{0};
    /** Set scn if bigger */
    void set_scn_if_bigger(const meta_t &meta) {
      ut_a(meta.valid());

      if (meta.scn > scn) {
        scn = meta.scn;
      }
    }

    bool valid() const { return scn > 0 && scn <= SCN_MAX; }
  } meta_t;

  /** SCN persister */
  class ScnPersister {
    /** The number gap of persist scn number into system tablespace */
    constexpr static size_t SCN_NUMBER_MAGIN = 8192;

   public:
    /** Write scn value into tablespace.

        @param[in]	metadata   scn metadata
     */
    void write(const meta_t *metadata);

    /** Write scn value redo log.

        @param[in]	metadata   scn metadata
        @param[in]	mini transacton context

     */
    void write_log(const meta_t *metadata, mtr_t *mtr);

    /**
       Writes a log record about scn rise.

       @param[in]		type	redo log record type
       @param[in,out]	log_ptr		current end of mini-transaction log
       @param[in/out]	mtr	mini-transaction

       @reval	end of mtr
     */
    byte *mlog_write_initial_scn_log_record(mlog_id_t type, byte *log_ptr,
                                            mtr_t *mtr);
    /** Read scn value from tablespace and increase GCS_SCN_NUMBER_MAGIN
     *  to promise unique.

        @param[in/out]	metadata   scn metadata

     */
    void read(meta_t *metadata);
    /**
       Parses an initial log record written by
       mlog_write_initial_scn_log_record.

       @param[in]	ptr		buffer
       @param[in]	end_ptr		buffer end
       @param[out]	type		log record type, should be
                                    MLOG_scn_METADATA
       @param[out]	scn

       @return parsed record end, NULL if not a complete record
     */
    static byte *mlog_parse_initial_scn_log_record(const byte *ptr,
                                                   const byte *end_ptr,
                                                   mlog_id_t *type,
                                                   meta_t *meta);
  };

  /** list of trx;*/
  class scn_list_t {
   public:
    scn_list_t() {}

    virtual ~scn_list_t() {
      ut_ad(UT_LIST_GET_LEN(m_list) == 0);
      ut_ad(scn_mgr == nullptr);
    }

    /* init length */
    void init(SCN *scn_arg) {
      UT_LIST_INIT(m_list);
      m_min_active_scn.store(0);
      mutex_create(LATCH_ID_SCN_LIST, &m_mutex);
      scn_mgr = scn_arg;
    }

    void destroy() {
      ut_ad(length() == 0);
      m_min_active_scn.store(0);
      mutex_free(&m_mutex);
      scn_mgr = nullptr;
    }

    /** Init min active scn after gcs boot and recovery. */
    void start_active_scn(scn_t scn) {
      ut_ad(scn != SCN_NULL);
      ut_ad(length() == 0);
      m_min_active_scn.store(scn);
    }

    // Getter
    scn_t load_min_active_scn() const { return m_min_active_scn.load(); }

    // Get min active scn by traversing list with lock.
    scn_t traverse_min_active_scn() const;

    /** Add into the tailer of list. */
    void add_list(trx_t *trx) {
      ut_ad(check_min_active_scn());
      ut_ad(lock_own());
      UT_LIST_ADD_FIRST(m_list, trx);
    }

    /** Remove trx from list and modify min active scn if necessary. */
    void remove_list(trx_t *trx);

    void lock() { mutex_enter(&m_mutex); }
    void unlock() { mutex_exit(&m_mutex); }

   private:
    /** list length */
    ulint length() const { return UT_LIST_GET_LEN(m_list); }

#if defined UNIV_DEBUG
    bool lock_own() const { return mutex_own(&m_mutex); }
#endif

    bool check_min_active_scn() const;

   private:
    /** trx list order by scn.*/
    PURE_LIST_BASE_NODE_T(trx_t, scn_list) m_list;

    /** min scn within trx list.*/
    std::atomic<scn_t> m_min_active_scn;

    /* mutex protect list.*/
    ib_mutex_t m_mutex;

    SCN *scn_mgr;
  };

 public:
  SCN();
  virtual ~SCN();

  /** Assign the init value by reading from tablespace */
  void boot();
  /** Generate a new scn number for transaction commit.
   *
   * @param[in/out]	trx	rw transaction.
   * @param[in/out]	mtr	commit mini transaction.
   *
   * @retval	scn */
  scn_t new_scn(trx_t *trx, mtr_t *mtr);

  /** Generate a unique scn number lonely */
  scn_t new_scn();

  void start_active_scn() {
    scn_t scn = load_next_scn();
    for (size_t i = 0; i < m_shards; i++) {
      latch_and_execute_list(i, [&scn](scn_list_t *list) -> bool {
        list->start_active_scn(scn);
        return false;
      });
    }
  }

  // load min active scn through traversing scn list with lock.
  scn_t traverse_min_active_scn() {
    scn_t min_scn = SCN_MAX;
    for (size_t i = 0; i < m_shards; i++) {
      latch_and_execute_list(i, [&min_scn](scn_list_t *list) -> bool {
        min_scn = std::min(min_scn, list->traverse_min_active_scn());
        return false;
      });
    }
    ut_a(min_scn != SCN_MAX);
    return min_scn;
  }

  // Load min active scn directly which is always maintained.
  scn_t load_min_active_scn() {
    scn_t min_scn = SCN_MAX;
    for (size_t i = 0; i < m_shards; i++) {
      min_scn = std::min(min_scn, m_lists[i].load_min_active_scn());
    }

    ut_a(min_scn != SCN_MAX);
    return min_scn;
  }

  /** Judge trx is still active by txn_desc.cmmt.scn.
   *  Attention: trx must be referenced or lock by trx_sys->shard.
   *
   *  @param[in]		trx
   *
   *  @revtal	true	mtr didn't commit
   * */
  bool is_slot_active(const trx_t *trx);

  /** Cleanup trx after commit in memory. */
  void erase(trx_t *trx);
  /**
     Push up current scn if bigger when recovery.

     @param[in]	scn
  */
  void recover_scn_if_bigger(const meta_t &meta);

  /**
   * Persist scn if current scn > persisted scn.
   *
   * @retval	true	if written */
  bool persist_scn();

  scn_t persisted_scn() const { return m_persisted_scn.load(); }
  scn_t load_scn() const { return m_scn.load(); }

 private:
  /** Disable the copy and assign function */
  SCN(const SCN &) = delete;
  SCN(const SCN &&) = delete;
  SCN &operator=(const SCN &) = delete;

  /**latch and execute lamda. */
  template <typename F>
  bool latch_and_execute_list(size_t shard_no, F &&f) {
    bool res;
    scn_list_t *list = &m_lists[shard_no];
    list->lock();
    res = std::forward<F>(f)(list);
    list->unlock();

    return res;
  }

  /** scn that will may be used by next transaction.*/
  scn_t load_next_scn() const { return m_scn.load() + 1; }

  /** scn lists were partitioned by trx->id. */
  int32_t map2shard(const trx_t *trx) const {
    ut_ad(trx->id > 0);
    return trx->id % m_shards;
  }

 private:
  std::atomic<scn_t> m_scn;

  ut::Cacheline_padded<scn_list_t> *m_lists;

  ScnPersister m_persister;

  /** Persisted scn value. */
  std::atomic<scn_t> m_persisted_scn;

  /** Serialize scn persist */
  ib_mutex_t m_persist_mutex;

  uint32_t m_shards;

  bool m_inited;
};

}  // namespace lizard

using scn_meta_t = lizard::SCN::meta_t;

#endif /* lizard0gcs0scn_h define */
