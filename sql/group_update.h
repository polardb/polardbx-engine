/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  group_update.h,v 1.0 01/05/2016 11:19:20 AM
 *       yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file group_update.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 01/05/2016 11:19:20 AM
 * @version 1.0
 * @brief group update for inventory
 *
 **/

#ifndef group_update_INC
#define group_update_INC

#include <pthread.h>
#include <string>

#include "map_helpers.h"
#include "sql/mysqld.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"  // THD
#include "sql/table.h"

extern ulonglong hotspot_update_max_wait_time;
extern bool hotspot_lock_type;
extern bool opt_hotspot;
extern bool hotspot_fast_insert_dup;
extern bool hotspot_for_autocommit;

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_gu_lock_status;
static PSI_mutex_key key_gu_lock;

static PSI_mutex_info all_gu_mutexes[] = {
    {&key_gu_lock_status, "GroupUpdate::group::lock_status", 0, 0, PSI_DOCUMENT_ME},
    {&key_gu_lock, "GroupUpdate::lock_", 0, 0, PSI_DOCUMENT_ME}};

static PSI_rwlock_key key_mgr_hash_lock;
static PSI_rwlock_key key_pool_lock;

static PSI_rwlock_info all_gu_rwlocks[] = {
    {&key_mgr_hash_lock, "GroupUpdateMgr::hash_lock_", 0, 0, PSI_DOCUMENT_ME},
    {&key_pool_lock, "GroupUpdatePool::lock_", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_key key_cond_leader;
static PSI_cond_key key_cond_follower;

static PSI_cond_info all_gu_conds[] = {
    {&key_cond_leader, "GroupUpdate::group::cond_leader", 0, 0, PSI_DOCUMENT_ME},
    {&key_cond_follower, "GroupUpdate::group::cond_follower", 0, 0, PSI_DOCUMENT_ME}};

/**
  Initialise all the instrumentation points
  used by this subsystem.
*/
static void init_gu_psi_keys(void) {
  int count;

  count = array_elements(all_gu_mutexes);
  mysql_mutex_register("sql", all_gu_mutexes, count);

  count = array_elements(all_gu_rwlocks);
  mysql_rwlock_register("sql", all_gu_rwlocks, count);

  count = array_elements(all_gu_conds);
  mysql_cond_register("sql", all_gu_conds, count);
}
#endif /* HAVE_PSI_INTERFACE */

class GroupUpdateMgr;
class GroupUpdatePool;

/**
 * @class GroupUpdate
 *
 * @brief group update for inventory
 *
 **/
class GroupUpdate {
 public:
  enum gu_status {
    GS_IDLE = 0,
    GS_LOCK,
    GS_COLLECT,
    GS_UPDATE,
    GS_PREPARE,
    GS_COMMIT,
    GS_LD_DONE,
    /* Above is the main status. */
    GS_ERROR,
    GS_ALL_CLEAR,
    /* Above is the error status. */
    GS_LEADER = 0x100,
    GS_FOLLOWER = 0x200,
    /* Above is the role status. */
    GS_NO_UPDATE = 0x400
    /* Above will block another group enter GS_UPDATE. */
  };

  GroupUpdate(GroupUpdateMgr *mgr)
      : key(nullptr),
        key_buffer_length(0),
        key_length(0),
        record_(nullptr),
        rec_buff_length_(0),
        max_rec_buff_length_(0),
        cur_idx_(0),
        version_(0),
        reuse_version_(0),
        first_(nullptr),
        last_thd_(nullptr),
        last_(&first_),
        mgr_(mgr),
        commit_cnt_(0),
        last_used_(0),
        next_free(nullptr),
        in_free_list(false) {
    mysql_mutex_init(key_gu_lock, &lock_, MY_MUTEX_INIT_SLOW);
  }

  virtual ~GroupUpdate() {
    mysql_mutex_destroy(&lock_);
    if (key) free(key);
    if (record_) free(record_);
  }

  /* for debug*/
  void my_assert(bool expect) {
    if (!expect) {
      ++group_update_assert_count;
      assert(0);
    }
  }

  /* This function clean the variables that do not used in commit stage. */
  void reset_list() {
    first_ = nullptr;
    last_thd_ = nullptr;
    last_ = &first_;
  }

  int lock() { return mysql_mutex_lock(&lock_); }
  int trylock() { return mysql_mutex_trylock(&lock_); }
  int unlock() { return mysql_mutex_unlock(&lock_); }
  void set_status(enum gu_status status_arg) {
    mysql_mutex_lock(&go_[cur_idx_].lock_status);
    go_[cur_idx_].status = status_arg;
    mysql_mutex_unlock(&go_[cur_idx_].lock_status);
  }
  void store_row(uchar *src) { memcpy(record_, src, rec_buff_length_); }
  void restore_row(uchar *dest) { memcpy(dest, record_, rec_buff_length_); }
  void alloc_record_buff(uint rec_buff_length_arg) {
    if (rec_buff_length_ == rec_buff_length_arg) return;

    if (max_rec_buff_length_ < rec_buff_length_arg) {
      if (record_) free(record_);
      record_ = (uchar *)malloc(rec_buff_length_arg);
      max_rec_buff_length_ = rec_buff_length_arg;
    }
    rec_buff_length_ = rec_buff_length_arg;
  }

  void dump_key(const uchar *key_arg, uint key_length_arg) {
    /*
     * dump_key means that we reuse the gu as a new key,
     * so we inc the reuse_version_
     */
    reuse_version_++;

    if (key_buffer_length < key_length_arg) {
      if (key) free(key);
      key = (uchar *)malloc(key_length_arg);
      key_buffer_length = key_length = key_length_arg;
    }
    my_assert(key_buffer_length >= key_length_arg);
    memcpy(key, key_arg, key_length_arg);
    key_length = key_length_arg;
  }

  bool has_leader() {
    struct group *go = &go_[cur_idx_];
    bool ret = false;

    mysql_mutex_lock(&go->lock_status);
    if (go->n_user > 0 || go->update_status == GS_NO_UPDATE) ret = true;
    mysql_mutex_unlock(&go->lock_status);

    return ret;
  }

  bool in_use() {
    /* Caller should hold the lock*/
    if (go_[0].n_user > 0 || go_[1].n_user > 0) return true;

    /* Have a try. no need lock. TODO */
    if (go_[cur_idx_].status != GS_IDLE || go_[1 - cur_idx_].status != GS_IDLE)
      return true;

    return false;
  }

  bool in_use_lock() {
    if (in_use()) return true;
    mysql_mutex_lock(&go_[0].lock_status);
    mysql_mutex_lock(&go_[1].lock_status);

    if (go_[cur_idx_].status != GS_IDLE ||
        go_[1 - cur_idx_].status != GS_IDLE ||
        go_[cur_idx_].update_status != GS_IDLE ||
        go_[1 - cur_idx_].update_status != GS_IDLE) {
      mysql_mutex_unlock(&go_[0].lock_status);
      mysql_mutex_unlock(&go_[1].lock_status);
      return true;
    }
    my_assert(go_[0].n_user == 0 && go_[1].n_user == 0);

    mysql_mutex_unlock(&go_[0].lock_status);
    mysql_mutex_unlock(&go_[1].lock_status);
    return false;
  }

  void set_leader(THD *thd, uint idx) {
    /* Here we can instead idx with cur_idx_, because only on group can
       stay in GS_COLLECT */
    my_assert(go_[idx].n_user == 0);
    my_assert(thd->gu_ctx.idx == cur_idx_);
    my_assert(first_ == nullptr && last_thd_ == nullptr);
    thd->next_to_commit = nullptr;
    *last_ = last_thd_ = thd;
    last_ = &(thd->next_to_commit);
    mysql_mutex_lock(&go_[cur_idx_].lock_status);
    go_[cur_idx_].n_user = 1;
    go_[cur_idx_].already_enter_commit = false;
    mysql_mutex_unlock(&go_[cur_idx_].lock_status);
  }

  void push_follower(THD *thd) {
    DEBUG_SYNC_C("before_push_follower");

    my_assert(first_ && last_thd_);
    my_assert(thd->gu_ctx.idx == cur_idx_);
    // for debug
    thd->gu_ctx.set_leader(first_);
    thd->next_to_commit = nullptr;
    mysql_mutex_lock(&go_[cur_idx_].lock_status);
    if (go_[cur_idx_].n_error_done) {
      /* previous thd already failed */
      ++go_[cur_idx_].n_user;
      mysql_mutex_unlock(&go_[cur_idx_].lock_status);
      return;
    }
    *last_ = last_thd_ = thd;
    last_ = &(thd->next_to_commit);
    ++go_[cur_idx_].n_user;
    mysql_mutex_unlock(&go_[cur_idx_].lock_status);
  }

  THD *fetch_and_empty() {
    THD *result = first_;
    first_ = nullptr;
    last_thd_ = nullptr;
    last_ = &first_;
    return result;
  }

  uchar *get_record() { return record_; }
  /* Protected by lock_. */
  uint get_idx() { return cur_idx_; }
  void change_idx() {
    cur_idx_ = 1 - cur_idx_;
    __sync_fetch_and_add(&version_, 1);
  }

  void set_mgr(GroupUpdateMgr *mgr) { mgr_ = mgr; }
  ulonglong get_version() { return version_; }
  ulonglong get_reuse_version() { return reuse_version_; }

  bool add_user(THD *thd, TABLE *table);
  bool enter_update();
  /* This function is used for the other group error and hotspot_lock_type. */
  void leader_update_error();
  void enter_prepare_and_unlock(THD *thd);
  bool follower_wait_all_done(THD *thd);
  bool leader_enter_commit(THD *thd);
  GroupUpdate *leader_done(THD *thd);
  /* Used for leader don't find row, or error in fill_record_. */
  GroupUpdate *leader_ignore_done(THD *thd);
  GroupUpdate *follower_done(THD *thd);
  GroupUpdate *error_done(THD *thd);

 public:
  uchar *key;
  uint key_buffer_length;
  uint key_length;

 protected:
  struct group {
    enum gu_status status, update_status, error_status;
    ulonglong role_status;
    volatile uint64 n_user, n_prepare, n_follower_waitting, n_follower_commit,
        n_error_done;
    bool already_enter_commit;
    mysql_mutex_t lock_status;
    mysql_cond_t cond_leader;
    mysql_cond_t cond_follower;
    group()
        : status(GS_IDLE),
          update_status(GS_IDLE),
          error_status(GS_IDLE),
          role_status(GS_IDLE),
          n_user(0),
          n_prepare(0),
          n_follower_waitting(0),
          n_follower_commit(0),
          n_error_done(0),
          already_enter_commit(false) {
      mysql_mutex_init(key_gu_lock_status, &lock_status, MY_MUTEX_INIT_SLOW);
      mysql_cond_init(key_cond_leader, &cond_leader);
      mysql_cond_init(key_cond_follower, &cond_follower);
    }

    ~group() {
      mysql_mutex_destroy(&lock_status);
      mysql_cond_destroy(&cond_leader);
      mysql_cond_destroy(&cond_follower);
    }
  };

  struct group go_[2];
  uchar *record_;
  ulonglong rec_buff_length_;
  ulonglong max_rec_buff_length_;
  uint cur_idx_;
  ulonglong version_;
  std::atomic<ulonglong> reuse_version_;

 public:
  mysql_mutex_t lock_;
  THD *first_;
  THD *last_thd_;
  THD **last_;
  GroupUpdateMgr *mgr_;

  /* Not used now, may used to delay reuse later. TODO*/
  uint commit_cnt_;
  ulonglong last_used_;

  GroupUpdate *next_free;
  /* GroupUpdate may free more than one times. */
  bool in_free_list;

 private:
  GroupUpdate(const GroupUpdate &other);
  const GroupUpdate &operator=(const GroupUpdate &other);
  friend class GroupUpdateMgr;
  friend class GroupUpdatePool;
};

/**
 * @class GroupUpdateMgr
 *
 * @brief
 *
 **/
class GroupUpdateMgr {
 public:
  /* for debug*/
  void my_assert(bool expect) {
    if (!expect) {
      ++group_update_assert_count;
      assert(0);
    }
  }

  GroupUpdateMgr() {
    mysql_rwlock_init(key_mgr_hash_lock, &hash_lock_);
    hash_ = new collation_unordered_map<std::string, GroupUpdate *>(
        system_charset_info, PSI_INSTRUMENT_ME);
  }

  virtual ~GroupUpdateMgr() {
    mysql_rwlock_destroy(&hash_lock_);
    delete hash_;
  }

  GroupUpdate *get_gu(uchar *key, uint key_length) {
    GroupUpdate *gu = nullptr;
    mysql_rwlock_rdlock(&hash_lock_);
    const auto it = hash_->find(std::string((char *)key, key_length));
    if (it != hash_->end()) {
      gu = it->second;
    }
    mysql_rwlock_unlock(&hash_lock_);

    return gu;
  }

  bool try_delete_from_hash(GroupUpdate *gu, ulonglong reuse_version) {
    mysql_rwlock_wrlock(&hash_lock_);
    if (gu->get_reuse_version() == reuse_version && gu->trylock() == 0) {
      if (gu->in_use_lock() || gu->in_free_list ||
          gu->get_reuse_version() > reuse_version) {
        mysql_rwlock_unlock(&hash_lock_);
        gu->unlock();
        return false;
      }
    } else {
      mysql_rwlock_unlock(&hash_lock_);
      return false;
    }
    hash_->erase(std::string((char *)gu->key, gu->key_length));
    mysql_rwlock_unlock(&hash_lock_);

    gu->in_free_list = true;
    gu->unlock();
    return true;
  }

  GroupUpdate *get_gu_and_lock(uchar *key, uint key_length);
  ulong get_gu_count() {
    ulong count = 0;
    mysql_rwlock_rdlock(&hash_lock_);
    count = hash_->size();
    mysql_rwlock_unlock(&hash_lock_);
    return count;
  }

 private:
  GroupUpdateMgr(const GroupUpdateMgr &other);
  const GroupUpdateMgr &operator=(const GroupUpdateMgr &other);

  collation_unordered_map<std::string, GroupUpdate *> *hash_;
  mysql_rwlock_t hash_lock_;
};

/**
 * @class GroupUpdatePool
 *
 * @brief
 *
 **/
class GroupUpdatePool {
 public:
  /* for debug*/
  void my_assert(bool expect) {
    if (!expect) {
      ++group_update_assert_count;
      assert(0);
    }
  }

  static GroupUpdatePool *get_instance() { return instance_; }

  /*
    Init in the main fist, using DCLP later. TODO:
  */
  static void init_instance() {
    init_gu_psi_keys();
    instance_ = new GroupUpdatePool();
  }

  bool try_release_gu(GroupUpdate *gu, ulonglong reuse_version) {
    bool ret;
    ret = gu->mgr_->try_delete_from_hash(gu, reuse_version);
    if (ret == true) {
      free_gu(gu);
    }

    return ret;
  }

  GroupUpdateMgr *get_mgr(TABLE_SHARE *s, bool force = true) {
    GroupUpdateMgr *mgr = nullptr;

    mysql_rwlock_rdlock(&lock_);
    mgr = s->mgr;
    mysql_rwlock_unlock(&lock_);

    if (mgr || !force) return mgr;

    mysql_rwlock_wrlock(&lock_);
    if (s->mgr) {
      mgr = s->mgr;
    } else {
      mgr = new GroupUpdateMgr();
      s->mgr = mgr;
    }
    mysql_rwlock_unlock(&lock_);

    return mgr;
  }

  ulong free_mgr(TABLE_SHARE *s) {
    ulong gu_count = 0;
    mysql_rwlock_wrlock(&lock_);
    if (s->mgr) {
      gu_count = s->mgr->get_gu_count();
      if (0 == gu_count) {
        delete s->mgr;
        s->mgr = nullptr;
      }
    }
    mysql_rwlock_unlock(&lock_);

    return gu_count;
  }

  GroupUpdate *get_free_gu(GroupUpdateMgr *mgr);

 protected:
  ulonglong get_list_count(GroupUpdate *head) {
    ulonglong i = 0;
    while (head && i < 1000) {
      head = head->next_free;
      ++i;
    }
    return i;
  }

  void free_gu(GroupUpdate *gu) {
    pthread_spin_lock(&free_list_lock_);
    my_assert(get_list_count(free_list_) == group_update_free_count);

    gu->next_free = free_list_;
    free_list_ = gu;
    ++group_update_free_count;

    my_assert(get_list_count(free_list_) == group_update_free_count);
    pthread_spin_unlock(&free_list_lock_);
  }

 private:
  GroupUpdatePool(const GroupUpdatePool &other);
  const GroupUpdatePool &operator=(const GroupUpdatePool &other);

  GroupUpdatePool() {
    free_list_ = nullptr;
    group_update_free_count = 0;
    pthread_spin_init(&free_list_lock_, PTHREAD_PROCESS_PRIVATE);
    mysql_rwlock_init(key_pool_lock, &lock_);
  }

  virtual ~GroupUpdatePool() {
    pthread_spin_destroy(&free_list_lock_);
    mysql_rwlock_destroy(&lock_);
  }

  /* Singleton pattern's instance */
  static GroupUpdatePool *instance_;
  GroupUpdate *free_list_;
  pthread_spinlock_t free_list_lock_;
  mysql_rwlock_t lock_;
};

extern GroupUpdate *gu_try_start_hotspot(THD *thd, const AccessPath *range_scan,
                                         TABLE *table);

extern void gu_handle_fail_or_error(THD *thd, GroupUpdate *gu,
                                    const ha_rows found_rows,
                                    bool view_check_fail, int error,
                                    bool gu_locked);

extern void gu_finish_commit(THD *thd);

#endif /* group_update_INC */
