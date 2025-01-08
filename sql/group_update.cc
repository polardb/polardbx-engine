/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  group_update.cc,v 1.0 01/05/2016 11:15:32 AM
 *       yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file group_update.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 01/05/2016 11:15:32 AM
 * @version 1.0
 * @brief group update for inventory
 *
 **/

#include "sql/range_optimizer/range_optimizer.h"
#include "sql/range_optimizer/path_helpers.h"
#include "sql/inventory/inventory_hint.h"
#include "sql/sql_lex.h"
#include "group_update.h"
#include "debug_sync.h"

#include "include/mysql/components/services/log_builtins.h"

ulonglong hotspot_update_max_wait_time = 100;
bool hotspot_lock_type = false;
bool opt_hotspot = false;
bool hotspot_fast_insert_dup = false;
bool hotspot_for_autocommit = false;

/*
 * Impl of GroupUpdate.
 * return true for success, false for fail
 */
bool GroupUpdate::add_user(THD *thd, TABLE *table) {
  struct group *go = &go_[cur_idx_];
  mysql_mutex_lock(&go->lock_status);
  if (go->n_user == 0) {
    /* GS_ERROR means that some follower may not clean up in the last useage.
       for defence. */
    while (go->error_status != GS_IDLE)
      mysql_cond_wait(&go->cond_leader, &go->lock_status);  // no cover line.
    my_assert(go->status == GS_IDLE);
    go->status = GS_LOCK;
    go->n_error_done = go->n_prepare = go->n_follower_waitting = 0;

    go->role_status = GS_IDLE;
    mysql_mutex_unlock(&go->lock_status);

    alloc_record_buff(table->s->rec_buff_length);
    thd->gu_ctx.set_ctx(this, true, cur_idx_, version_, get_reuse_version());
    return true;
  } else {
    /* GS_ERROR means that error happens in this group, just return fail */
    /* TODO: better solution: wait until previous group leader change_idx and
     * error_done, and join the next group */
    if (go->error_status != GS_IDLE) {
      mysql_mutex_unlock(&go->lock_status);
      return false;
    }
    mysql_mutex_unlock(&go->lock_status);
    restore_row(table->record[0]);
    thd->gu_ctx.set_ctx(this, false, cur_idx_, version_, get_reuse_version());
    return true;
  }
}

bool GroupUpdate::enter_update() {
  DEBUG_SYNC_C("before_leader_enter_update");

  struct timespec abstime;
  int err = 0;
  struct group *go = &go_[cur_idx_];

  mysql_mutex_lock(&go->lock_status);
  my_assert(go->status == GS_COLLECT);
  do {
    set_timespec_nsec(&abstime, hotspot_update_max_wait_time * 1000);
    mysql_cond_timedwait(&go->cond_leader, &go->lock_status, &abstime);
  } while (go->update_status == GS_NO_UPDATE);

  err = (go->error_status != GS_IDLE);
  if (!err) go->status = GS_UPDATE;

  mysql_mutex_unlock(&go->lock_status);

  return err;
}

void GroupUpdate::leader_update_error() {
  struct group *go = &go_[cur_idx_];

  mysql_mutex_lock(&go->lock_status);
  my_assert((go->status == GS_COLLECT && go->error_status != GS_IDLE) ||
            (go->status == GS_UPDATE && go->error_status == GS_IDLE));

  if (go->n_user > 1) mysql_cond_broadcast(&go->cond_follower);
  mysql_mutex_unlock(&go->lock_status);

  /* signal the other group not to update. */
  go = &go_[1 - cur_idx_];
  mysql_mutex_lock(&go->lock_status);
  go->update_status = GS_NO_UPDATE;

  my_assert(go->status == GS_IDLE);
  my_assert(go->n_user == 0);
  mysql_mutex_unlock(&go->lock_status);

  reset_list();
  change_idx();

  unlock();
}

void GroupUpdate::enter_prepare_and_unlock(THD *thd) {
  struct group *go = &go_[cur_idx_];

  mysql_mutex_lock(&go->lock_status);
  my_assert(go->status == GS_UPDATE);
  go->status = GS_PREPARE;
  go->n_prepare = go->n_user;
  thd->gu_ctx.set_user(go->n_prepare);
  mysql_mutex_unlock(&go->lock_status);

  /* signal the other group not to update. */
  go = &go_[1 - cur_idx_];
  mysql_mutex_lock(&go->lock_status);
  go->update_status = GS_NO_UPDATE;

  my_assert(go->status == GS_IDLE);
  my_assert(go->n_user == 0);
  mysql_mutex_unlock(&go->lock_status);

  reset_list();
  change_idx();

  unlock();
  /*
     Now the new group is ready for leader enter
     But the leader will be blocked by the row lock,
     if the hotspot row lock is not enable.
     */
}

bool GroupUpdate::follower_wait_all_done(THD *thd) {
  uint idx = thd->gu_ctx.idx;
  struct group *go = &go_[idx];
  bool err = false;

  mysql_mutex_lock(&go->lock_status);
  my_assert(go->n_user > 1 || go->n_prepare > 1);
  /* my_assert(thd->get_transaction()->m_flags.pending); */
  my_assert(thd->tx_commit_pending);

  ++go->n_follower_waitting;
  /* with GS_LEADER means that collect is end, the n_prepare has been set now.
   */
  if (go->role_status & GS_LEADER &&
      go->n_follower_waitting == go->n_prepare - 1) {
    my_assert(go->status == GS_PREPARE);
    mysql_cond_broadcast(&go->cond_leader);
  }

  thd->enter_cond(&go->cond_follower, &go->lock_status,
                  &stage_hotspot_wait_for_commit, nullptr, __FUNCTION__,
                  __FILE__, __LINE__);
  /* while (thd->get_transaction()->m_flags.pending && go->status != GS_LD_DONE && */
  while (/* thd->tx_commit_pending && */ go->status != GS_LD_DONE &&
         go->error_status == GS_IDLE &&
         (!thd->killed || go->already_enter_commit || go->status == GS_COMMIT))
    mysql_cond_wait(&go->cond_follower, &go->lock_status);

  DEBUG_SYNC(thd, "follower_wait_all_done_after_wait");

  /* If leader is in GS_COMMIT state, followers don't respond to kill here */
  err = (go->error_status != GS_IDLE ||
         (thd->killed && !go->already_enter_commit));

  /*
    Here we should reduce the n_follower_waitting.
    This also used to block leader enter commit, when follower is killed.
  */
  --go->n_follower_waitting;

  mysql_mutex_unlock(&go->lock_status);
  thd->exit_cond(&stage_query_end, __FUNCTION__, __FILE__, __LINE__);

  if (err) {
    thd->get_stmt_da()->set_overwrite_status(true);
    my_error(ER_ERROR_DURING_COMMIT, MYF(0), 113, "");
    thd->get_stmt_da()->set_overwrite_status(false);

    mysql_mutex_lock(&thd->LOCK_tx_commit_pending_mutex);
    thd->tx_commit_pending = false;
    mysql_mutex_unlock(&thd->LOCK_tx_commit_pending_mutex);
  } else {
    mysql_mutex_lock(&thd->LOCK_tx_commit_pending_mutex);
    while (thd->tx_commit_pending) {
      LogErr(ERROR_LEVEL, ER_LIZARD,
             "Group Update: Waiting for tx_commit_pending reseting.");
      mysql_cond_wait(&thd->COND_tx_commit_pending_cond_var,
                      &thd->LOCK_tx_commit_pending_mutex);
    }
    mysql_mutex_unlock(&thd->LOCK_tx_commit_pending_mutex);
  }

  return err;
}

bool GroupUpdate::leader_enter_commit(THD *thd) {
  struct group *go = &go_[thd->gu_ctx.idx];
  bool err = false;

  mysql_mutex_lock(&go->lock_status);
  my_assert(go->status == GS_PREPARE);
  go->role_status |= GS_LEADER;

  thd->enter_cond(&go->cond_leader, &go->lock_status,
                  &stage_hotspot_wait_for_commit, nullptr, __FUNCTION__,
                  __FILE__, __LINE__);
  while (
      (!(go->n_follower_waitting == go->n_prepare - 1 || go->n_prepare == 1)) &&
      go->error_status == GS_IDLE && !thd->killed)
    mysql_cond_wait(&go->cond_leader, &go->lock_status);

  err = (go->error_status != GS_IDLE || thd->killed);
  my_assert(go->n_follower_waitting == go->n_prepare - 1 ||
            go->n_prepare == 1 || err);

  if (!err) {
    // kill may wakeup follower earlier, so both leader and followers may wait
    // next batch leader
    go->n_follower_commit = go->n_follower_waitting + 1;
    go->status = GS_COMMIT;
    go->role_status = GS_IDLE;
    go->already_enter_commit = true;
  }

  mysql_mutex_unlock(&go->lock_status);
  thd->exit_cond(&stage_query_end, __FUNCTION__, __FILE__, __LINE__);

  if (err) {
    thd->get_stmt_da()->set_overwrite_status(true);
    my_error(ER_ERROR_DURING_COMMIT, MYF(0), 112, "");
    thd->get_stmt_da()->set_overwrite_status(false);
  }

  return err;
}

GroupUpdate *GroupUpdate::leader_ignore_done(THD *thd) {
  struct group *go = &go_[thd->gu_ctx.idx];
  GroupUpdate *can_reuse = nullptr;

  mysql_mutex_lock(&go->lock_status);
  my_assert(go->n_user == 0);
  my_assert(go->status == GS_LOCK);

  go->status = GS_IDLE;
  go->error_status = GS_IDLE;
  mysql_mutex_unlock(&go->lock_status);

  can_reuse = thd->gu_ctx.get_gu();

  thd->gu_ctx.reset();
  thd->gu_ctx.set_need_release_gu(true);

  return can_reuse;
}

GroupUpdate *GroupUpdate::leader_done(THD *thd) {
  struct group *go = &go_[thd->gu_ctx.idx];
  GroupUpdate *can_reuse = nullptr;

  mysql_mutex_lock(&go->lock_status);
  go->n_user = 0;

  my_assert(go->status == GS_COMMIT);
  if (go->n_prepare > 1) {
    go->status = GS_LD_DONE;
    mysql_cond_broadcast(&go->cond_follower);
    if ((--go->n_follower_commit) == 0) {
      /* this is the last thd of group, same as follower*/
      go->status = GS_IDLE;
      go->role_status = GS_IDLE;
      mysql_mutex_unlock(&go->lock_status);

      __sync_fetch_and_add(&commit_cnt_, go->n_prepare);

      /* signal the other group to update. */
      go = &go_[1 - thd->gu_ctx.idx];
      my_assert(cur_idx_ == 1 - thd->gu_ctx.idx);
      mysql_mutex_lock(&go->lock_status);
      /*
        Here we let another group enter GS_UPDATE, may let this group add new
        user, because another group will change idx and unlock with enter
        GS_PREPARE.
      */
      go->update_status = GS_IDLE;
      if (go->status == GS_COLLECT) mysql_cond_broadcast(&go->cond_leader);
      mysql_mutex_unlock(&go->lock_status);

      can_reuse = thd->gu_ctx.get_gu();
    } else
      mysql_mutex_unlock(&go->lock_status);
  } else {
    my_assert(go->n_prepare == 1);
    go->status = GS_IDLE;
    go->role_status = GS_IDLE;
    mysql_mutex_unlock(&go->lock_status);

    DEBUG_SYNC_C("leader_done_after_clean_status");

    /* signal the other group to update. */
    go = &go_[1 - thd->gu_ctx.idx];
    my_assert(cur_idx_ == 1 - thd->gu_ctx.idx);
    mysql_mutex_lock(&go->lock_status);
    /* Here we let another group enter GS_UPDATE. */
    go->update_status = GS_IDLE;
    if (go->status == GS_COLLECT) mysql_cond_broadcast(&go->cond_leader);
    mysql_mutex_unlock(&go->lock_status);

    can_reuse = thd->gu_ctx.get_gu();
  }

  thd->gu_ctx.reset();
  return can_reuse;
}

GroupUpdate *GroupUpdate::follower_done(THD *thd) {
  struct group *go = &go_[thd->gu_ctx.idx];
  GroupUpdate *can_reuse = nullptr;

  mysql_mutex_lock(&go->lock_status);

  if ((--go->n_follower_commit) == 0) {
    /* this is the last follower*/
    go->status = GS_IDLE;
    go->role_status = GS_IDLE;
    mysql_mutex_unlock(&go->lock_status);

    __sync_fetch_and_add(&commit_cnt_, go->n_prepare);

    /* signal the other group to update. */
    go = &go_[1 - thd->gu_ctx.idx];
    my_assert(cur_idx_ == 1 - thd->gu_ctx.idx);
    mysql_mutex_lock(&go->lock_status);
    /*
      Here we let another group enter GS_UPDATE, may let this group add new
      user, because another group will change idx and unlock with enter
      GS_PREPARE.
    */
    go->update_status = GS_IDLE;
    if (go->status == GS_COLLECT) mysql_cond_broadcast(&go->cond_leader);
    mysql_mutex_unlock(&go->lock_status);

    can_reuse = thd->gu_ctx.get_gu();
  } else
    mysql_mutex_unlock(&go->lock_status);

  /* clean the gu_ctx here. */
  thd->gu_ctx.reset();
  return can_reuse;
}

GroupUpdate *GroupUpdate::error_done(THD *thd) {
  struct group *go = &go_[thd->gu_ctx.idx];
  bool one_user = false, last_user = false;
  GroupUpdate *can_reuse = nullptr;

  mysql_mutex_lock(&go->lock_status);

  if (go->error_status == GS_IDLE) {
    /* This is the first error session */
    my_assert(go->n_error_done == 0);
    go->error_status = GS_ERROR;

    if (go->n_user == 1) {
      my_assert(thd->gu_ctx.is_leader());
      one_user = true;
    } else {
      if (thd->gu_ctx.is_follower()) mysql_cond_broadcast(&go->cond_leader);
      mysql_cond_broadcast(&go->cond_follower);
    }
  } else {
    /* This the last status. */
    my_assert(go->status != GS_IDLE);
  }

  if (++go->n_error_done >= go->n_user) {
    my_assert(go->error_status != GS_IDLE);
    go->n_user = 0;
    go->status = GS_IDLE;
    go->role_status = GS_IDLE;
    go->error_status = GS_IDLE;
    mysql_mutex_unlock(&go->lock_status);

    /* signal the other group to update. */
    go = &go_[1 - thd->gu_ctx.idx];

    mysql_mutex_lock(&go->lock_status);
    /*
      Here we let another group enter GS_UPDATE, may let this group add new
      user, because another group will change idx and unlock with enter
      GS_PREPARE.
      */
    if (hotspot_lock_type && go->status != GS_IDLE) go->error_status = GS_ERROR;

    go->update_status = GS_IDLE;
    if (go->status == GS_COLLECT || go->error_status != GS_IDLE)
      mysql_cond_broadcast(&go->cond_leader);
    mysql_mutex_unlock(&go->lock_status);
    last_user = true;
  } else
    mysql_mutex_unlock(&go->lock_status);

  if (!thd->is_error() && !one_user) {
    /* In some case, the in group update success without commit. */
    thd->get_stmt_da()->set_overwrite_status(true);
    my_error(ER_ERROR_DURING_COMMIT, MYF(0), 114, "");
    thd->get_stmt_da()->set_overwrite_status(false);
  }

  /* We do not count error only if there is no error and only one user. */
  if (thd->is_error()) {
    ++group_update_fail_count;
  }

  if (one_user || last_user) {
    can_reuse = thd->gu_ctx.get_gu();
  }

  thd->gu_ctx.reset();

  return can_reuse;
}

/* Impl of GroupUpdateMgr. */
GroupUpdate *GroupUpdateMgr::get_gu_and_lock(uchar *key, uint key_length) {
  GroupUpdate *gu = nullptr;
  mysql_rwlock_rdlock(&hash_lock_);
  auto it = hash_->find(std::string((char *)key, key_length));

  if (it == hash_->end()) {
    mysql_rwlock_unlock(&hash_lock_);
    mysql_rwlock_wrlock(&hash_lock_);
    it = hash_->find(std::string((char *)key, key_length));
    if (it == hash_->end()) {
      /* Here gu has alreay been locked. */
      gu = GroupUpdatePool::get_instance()->get_free_gu(this);
      gu->dump_key(key, key_length);
      hash_->emplace(std::string((char *)key, key_length), gu);
    } else {
      gu = it->second;
      gu->lock();
    }
  } else {
    gu = it->second;
    gu->lock();
    my_assert(gu->in_free_list == false);
  }

  mysql_rwlock_unlock(&hash_lock_);
  gu->in_free_list = false;

  return gu;
}

/* Impl of GroupUpdatePool. */
GroupUpdatePool *GroupUpdatePool::instance_ = nullptr;

/* Return a locked gu. */
GroupUpdate *GroupUpdatePool::get_free_gu(GroupUpdateMgr *mgr) {
  GroupUpdate *gu = nullptr;
  pthread_spin_lock(&free_list_lock_);
  my_assert(get_list_count(free_list_) == group_update_free_count);

  while (free_list_ && gu == nullptr) {
    gu = free_list_;
    gu->lock();
    my_assert(!gu->in_use());
    my_assert(!gu->has_leader());
    my_assert(gu->in_free_list == true);

    free_list_ = free_list_->next_free;
    gu->next_free = nullptr;
    gu->set_mgr(mgr);
    --group_update_free_count;
    if (gu->in_use_lock()) {
      /* For defence here. */
      gu->unlock();
      gu = nullptr;  // no cover line.
      my_assert(0);  // no cover line.
    }
  }
  my_assert(free_list_ || group_update_free_count == 0);

  my_assert(get_list_count(free_list_) == group_update_free_count);
  pthread_spin_unlock(&free_list_lock_);
  if (gu == nullptr) {
    gu = new GroupUpdate(mgr);
    ++group_update_total_count;
    gu->lock();
  } else
    ++group_update_reuse_count;
  my_assert(gu->next_free == nullptr);
  return gu;
}

GroupUpdate *gu_try_start_hotspot(THD *thd, const AccessPath *range_scan,
                                  TABLE *table) {
  im::Inventory_hint *hint;
  GroupUpdate *gu = nullptr;

  thd->gu_ctx.reset();

  if (!opt_hotspot) {
    return nullptr;
  }

  hint = nullptr;
  if (thd->lex->opt_hints_global) {
    hint = thd->lex->opt_hints_global->inventory_hint;
  }

  if (!(hotspot_for_autocommit &&
        thd->get_transaction()->is_empty(Transaction_ctx::SESSION)) &&
      !(hint && hint->is_specified(IC_HINT_COMMIT))) {
    return nullptr;
  }

  /*
    Disable group update if
    1) bin logging is disabled,
    2) or table is partitioned,
    3) or it's in trigger processing,
    4) or update is run within function.
  */
  if (range_scan && range_scan->type == AccessPath::INDEX_RANGE_SCAN &&
      (thd->variables.option_bits & OPTION_BIN_LOG) &&
#ifdef WITH_PARTITION_STORAGE_ENGINE
      !(table->part_info) &&
#endif
      !(table->triggers) && !(thd->in_sub_stmt)) {
    uint index = ::used_index(range_scan);

    if ((index == 0 || (table->key_info[index].flags &
                        (HA_NOSAME | HA_NULL_PART_KEY)) == HA_NOSAME) &&
        (get_used_key_parts(range_scan) ==
         table->key_info[index].usable_key_parts)) {
      if (range_scan->index_range_scan().num_ranges == 1) {
        QUICK_RANGE *range = range_scan->index_range_scan().ranges[0];
        if (range->flag & EQ_RANGE) {
          gu =
              GroupUpdatePool::get_instance()->get_mgr(table->s)->get_gu_and_lock(
                  range->min_key, range->min_length);
          gu->add_user(thd, table);
          return gu;
        }
      }
    }
  }

  return nullptr;
}

void gu_handle_fail_or_error(THD *thd, GroupUpdate *gu,
                             const ha_rows found_rows, bool view_check_fail,
                             int error, bool gu_locked) {
  if (thd->gu_ctx.is_not_gu()) {
    return;
  }
  /*
    If view check option fail, 'found' would be decreased, its value would
    still be 0, need to handle this in the ELSE IF part for leader.
  */
  /*
    Revision:
    If view check option fail, 'found' would NOT be decreased, its value would
    NOT be 0, now handle this in the ELSE IF part for leader.
  */
  if (found_rows == 0 && !(thd->gu_ctx.is_leader() && view_check_fail)) {
    if (thd->gu_ctx.is_leader()) {
      DEBUG_SYNC_C("before_leader_ignore_done");
      /* If leader found none, we should reuse the gu. */
      gu->leader_ignore_done(thd);
      gu->unlock();
      // GroupUpdatePool::get_instance()->try_release_gu(
      //     gu, thd->gu_ctx.version, thd->gu_ctx.reuse_version);
    }

    if (thd->gu_ctx.is_follower()) {
      /* If leader found none, we just ignore. May fail on fill_record. */
      thd->gu_ctx.reset();
      gu->unlock();
    }
  }
  /* Do cleanup and release lock if there is problem (error > 0). */
  else if (error > 0 && thd->gu_ctx.is_leader() && gu_locked) {
    gu->leader_update_error();
  }
}

void gu_finish_commit(THD *thd) {
  if (thd->gu_ctx.is_leader()) {
    GroupUpdate *gu;
    DEBUG_SYNC(thd, "before_leader_done");

    if ((gu = thd->gu_ctx.get_gu()->leader_done(thd)) != nullptr) {
      DEBUG_SYNC(thd, "after_leader_done_before_release_gu");
      GroupUpdatePool::get_instance()->try_release_gu(
          gu, thd->gu_ctx.reuse_version);
    }
    ++group_update_leader_count;
  } else if (thd->gu_ctx.is_follower()) {
    GroupUpdate *gu;
    if ((gu = thd->gu_ctx.get_gu()->follower_done(thd)) != nullptr) {
      GroupUpdatePool::get_instance()->try_release_gu(
          gu, thd->gu_ctx.reuse_version);
    }
    ++group_update_follower_count;
  }
}


