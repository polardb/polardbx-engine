/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus_recovery_manager.cc,v 1.0 08/22/2016 09:16:32 AM
 *droopy.hw(droopy.hw@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus_recovery_manager.cc
 * @author droopy.hw(droopy.hw@alibaba-inc.com)
 * @date 08/22/2016 09:16:32 PM
 * @version 1.0
 * @brief The implement of the CONSENSUS recovery
 *
 **/

#include "consensus_recovery_manager.h"

#include "consensus_log_manager.h"
#include "handler.h"
#include "mysql/components/services/mysql_mutex_service.h"
#include "mysql/plugin.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld.h"
#include "psi_memory_key.h"
#include "raft/raft0err.h"
#include "sql_plugin.h"

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
#endif

int Consensus_recovery_manager::init() {
  key_LOCK_consensus_log_recover_hash =
      key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
  mysql_mutex_init(key_LOCK_consensus_log_recover_hash,
                   &LOCK_consensuslog_recover_hash, MY_MUTEX_INIT_FAST);
  inited = TRUE;
  last_leader_term_index = 0;
  return 0;
}

int Consensus_recovery_manager::cleanup() {
  if (inited) {
    clear();
    mysql_mutex_destroy(&LOCK_consensuslog_recover_hash);
  }
  return 0;
}

void Consensus_recovery_manager::add_trx_in_binlog(uint64 consensus_index,
                                                   uint64 xid) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  internal_xids_in_binlog[xid] = consensus_index;

  raft::system(ER_RAFT_RECOVERY)
      << "XID = [" << xid << "]"
      << " Index = [" << consensus_index
      << " ] was added at "
         "Consensus_recovery_manager::add_trx_in_binlog";
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void Consensus_recovery_manager::add_trx_in_binlog(uint64 consensus_index,
                                                   const XID &xid) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  external_xids_in_binlog[xid] = consensus_index;

  raft::system(ER_RAFT_RECOVERY)
      << "XID = [" << xid.key() << "]"
      << " Index = [" << consensus_index
      << " ] was added at "
         "Consensus_recovery_manager::add_trx_in_binlog";
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

template <>
void Consensus_recovery_manager::add_pending_recovering_trx<
    Pending_recovering_trx::xid_type::INTERNAL>(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XID &xid,
    const XA_specification &xa_spec) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter = internal_xids_in_binlog.find(xid.get_my_xid());
  if (iter != internal_xids_in_binlog.end()) {
    uint64 consensus_index = iter->second;
    raft::system(ER_RAFT_RECOVERY)
        << "XID = [" << xid.get_my_xid() << "] Index = [" << consensus_index
        << "]"
        << " was added into commit map at "
           "Consensus_recovery_manager::add_xid_to_commit_map";
    add_pending_recovering_trx(ht, Pending_recovering_trx::xid_type::INTERNAL,
                               current_state, next_state, xid, xa_spec,
                               consensus_index);
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

template <>
void Consensus_recovery_manager::add_pending_recovering_trx<
    Pending_recovering_trx::xid_type::EXTERNAL>(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XID &xid,
    const XA_specification &xa_spec) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter = external_xids_in_binlog.find(xid);
  if (iter != external_xids_in_binlog.end()) {
    uint64 consensus_index = iter->second;
    raft::system(ER_RAFT_RECOVERY)
        << "XID = [" << xid.key() << "] Index = [" << consensus_index << "]"
        << " was added into commit map at "
           "Consensus_recovery_manager::nladd_xid_to_commit_map";
    add_pending_recovering_trx(ht, Pending_recovering_trx::xid_type::EXTERNAL,
                               current_state, next_state, xid, xa_spec,
                               consensus_index);
  } else {
    raft::warn(ER_RAFT_RECOVERY)
        << "XID = [" << xid.key() << "] "
        << "was not found at Consensus_recovery_manager::add_xid_to_commit_map";
    assert(current_state == enum_ha_recover_xa_state::PREPARED_IN_TC);
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void Consensus_recovery_manager::add_pending_recovering_trx(
    handlerton &ht, Pending_recovering_trx::xid_type type,
    enum_ha_recover_xa_state current_state, enum_ha_recover_xa_state next_state,
    const XID &xid, const XA_specification &xa_spec, uint64 consensus_index) {
  XID *dup_xid = (XID *)my_memdup(key_memory_xa_transaction_contexts, &xid,
                                  sizeof(XID), MYF(0));
  Pending_Recovering_trxs[consensus_index] =
      std::make_unique<Pending_recovering_trx>(ht, type, current_state,
                                               next_state, dup_xid, xa_spec,
                                               consensus_index);
}

void Consensus_recovery_manager::clear_trx_in_binlog() {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  internal_xids_in_binlog.clear();
  external_xids_in_binlog.clear();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void Consensus_recovery_manager::clear() {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  internal_xids_in_binlog.clear();
  external_xids_in_binlog.clear();
  Pending_Recovering_trxs.clear();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

uint64 Consensus_recovery_manager::
    get_max_consensus_index_from_pending_recovering_trxs() {
  uint64 max_consensus_index;
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter = Pending_Recovering_trxs.rbegin();
  if (iter == Pending_Recovering_trxs.rend()) {
    max_consensus_index = 0;
  } else {
    max_consensus_index = iter->first;
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);

  raft::system(ER_RAFT_RECOVERY)
      << "Found max consensus index = [" << max_consensus_index << "]"
      << "at "
         "Consensus_recovery_manager::get_max_consensus_index_from_recover_trx_"
         "hash";
  return max_consensus_index;
}

int Consensus_recovery_manager::truncate_pending_recovering_trxs(
    uint64 consensus_index) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  for (auto iter = Pending_Recovering_trxs.begin();
       iter != Pending_Recovering_trxs.end();) {
    if (iter->first >= consensus_index) {
      raft::system(ER_RAFT_RECOVERY)
          << "XID = [" << iter->second->name() << "] Index = [" << iter->first
          << "]"
          << " is rollback since index large than " << consensus_index
          << " at Consensus_recovery_manager::truncate_pending_recovering_trxs";

      iter->second->withdraw();
      Pending_Recovering_trxs.erase(iter++);
    } else {
      iter++;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return 0;
}

bool Consensus_recovery_manager::is_pending_recovering_trx_empty() {
  bool empty = FALSE;
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  empty = Pending_Recovering_trxs.empty();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return empty;
}

// truncate the continuous xids of the prepared xids
int Consensus_recovery_manager::truncate_not_confirmed_pending_recovering_trxs(
    uint64 max_index_in_binlog_file) {
  auto iter = Pending_Recovering_trxs.rbegin();
  uint64 index = max_index_in_binlog_file;

  while (iter != Pending_Recovering_trxs.rend() && iter->first == index) {
    iter++;
    index--;
  }

  index++;
  if (index <= max_index_in_binlog_file) {
    raft::system(ER_RAFT_RECOVERY)
        << "Last leader term index to = [" << index - 1 << "] but max index = ["
        << max_index_in_binlog_file << "] in binlog file at "
        << "Consensus_recovery_manager::truncate_not_confirmed_pending_"
           "recovering_trxs ";
    truncate_pending_recovering_trxs(index);
    set_last_leader_term_index(index - 1);
  }

  return 0;
}

int Consensus_recovery_manager::recover_remaining_pending_recovering_trxs() {
  uint64 recover_start_apply_index =
      consensus_log_manager.get_consensus_info()->get_start_apply_index();

  if (opt_recover_snapshot && recover_start_apply_index) {
    truncate_not_confirmed_pending_recovering_trxs(
        get_last_leader_term_index());
  }

  for (auto &iter : Pending_Recovering_trxs) {
    if (recover_start_apply_index == 0 ||
        iter.first <= recover_start_apply_index) {
      iter.second->recover();
    } else {
      // this is not possible
      assert(0);
      iter.second->withdraw();
    }
  }
  Pending_Recovering_trxs.clear();
  return 0;
}

int Pending_recovering_trx::withdraw() {
  int ret = 0;
  if (immutable) {
    raft::error(ER_RAFT_RECOVERY)
        << "XID = [" << xid->get_my_xid() << "] "
        << "is immutable at Pending_recovering_trx::rollback";
  }
  immutable = true;

  assert(is_state_legal());

  if (type == xid_type::INTERNAL) {
    ret = ht.rollback_by_xid(&ht, xid, xa_spec);
  } else if (type == xid_type::EXTERNAL) {
    if (current_state == enum_ha_recover_xa_state::PREPARED_IN_SE) {
      ret = ht.rollback_by_xid(&ht, xid, xa_spec);
    } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
      // do nothing
    }
  }

  return ret;
}

int Pending_recovering_trx::recover() {
  int ret = 0;
  if (immutable) {
    raft::error(ER_RAFT_RECOVERY)
        << "XID = [" << xid->get_my_xid() << "] "
        << "is immutable at Pending_recovering_trx::commit";
  }
  immutable = true;

  assert(is_state_legal());

  if (type == xid_type::INTERNAL) {
    ret = ht.commit_by_xid(&ht, xid, xa_spec);
  } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_SE) {
    if (next_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
      ret = ht.set_prepared_in_tc_by_xid(&ht, xid, xa_spec);
    } else if (next_state ==
               enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE) {
      ret = ht.commit_by_xid(&ht, xid, xa_spec);
    }
  } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
    if (next_state == enum_ha_recover_xa_state::COMMITTED) {
      ret = ht.commit_by_xid(&ht, xid, xa_spec);
    } else if (next_state == enum_ha_recover_xa_state::ROLLEDBACK) {
      ret = ht.rollback_by_xid(&ht, xid, xa_spec);
    }
  }
  return ret;
}

Pending_recovering_trx::Pending_recovering_trx(
    handlerton &ht, xid_type type, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, XID *xid,
    const XA_specification &xa_spec, uint64 consensus_index)
    : immutable(false),
      ht(ht),
      type(type),
      current_state(current_state),
      next_state(next_state),
      xid(xid),
      xa_spec(xa_spec.clone()),
      consensus_index(consensus_index) {}

Pending_recovering_trx::~Pending_recovering_trx() {
  if (!immutable) {
    raft::error(ER_RAFT_RECOVERY)
        << "XID = [" << xid->get_my_xid() << "] "
        << "is not recover or withdraw at "
           "Pending_recovering_trx::~Pending_recovering_trx";
  }

  if (xid != nullptr) {
    my_free(xid);
    xid = nullptr;
  }
  if (xa_spec != nullptr) {
    delete xa_spec;
    xa_spec = nullptr;
  }
}

bool Pending_recovering_trx::is_state_legal() {
  bool ret = true;
  if (type == xid_type::INTERNAL) {
    if (current_state != enum_ha_recover_xa_state::PREPARED_IN_SE ||
        next_state != enum_ha_recover_xa_state::COMMITTED) {
      ret = false;
    }
  } else if (type == xid_type::EXTERNAL) {
    if (current_state == enum_ha_recover_xa_state::PREPARED_IN_SE) {
      if (next_state != enum_ha_recover_xa_state::PREPARED_IN_TC &&
          next_state != enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE) {
        ret = false;
      }
    } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
      if (next_state != enum_ha_recover_xa_state::COMMITTED &&
          next_state != enum_ha_recover_xa_state::ROLLEDBACK) {
        ret = false;
      }
    } else {
      ret = false;
    }
  }
  return ret;
}
