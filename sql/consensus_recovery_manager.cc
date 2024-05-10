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

#include <algorithm>

#include "consensus_recovery_manager.h"

#include "consensus_log_manager.h"
#include "handler.h"
#include "mysql/components/services/mysql_mutex_service.h"
#include "mysql/plugin.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld.h"
#include "psi_memory_key.h"
#include "sql/consensus/consensus_err.h"
#include "sql_plugin.h"

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
#endif

int Consensus_recovery_manager::init() {
  key_LOCK_consensus_log_recover_hash =
      key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
  mysql_mutex_init(key_LOCK_consensus_log_recover_hash,
                   &LOCK_consensuslog_recover_hash, MY_MUTEX_INIT_FAST);
  inited = true;
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

  xp::info(ER_XP_RECOVERY)
      << "XID = [" << xid << "]"
      << " Index = [" << consensus_index
      << " ] was added at "
         "Consensus_recovery_manager::add_trx_in_binlog ";
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void Consensus_recovery_manager::add_trx_in_binlog(uint64 consensus_index,
                                                   const XID &xid) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  external_xids_in_binlog[xid] = consensus_index;

  xp::info(ER_XP_RECOVERY) << "XID = [" << xid << "]"
                             << " Index = [" << consensus_index
                             << " ] was added at "
                                "Consensus_recovery_manager::add_trx_in_binlog";
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

template <>
void Consensus_recovery_manager::add_pending_recovering_trx<
    Pending_recovering_trx::xid_type::INTERNAL>(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XA_recover_txn *xa_trx,
    const XA_specification &xa_spec) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter = internal_xids_in_binlog.find(xa_trx->id.get_my_xid());
  if (iter != internal_xids_in_binlog.end()) {
    uint64 consensus_index = iter->second;
    xp::system(ER_XP_RECOVERY)
        << "XID = [" << xa_trx->id << "] Index = [" << consensus_index
        << "], current_state=" << (int)current_state
        << ", next_state=" << (int)next_state
        << " was added into commit map at "
           "Consensus_recovery_manager::add_xid_to_commit_map";
    add_pending_recovering_trx(ht, Pending_recovering_trx::xid_type::INTERNAL,
                               current_state, next_state, xa_trx, xa_spec,
                               consensus_index);
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

template <>
void Consensus_recovery_manager::add_pending_recovering_trx<
    Pending_recovering_trx::xid_type::EXTERNAL>(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XA_recover_txn *xa_trx,
    const XA_specification &xa_spec) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter = external_xids_in_binlog.find(xa_trx->id);
  if (iter != external_xids_in_binlog.end()) {
    uint64 consensus_index = iter->second;
    xp::system(ER_XP_RECOVERY)
        << "XID = [" << xa_trx->id << "] Index = [" << consensus_index << "]"
        << "], current_state=" << (int)current_state
        << ", next_state=" << (int)next_state
        << " was added into commit map at "
           "Consensus_recovery_manager::add_xid_to_commit_map";
    add_pending_recovering_trx(ht, Pending_recovering_trx::xid_type::EXTERNAL,
                               current_state, next_state, xa_trx, xa_spec,
                               consensus_index);
  } else {
    xp::warn(ER_XP_RECOVERY)
        << "XID = [" << xa_trx->id << "] "
        << "was not found at Consensus_recovery_manager::add_xid_to_commit_map";
    assert(current_state == enum_ha_recover_xa_state::PREPARED_IN_TC);
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void Consensus_recovery_manager::add_pending_recovering_trx(
    handlerton &ht, Pending_recovering_trx::xid_type type,
    enum_ha_recover_xa_state current_state, enum_ha_recover_xa_state next_state,
    const XA_recover_txn *xa_trx, const XA_specification &xa_spec,
    uint64 consensus_index) {
  if (std::count(Pending_recovering_trxs.begin(), Pending_recovering_trxs.end(),
                 consensus_index)) {
    xp::fatal(ER_XP_RECOVERY) << "same index = [" << consensus_index << "] "
                              << "is already in pending recovering trxs";
  }

  Pending_recovering_trxs.insert(std::make_unique<Pending_recovering_trx>(
      ht, type, current_state, next_state, xa_trx, xa_spec, consensus_index));
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
  Pending_recovering_trxs.clear();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

uint64 Consensus_recovery_manager::
    get_max_consensus_index_from_pending_recovering_trxs() {
  uint64 res = 0;
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  if (!Pending_recovering_trxs.empty()) {
    res = (*Pending_recovering_trxs.rbegin())->index();
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);

  return res;
}

int Consensus_recovery_manager::truncate_pending_recovering_trxs(
    uint64 consensus_index) {
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);

  auto lower_bound =
      std::lower_bound(Pending_recovering_trxs.begin(),
                       Pending_recovering_trxs.end(), consensus_index);

  std::for_each(
      lower_bound, Pending_recovering_trxs.end(),
      [&](const std::unique_ptr<Pending_recovering_trx> &trx) {
        assert(trx->index() >= consensus_index);
        xp::system(ER_XP_RECOVERY)
            << "XID = [" << trx->xid() << "] Index = [" << trx->index() << "]"
            << " is rollback since index large than " << consensus_index
            << " at "
               "Consensus_recovery_manager::truncate_pending_recovering_trxs ";
        trx->truncate();
        trx->withdraw();
      });

  Pending_recovering_trxs.erase(lower_bound, Pending_recovering_trxs.end());

  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return 0;
}

bool Consensus_recovery_manager::is_pending_recovering_trx_empty() {
  bool empty = false;
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  empty = Pending_recovering_trxs.empty();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return empty;
}

int Consensus_recovery_manager::recover_remaining_pending_recovering_trxs() {
  for (auto &trx : Pending_recovering_trxs) {
    trx->recover();
  }
  Pending_recovering_trxs.clear();
  return 0;
}

int Pending_recovering_trx::withdraw() {
  int ret = 0;
  xp::system(ER_XP_RECOVERY)
      << "XID = [" << xa_trx->id << "] "
      << ", type: " << (int)type << ", current_state: " << (int)current_state
      << ", next_state: " << (int)next_state
      << ", is immutable at Pending_recovering_trx::withdraw";
  assert(!processed);
  processed = true;

  assert(is_state_legal());

  if (type == xid_type::INTERNAL) {
    ret = ht.rollback_by_xid(&ht, &xa_trx->id, xa_spec);
    final_state = enum_ha_recover_xa_state::NOT_FOUND;
  } else if (type == xid_type::EXTERNAL) {
    if (current_state == enum_ha_recover_xa_state::PREPARED_IN_SE) {
      ret = ht.rollback_by_xid(&ht, &xa_trx->id, xa_spec);
      final_state = enum_ha_recover_xa_state::NOT_FOUND;
    } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
      final_state = enum_ha_recover_xa_state::PREPARED_IN_TC;
    }
  }
  return ret;
}

int Pending_recovering_trx::recover() {
  int ret = 0;
  xp::system(ER_XP_RECOVERY)
      << "XID = [" << xa_trx->id << "] "
      << ", type: " << (int)type << ", current_state: " << (int)current_state
      << ", next_state: " << (int)next_state
      << ", is immutable at Pending_recovering_trx::recover";
  assert(!processed);
  processed = true;

  assert(is_state_legal());

  if (type == xid_type::INTERNAL) {
    ret = ht.commit_by_xid(&ht, &xa_trx->id, xa_spec);
    final_state = enum_ha_recover_xa_state::COMMITTED;
  } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_SE) {
    if (next_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
      ret = ht.set_prepared_in_tc_by_xid(&ht, &xa_trx->id, xa_spec);
      final_state = enum_ha_recover_xa_state::PREPARED_IN_TC;
    } else if (next_state ==
               enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE) {
      ret = ht.commit_by_xid(&ht, &xa_trx->id, xa_spec);
      final_state = enum_ha_recover_xa_state::COMMITTED;
    }
  } else if (current_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
    if (next_state == enum_ha_recover_xa_state::COMMITTED) {
      ret = ht.commit_by_xid(&ht, &xa_trx->id, xa_spec);
      final_state = enum_ha_recover_xa_state::COMMITTED;
    } else if (next_state == enum_ha_recover_xa_state::ROLLEDBACK) {
      ret = ht.rollback_by_xid(&ht, &xa_trx->id, xa_spec);
      final_state = enum_ha_recover_xa_state::NOT_FOUND;
    }
  }
  return ret;
}

Pending_recovering_trx::Pending_recovering_trx(
    handlerton &ht, xid_type type, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XA_recover_txn *tmp_xa_trx,
    const XA_specification &xa_spec, uint64 consensus_index)
    : processed(false),
      ht(ht),
      type(type),
      current_state(current_state),
      next_state(next_state),
      final_state(enum_ha_recover_xa_state::NOT_FOUND),
      xa_trx(nullptr),
      xa_spec(xa_spec.clone()),
      consensus_index(consensus_index) {
  xa_trx = new XA_recover_txn();
  xa_trx->id = tmp_xa_trx->id;
  xa_trx->mod_tables = tmp_xa_trx->mod_tables;
}

Pending_recovering_trx::~Pending_recovering_trx() {
  if (!processed) {
    xp::fatal(ER_XP_RECOVERY)
        << "XID = [" << xa_trx->id << "] "
        << "is not recover or withdraw before "
           "Pending_recovering_trx::~Pending_recovering_trx: "
        << ", type: " << (int)type << ", current_state: " << (int)current_state
        << ", next_state: " << (int)next_state;
  }

  // final_state of a trx should be stable state, there are three stable
  // states: NOT_FOUND, COMMITTED, PREPARED_IN_TC
  assert(final_state == enum_ha_recover_xa_state::NOT_FOUND ||
         final_state == enum_ha_recover_xa_state::COMMITTED ||
         final_state == enum_ha_recover_xa_state::PREPARED_IN_TC);

  if (final_state == enum_ha_recover_xa_state::PREPARED_IN_TC) {
    if (Recovered_xa_transactions::instance().add_prepared_xa_transaction(
            xa_trx)) {
      xp::fatal(ER_XP_RECOVERY)
          << "Failed to add prepared xa transaction in "
             "Pending_recovering_trx::~Pending_recovering_trx";
    }
  }

  if (xa_trx != nullptr) {
    delete xa_trx;
    xa_trx = nullptr;
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
