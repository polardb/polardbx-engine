/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus_recovery_manager.h,v 1.0 08/22/2016 12:37:45 PM
 *droopy.hw(droopy.hw@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus_recovery_manager.h
 * @author droopy.hw(droopy.hw@alibaba-inc.com)
 * @date 08/22/2016 12:37:45 PM
 * @version 1.0
 * @brief the CONSENSUS recovery interface
 *
 **/

#ifndef CONSENSUS_RECOVERY_MANAGER_INCLUDE
#define CONSENSUS_RECOVERY_MANAGER_INCLUDE

// #include "my_global.h"
#include "binlog.h"
#include "handler.h"
#include "hash.h"
#include "xa.h"
#include "xa_specification.h"

#include <atomic>
#include <map>
#include <tuple>
#include <vector>

#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
#endif

/*
 *
 * Internal_xid: PREPARE_IN_SE ---recover--> COMMITTED
 *                             ---withdraw--> NOT_FOUND
 *
 * External_xid: PREPARE_IN_SE ---recover--> PREPARE_IN_TC/
 *                                           COMMITTED_WITH_ONEPHASE
 *                             ---withdraw--> NOT_FOUND
 *               PREPARE_IN_TC ---recover--> COMMITTED/ROLLBACK
 *                             ---withdraw--> PREPARE_IN_TC
 */
class Pending_recovering_trx {
 public:
  enum class xid_type {
    INTERNAL,
    EXTERNAL,
  };

  Pending_recovering_trx(handlerton &ht, xid_type type,
                         enum_ha_recover_xa_state current_state,
                         enum_ha_recover_xa_state next_state,
                         const XA_recover_txn *xa_trx,
                         const XA_specification &xa_spec,
                         uint64 consensus_index);

  ~Pending_recovering_trx();

  Pending_recovering_trx(const Pending_recovering_trx &other) = delete;
  Pending_recovering_trx &operator=(const Pending_recovering_trx &other) =
      delete;
  Pending_recovering_trx(Pending_recovering_trx &&other) = delete;
  Pending_recovering_trx &operator=(Pending_recovering_trx &&other) = delete;

  bool operator<(const Pending_recovering_trx &rhs) const {
    return index() < rhs.index();
  }

  [[nodiscard]] std::string name() const { return xa_trx->id.get_data(); }
  [[nodiscard]] XID &xid() const { return xa_trx->id; }
  [[nodiscard]] uint64 index() const { return consensus_index; }

  void truncate() { xa_spec->clear(); }
  int withdraw();

  int recover();

 private:
  bool is_state_legal();

 private:
  bool processed;

  handlerton &ht;
  const xid_type type;
  const enum_ha_recover_xa_state current_state;
  const enum_ha_recover_xa_state next_state;
  enum_ha_recover_xa_state final_state;
  XA_recover_txn *xa_trx;
  XA_specification *xa_spec;
  const uint64 consensus_index;
};

static inline bool operator<(const Pending_recovering_trx &lhs, uint64 rhs) {
  return lhs.index() < rhs;
}

static inline bool operator<(uint64 lhs, const Pending_recovering_trx &rhs) {
  return lhs < rhs.index();
}

static inline bool operator==(uint64 lhs, const Pending_recovering_trx &rhs) {
  return !(lhs < rhs) && !(rhs < lhs);
}

static inline bool operator==(const Pending_recovering_trx &lhs, uint64 rhs) {
  return !(lhs < rhs) && !(rhs < lhs);
}

static inline bool operator<(const std::unique_ptr<Pending_recovering_trx> &lhs,
                             uint64 rhs) {
  return *lhs < rhs;
}

static inline bool operator<(
    uint64 lhs, const std::unique_ptr<Pending_recovering_trx> &rhs) {
  return lhs < *rhs;
}

static inline bool operator==(
    const std::unique_ptr<Pending_recovering_trx> &lhs, uint64 rhs) {
  return !(lhs < rhs) && !(rhs < lhs);
}

static inline bool operator==(
    uint64 lhs, const std::unique_ptr<Pending_recovering_trx> &rhs) {
  return !(lhs < rhs) && !(rhs < lhs);
}

static inline bool operator<(
    const std::unique_ptr<Pending_recovering_trx> &lhs,
    const std::unique_ptr<Pending_recovering_trx> &rhs) {
  return *lhs < *rhs;
}

class Consensus_recovery_manager {
 public:
  Consensus_recovery_manager()
      : inited(false),
        key_LOCK_consensus_log_recover_hash(),
        last_leader_term_index(0),
        internal_xids_in_binlog(),
        external_xids_in_binlog(),
        Pending_recovering_trxs() {}
  ~Consensus_recovery_manager() = default;

  int init();
  int cleanup();

  [[nodiscard]] uint64 get_last_leader_term_index() const {
    return last_leader_term_index;
  }

  void set_last_leader_term_index(uint64 last_leader_term_index_arg) {
    last_leader_term_index = last_leader_term_index_arg;
  }

  // step 1: collect trx in binlog
  void add_trx_in_binlog(uint64 consensus_index, uint64 xid);
  void add_trx_in_binlog(uint64 consensus_index, const XID &xid);

  // step 2: collect pending recovering trx in engine
  template <Pending_recovering_trx::xid_type XID_TYPE>
  void add_pending_recovering_trx(handlerton &ht,
                                  enum_ha_recover_xa_state current_state,
                                  enum_ha_recover_xa_state next_state,
                                  const XA_recover_txn *xa_trx,
                                  const XA_specification &xa_spec);

  // step 3: truncate pending recovering trx after commit idx
  int truncate_pending_recovering_trxs(uint64 consensus_index);

  // step 4: recovering remaining pending recovering trx after truncation
  int recover_remaining_pending_recovering_trxs();

  void clear();
  void clear_trx_in_binlog();
  uint64 get_max_consensus_index_from_pending_recovering_trxs();
  bool is_pending_recovering_trx_empty();

 private:
  bool inited;
  PSI_mutex_key key_LOCK_consensus_log_recover_hash;
  mysql_mutex_t LOCK_consensuslog_recover_hash;

  uint64 last_leader_term_index;
  std::map<uint64, uint64> internal_xids_in_binlog;
  std::map<XID, uint64> external_xids_in_binlog;
  std::set<std::unique_ptr<Pending_recovering_trx>> Pending_recovering_trxs;

 private:
  void add_pending_recovering_trx(handlerton &ht,
                                  Pending_recovering_trx::xid_type type,
                                  enum_ha_recover_xa_state prepare_state,
                                  enum_ha_recover_xa_state committed_state,
                                  const XA_recover_txn *xa_trx,
                                  const XA_specification &xa_spec,
                                  uint64 consensus_index);
};

#endif
