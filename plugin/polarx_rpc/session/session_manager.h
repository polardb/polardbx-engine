//
// Created by zzy on 2022/7/25.
//

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "../common_define.h"
#include "../global_defines.h"
#include "../polarx_rpc.h"
#include "../server/epoll_group_ctx.h"
#include "../utility/atomicex.h"
#include "../utility/error.h"

#include "session.h"
#include "session_base.h"

namespace polarx_rpc {

class CtcpConnection;

class CsessionManager final {
  NO_COPY_MOVE(CsessionManager);

private:
  const std::string host_;
  const uint16_t port_;

  /// login info
  std::string user_;

  CspinRWLock session_lock_;
  std::unordered_map<uint64_t, std::shared_ptr<Csession>> sessions_;

public:
  CsessionManager(std::string host, uint16_t port)
      : host_(std::move(host)), port_(port) {}

  inline void set_user(const char *user) { user_ = user; }

  inline void shutdown(std::atomic<int> &counter) {
    DBG_LOG(("SessionMgr %p shutdown", this));
    std::vector<std::shared_ptr<Csession>> free_sessions;
    {
      CautoSpinRWLock lck(session_lock_, true, session_poll_rwlock_spin_cnt);
      free_sessions.reserve(sessions_.size());
      for (auto &s : sessions_)
        free_sessions.emplace_back(std::move(s.second));
      sessions_.clear();
    }
    counter.fetch_sub(static_cast<int>(free_sessions.size()),
                      std::memory_order_release);
    for (auto &s : free_sessions)
      s->shutdown();
  }

  void execute(CtcpConnection &tcp, const uint64_t &sid, msg_t &&msg,
               std::map<uint64_t, bool> &notify_set);
#ifdef MYSQL8
  err_t get_tso_mysql80(CtcpConnection &tcp, uint64_t &ts, int32_t batch_count);
#endif
  void execute_locally(CtcpConnection &tcp, const uint64_t &sid, msg_t &&msg);

  static err_t sql_stmt_execute_locally(CtcpConnection &tcp,
                                        reusable_session_t &session,
                                        const Polarx::Sql::StmtExecute &msg);

  inline err_t new_session(CtcpConnection &tcp, const uint64_t &sid,
                           std::shared_ptr<Csession> &ptr);

  inline std::shared_ptr<Csession> get_session(uint64_t sid) {
    CautoSpinRWLock lck(session_lock_, false, session_poll_rwlock_spin_cnt);
    auto it = sessions_.find(sid);
    if (it != sessions_.end())
      return it->second;
    return {};
  }

  inline bool remove_and_shutdown(std::atomic<int> &counter, uint64_t sid) {
    DBG_LOG(("SessionMgr %p remove and shutdown session %lu", this, sid));
    std::shared_ptr<Csession> s;
    {
      CautoSpinRWLock lck(session_lock_, true, session_poll_rwlock_spin_cnt);
      auto it = sessions_.find(sid);
      if (it != sessions_.end()) {
        s.swap(it->second);
        sessions_.erase(it);
      }
    }
    if (s) {
      counter.fetch_sub(1, std::memory_order_release);
      s->shutdown();
      return true;
    }
    return false;
  }
};

} // namespace polarx_rpc
