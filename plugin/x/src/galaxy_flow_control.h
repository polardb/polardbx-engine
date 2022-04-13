//
// Created by zzy on 2021/12/2.
//

#pragma once

#include <atomic>
#include <cstdint>

#include "plugin/x/src/helper/multithread/synchronize.h"
#include "plugin/x/src/xpl_performance_schema.h"

class THD;

namespace xpl {

class Galaxy_flow_control {
 public:
  inline void set_thd(THD *thd) { m_thd = thd; }

  inline bool flow_consume(int32_t token) {
    auto before = m_flow_counter.fetch_sub(token);
    auto after = before - token;
    return after > 0;
  }

  inline bool flow_wait() {
    bool exit;
    wait_begin();
    auto lck(m_flow_sync.block());
    while (!(exit = m_exit.load(std::memory_order_acquire)) &&
           m_flow_counter.load(std::memory_order_acquire) <= 0)
      // Use 1s timeout to prevent missing notify.
      lck.timed_wait(1000000000);
    wait_end();
    return !exit;
  }

  inline void flow_reset(int32_t token) {
    m_flow_counter.store(token, std::memory_order_release);
  }

  inline void flow_offer(int32_t token) {
    auto lck(m_flow_sync.block());
    flow_reset(token);
    lck.notify();  // Only one wait thread.
  }

  inline void exit() {
    auto lck(m_flow_sync.block());
    m_exit.store(true, std::memory_order_release);
    lck.broadcast();
  }

 private:
  THD *m_thd{nullptr};

  std::atomic<bool> m_exit{false};

  Synchronize m_flow_sync{KEY_mutex_gx_flow_control, KEY_cond_gx_flow_control};
  std::atomic<int64_t> m_flow_counter{0};

  void wait_begin();
  void wait_end();
};

}  // namespace xpl
