//
// Created by zzy on 2022/9/6.
//

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>

class THD;

namespace polarx_rpc {

class CflowControl {
 public:
  inline void set_thd(THD *thd) { thd_ = thd; }

  inline bool flow_consume(int32_t token) {
    auto before = flow_counter_.fetch_sub(token);
    auto after = before - token;
    return after >= 0;
  }

  inline bool flow_wait() {
    bool exit;
    wait_begin();
    {
      std::unique_lock<std::mutex> lck(mutex_);
      while (!(exit = exit_.load(std::memory_order_acquire)) &&
             flow_counter_.load(std::memory_order_acquire) < 0)
        /// use 1s timeout to prevent missing notify
        cv_.wait_for(lck, std::chrono::seconds(1));
    }
    wait_end();
    return !exit;
  }

  inline void flow_reset(int32_t token) {
    flow_counter_.store(token, std::memory_order_release);
  }

  inline void flow_offer(int32_t token) {
    std::lock_guard<std::mutex> lck(mutex_);
    flow_reset(token);
    cv_.notify_one();  /// only one wait thread
  }

  inline void exit() {
    std::lock_guard<std::mutex> lck(mutex_);
    exit_.store(true, std::memory_order_release);
    cv_.notify_all();
  }

 private:
  THD *thd_{nullptr};

  std::atomic<bool> exit_{false};

  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<int64_t> flow_counter_{0};

  void wait_begin();
  void wait_end();
};

}  // namespace polarx_rpc
