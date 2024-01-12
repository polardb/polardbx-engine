/**
 * Replica Consistent Read Manager
 */

#include "replica_read_manager.h"

#include "mysqld.h"

uint64_t opt_replica_read_timeout = 0;

ReplicaReadManager replica_read_manager;

void ReplicaReadManager::update_lsn(uint64_t new_lsn) {
  std::unique_lock<std::mutex> lock(m_mutex);
  // Make sure the applied index must be monotonically increasing
  if (new_lsn <= m_applied_lsn.load(std::memory_order_acquire)) {
    return;
  }

  while (!m_wait_queue.empty() && m_wait_queue.top()->lsn <= new_lsn) {
    m_wait_queue.top()->cv.notify_one();
    m_wait_queue.pop();
  }

  m_applied_lsn.store(new_lsn, std::memory_order_release);
}

bool ReplicaReadManager::wait_for_lsn(uint64_t read_lsn) {
  // fast lock-free check
  if (read_lsn <= m_applied_lsn.load(std::memory_order_acquire)) {
    return true;
  }

  std::unique_lock<std::mutex> lock(m_mutex);
  if (read_lsn <= m_applied_lsn.load(std::memory_order_acquire)) {
    return true;
  }

  ReadLsnCond cond(read_lsn);
  m_wait_queue.push(&cond);
  auto status = cond.cv.wait_for(
      lock, std::chrono::milliseconds(opt_replica_read_timeout));
  if (status == std::cv_status::timeout) {
    m_wait_queue.erase(&cond);
    if (read_lsn <= m_applied_lsn.load(std::memory_order_acquire)) {
      return true;
    }
    return false;
  } else {
    return true;
  }
}
