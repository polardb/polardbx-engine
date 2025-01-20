#ifndef SLOW_QUERY_BLOCK_H
#define SLOW_QUERY_BLOCK_H

#include <pthread.h>
#include <sys/resource.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <regex>

#include "mutex_lock.h"
#include "my_sys.h"
#include "my_systime.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/sql_class.h"

namespace polarx {

extern PSI_mutex_key key_LOCK_slow_query_user_pattern;
extern mysql_mutex_t LOCK_slow_query_user_pattern;

/** Check if the user name matches the slow query block pattern. */
void check_sqb_user_pattern(THD *thd); 

class Sqb_config {
 public:
  Sqb_config() = default;

  Sqb_config(ulong time_limit, ulong cpu_time_limit, ulong cpu_limit,
             ulong interval)
      : sqb_exec_timeout(time_limit),
        sqb_exec_timeout_for_cpu_exceed(cpu_time_limit),
        sqb_cpu_percent_threshold(cpu_limit),
        sqb_check_interval(interval) {}

  bool is_valid() const;

  ulong sqb_exec_timeout;                 // Maximum execution time allowed
  ulong sqb_exec_timeout_for_cpu_exceed;  // Time threshold for
                                          // CPU usage check
  ulong sqb_cpu_percent_threshold;        // Maximum CPU usage
                                          // percentage allowed
  ulong sqb_check_interval;               // Polling interval in milliseconds
};

class Background_poller;

class Search_process_list : public Do_THD_Impl {
 public:
  explicit Search_process_list(const Sqb_config &cfg) : config(cfg) {}
  void operator()(THD *inspect_thd) override;

 private:
  double calculate_cpu_usage(pthread_t thread_id, ulong total_time,
                          THD *thd_to_kill) const;
  bool should_kill_query(THD *thd_to_kill, ulong total_time,
                         double cpu_usage) const;
  const Sqb_config &config;
};

class Background_poller {
 public:
  Background_poller();
  ~Background_poller();

  Background_poller(const Background_poller &) = delete;
  Background_poller &operator=(const Background_poller &) = delete;

  void start();
  void stop();

 private:
  static void *poll_function(void *arg);

  my_thread_handle work_thread;
  std::atomic<bool> running;
};

extern Background_poller *g_background_poller;

extern void create_background_poller();
extern void destory_background_poller();

}  // namespace polarx

#endif  // SLOW_QUERY_BLOCK_H
