#include "slow_query_block.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"
#include "storage/innobase/include/ut0log.h"
namespace polarx {

PSI_mutex_key key_LOCK_slow_query_user_pattern;
mysql_mutex_t LOCK_slow_query_user_pattern;

Background_poller *g_background_poller = nullptr;

void check_sqb_user_pattern(THD *thd) {
  if (!sqb_enable_slow_query_block) return;

  MUTEX_LOCK(lock_slow_query_user_pattern, &LOCK_slow_query_user_pattern);

  const std::string user_pattern = sqb_user_pattern;
  const char *user_name = thd->security_context()->priv_user().str;

  if (user_pattern == "" || user_name == nullptr) return;

  std::regex pattern(user_pattern);
  /** Only when set patern and match, block will be triggered */
  thd->sqb_should_block = std::regex_search(user_name, pattern);
}

bool Sqb_config::is_valid() const {
  return sqb_exec_timeout > 0 ||
         (sqb_exec_timeout_for_cpu_exceed > 0 && sqb_cpu_percent_threshold > 0);
}

ulong get_cur_time_ms() {
  ulong cur_utime = my_micro_time();
  timeval cur_time;
  my_micro_time_to_timeval(cur_utime, &cur_time);
  using namespace std::chrono;
  auto start_s = seconds(cur_time.tv_sec);
  auto start_usec = microseconds(cur_time.tv_usec);
  return duration_cast<milliseconds>(start_s).count() +
          duration_cast<milliseconds>(start_usec).count();
}

Sqb_config get_sqb_config() {
  Sqb_config config(sqb_exec_timeout, sqb_exec_timeout_for_cpu_exceed,
                    sqb_cpu_percent_threshold, sqb_check_interval);
  return config;
}

void Search_process_list::operator()(THD *thd_to_kill) {
  if (!thd_to_kill) return;

  /** If the account is not assigned, dont kill the query. */
  if (!thd_to_kill->security_context()->has_account_assigned()) return;

  MUTEX_LOCK(lock_thd_data, &thd_to_kill->LOCK_thd_data);

  if (!thd_to_kill->sqb_should_block ||
      thd_to_kill->killed == THD::KILL_CONNECTION ||
      thd_to_kill->slave_thread || !thd_to_kill->sqb_is_block_command()) {
    return;
  }

  pthread_t thread_id = thd_to_kill->real_id;

  ulong total_time = 0;
  /** When sqb_start_time is 0, it means the query is ended, cpu usage is 0. */
  ulong sqb_start_time = thd_to_kill->sqb_start_time;
  if (sqb_start_time > 0) {
    total_time = get_cur_time_ms() - sqb_start_time;
  }

  double cpu_usage = calculate_cpu_usage(thread_id, total_time, thd_to_kill);
  // ib::error() << "cpu_usage: " << cpu_usage << " total_time: " << total_time;

  if (should_kill_query(thd_to_kill, total_time, cpu_usage)) {
    thd_to_kill->awake(THD::KILL_QUERY);
  }
}

double Search_process_list::calculate_cpu_usage(pthread_t thread_id,
                                             ulong total_time,
                                             THD *thd_to_kill) const {
  if (total_time <= 0) return 0;
  clockid_t cid;
  if (thread_id != 0 && pthread_getcpuclockid(thread_id, &cid) == 0) {
    struct timespec ts;
    clock_gettime(cid, &ts);
    using namespace std::chrono;
    auto start_s = seconds(ts.tv_sec);
    auto start_ns = nanoseconds(ts.tv_nsec);
    ulong total_cpu_time = duration_cast<milliseconds>(start_s).count() +
                           duration_cast<milliseconds>(start_ns).count() -
                           thd_to_kill->sqb_cpu_start_time;
    double res = static_cast<double>(total_cpu_time * 100.0) / total_time;
    return res;
  }

  return 0;
}

bool Search_process_list::should_kill_query(THD *thd_to_kill, ulong total_time,
                                            double cpu_usage) const {

  if (config.sqb_exec_timeout > 0 && total_time > config.sqb_exec_timeout * 1000) {
    thd_to_kill->sqb_ret_error = Sqb_ret_error::SQB_RET_ERROR_TIME;
    return true;
  } else if (config.sqb_cpu_percent_threshold > 0 &&
             cpu_usage >= config.sqb_cpu_percent_threshold &&
             total_time > config.sqb_exec_timeout_for_cpu_exceed * 1000) {
    thd_to_kill->sqb_ret_error = Sqb_ret_error::SQB_RET_ERROR_CPU_AND_TIME;
    return true;
  }

  return false;
}

Background_poller::Background_poller() : running(false) {}

Background_poller::~Background_poller() { stop(); }

void Background_poller::start() {
  bool expected = false;
  if (running.compare_exchange_strong(expected, true)) {
    my_thread_create(&work_thread, nullptr, poll_function, this);
  }
}

void Background_poller::stop() {
  if (running.exchange(false)) {
    my_thread_join(&work_thread, nullptr);
  }
}

void *Background_poller::poll_function(void *arg) {
  auto *poller = static_cast<Background_poller *>(arg);

  while (poller->running) {
    Sqb_config config = get_sqb_config();
    Search_process_list search_process_list(config);
    if (Global_THD_manager::is_initialized() && config.is_valid()) {
      Global_THD_manager::get_instance()->do_for_all_thd(&search_process_list);
    }
    auto check_interval = std::chrono::seconds(config.sqb_check_interval);
    my_sleep(std::chrono::duration_cast<std::chrono::microseconds>(check_interval).count());
  }
  return nullptr;
}

void create_background_poller() {
  if (!sqb_enable_slow_query_block) return;

  if (!g_background_poller) {
    g_background_poller = new Background_poller();
    g_background_poller->start();
  }
}

void destory_background_poller() {
  if (g_background_poller) {
    delete g_background_poller;
    g_background_poller = nullptr;
  }
}

}  // namespace polarx
