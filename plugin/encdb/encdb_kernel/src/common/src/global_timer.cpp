#include "global_timer.h"
#include <ctime>
#include "db_operation.h"

namespace encdb {

std::string Timer::Now() {
  thread_local std::time_t cached_os_time = 0;
  thread_local std::string cached_db_sys_time;
  std::time_t now;
  std::time(&now);
  if (now != cached_os_time) {
    encdb::hook::encdb_execute_sql_read(encdb::encdb_sql_read_sys_time(),
                                        cached_db_sys_time);
    cached_os_time = now;
  }
  return cached_db_sys_time;
}

}  // namespace encdb
