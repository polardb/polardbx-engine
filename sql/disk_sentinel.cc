/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#include <string>
#include "my_systime.h"  // my_time()
#include "sql/disk_sentinel.h"
#include "sql/sys_vars_ext.h"
#include "sql/binlog.h"
#include "my_sys.h"
#include "sql/log.h"
#include "sql/consensus/consensus_err.h"
#include "sql/mysqld.h"     // mysql_real_data_home
#include "my_stacktrace.h"
#include "sql/rpl_replica.h"
#include "sql/komodo0xlog.h"

DiskSentinel global_disk_sentinel;


#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key key_disk_writeable_sentinel;
PSI_mutex_key key_disk_hang_sentinel;
PSI_thread_key key_thread_disk_writeable_sentinel;
PSI_thread_key key_thread_disk_hang_sentinel;
#endif

int DiskSentinel::init()
{
  assert(!inited);
  inited = true;
  const auto basename = log_bin_basename ? log_bin_basename : relay_log_basename;
  snprintf(data.file_name, sizeof(data.file_name), 
           "%s/%s", mysql_real_data_home_ptr, data.disk_name);
  if (basename) {
    const auto log_dir_length = basename_index(basename, strlen(basename));
    snprintf(log.file_name, sizeof(data.file_name), 
            "%.*s/%s", log_dir_length, basename, log.disk_name);
  } else {
    snprintf(log.file_name, sizeof(data.file_name), 
            "%s/%s", mysql_real_data_home_ptr, log.disk_name);
  }

  if (writeable_sentinel.init(key_disk_writeable_sentinel, 
                              key_thread_disk_writeable_sentinel)
      || hang_sentinel.init(key_disk_hang_sentinel, 
                            key_thread_disk_hang_sentinel))
    return 1;

  xp::info(ER_XP_0) 
    << "Disk sentinel: Create threads succ"
    << ", data_file_name:" << data.file_name
    << ", log_file_name:" << log.file_name
    << ", check_interval_sec:" << opt_disk_sentinel_check_interval
    << ", max_retries:" << opt_disk_sentinel_max_retries
    << ", delay_threshold_ms:" << opt_disk_sentinel_delay_threshold;

  return 0;
}

void DiskSentinel::cleanup()
{
  if (!inited)
    return;
  inited = false;
  writeable_sentinel.cleanup();
  hang_sentinel.cleanup();
}

int DiskSentinel::SentinelThread::init(PSI_mutex_key &mutex_key,
  PSI_thread_key &thread_key)
{
  int error = 0;
  mysql_mutex_init(mutex_key, &mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(0, &cond);

  if ((error= mysql_thread_create(thread_key, &handler, nullptr, 
                                  disk_sentinel_run, this))) {
    xp::error(ER_XP_0) << "Failed to create thread."
      << ", errno=" << error;
    return 1;
  }

  return 0;
}

void DiskSentinel::SentinelThread::cleanup()
{
  terminate = true;
  mysql_cond_signal(&cond);
  /* wait thread_handler exit */
  my_thread_join(&handler, NULL);
  mysql_mutex_destroy(&mutex);
  mysql_cond_destroy(&cond);
}

void *disk_sentinel_run(void *arg)
{
  my_thread_init();
  DiskSentinel::SentinelThread &sentinel = *(DiskSentinel::SentinelThread *)arg;
  my_thread_self_setname(sentinel.name);

  xp::info(ER_XP_0) << "Thread " << sentinel.name << " run.";

  struct timespec ts;
  int err;
  for(;;)
  {
    set_timespec(&ts, opt_disk_sentinel_check_interval);
    mysql_mutex_lock(&sentinel.mutex);
    err = mysql_cond_timedwait(&sentinel.cond, &sentinel.mutex, &ts);
    if (sentinel.terminate) {
      mysql_mutex_unlock(&sentinel.mutex);
      break;
    }
    if (!opt_enable_disk_sentinel) {
      mysql_mutex_unlock(&sentinel.mutex);
      continue;
    }

    if (err == ETIMEDOUT || err == ETIME)
      sentinel.timeout_watcher();

    mysql_mutex_unlock(&sentinel.mutex);
  }
  my_thread_end();
  xp::info(ER_XP_0) << "Thread " << sentinel.name << " exited.";
  return NULL;
}

void DiskSentinel::WriteableSentinel::timeout_watcher()
{
  DBUG_EXECUTE_IF("disk_sentinel_io_hang", { sleep(30ULL); });
  global_disk_sentinel.data.check_disk_writeable();
  global_disk_sentinel.log.check_disk_writeable();
}

void DiskSentinel::HangSentinel::timeout_watcher()
{
  global_disk_sentinel.data.check_disk_hang();
  global_disk_sentinel.log.check_disk_hang();
}
void DiskSentinel::DiskItem::check_disk_writeable()
{
  char data[256];
  File fd;
  const auto disk_sentinel_delay_threshold = opt_disk_sentinel_delay_threshold;
  /* write to file */
  if ((fd = my_open(file_name, 
                    O_RDWR|O_CREAT|O_TRUNC|O_SYNC, MYF(MY_WME))) < 0) {
    xp::warn(ER_XP_0) << "Disk sentinel: open failed"
      << ", file_name:" << file_name
      << ", errno:" << my_errno()
      << ", count:" << check_count;
    return ;
  }

  auto startTime = my_milli_time();
  snprintf(data, 
           sizeof(data),
           "Disk sentinel count: %lu,\t"
           "timestamp: %llu sec,\t"
           "last write delay:%lu ms\t",
           check_count.load(), startTime / 1000000, last_write_delay);

  DBUG_EXECUTE_IF("simulate_disk_error_on_disk_sentinel", {
    DBUG_SET("+d,simulate_file_write_error");
  });

  if (my_write(fd, (const uchar*)data, 
               strlen(data), MYF_RW) == MY_FILE_ERROR) {
    DBUG_EXECUTE_IF("simulate_disk_error_on_disk_sentinel", {
      DBUG_SET("-d,simulate_file_write_error");
    });
    xp::warn(ER_XP_0) << "Disk sentinel: write failed"
      << ", file_name:" << file_name
      << ", errno:" << my_errno()
      << ", count:" << check_count;
    my_close(fd, MYF(MY_WME));
    return ;
  }
  last_write_delay = my_milli_time() - startTime;
  my_close(fd, MYF(MY_WME));

  if (disk_sentinel_delay_threshold > 0 &&
      last_write_delay > disk_sentinel_delay_threshold) {
    xp::warn(ER_XP_0) 
      << "Disk sentinel: write too slow"
      << ", count:" << check_count
      << ", cost_ms:" << last_write_delay
      << ", threshold_ms:" << disk_sentinel_delay_threshold;
    return ;
  }

  check_count++;
  return ;
}

void DiskSentinel::DiskItem::check_disk_hang()
{
  if (check_count.load() == last_check_count) {
    fail_retries++;
    const auto disk_sentinel_max_retries =  opt_disk_sentinel_max_retries;
    if (fail_retries >= disk_sentinel_max_retries
        && komodo::xlog_is_leader_not_single()) {
      my_safe_print_system_time();
      my_safe_printf_stderr(
        "[Fatal] Disk sentinel: IO problem occurs.\n"
        "-----------------------------------------\n"
        "check interval %llu sec, max_failures %llu, write delay threshold %llu ms\n"
        "Current storage:      %s\n"
        "Current disk:         %s\n"
        "Disk full or IO hang may happen on storage system. Abort myself.\n"
        "-----------------------------------------\n",
        opt_disk_sentinel_check_interval,
        disk_sentinel_max_retries, 
        opt_disk_sentinel_delay_threshold,
        "Local Disk",
        disk_name
      );
      if (DBUG_EVALUATE_IF("simulate_skip_exit", false, true))
        _exit(MYSQLD_FAILURE_EXIT);
      fail_retries = 0;
    }
  } else {
    last_check_count = check_count.load();
    fail_retries = 0;
  }
}
