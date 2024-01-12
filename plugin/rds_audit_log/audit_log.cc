/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights
   reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file

  @brief
  logging of all commands
*/

#include "audit_log.h"
#include <my_systime.h>
#include <mysql/components/my_service.h>
#include <mysql/components/services/log_builtins.h>
#include <mysql/psi/mysql_file.h>
#include <mysql/psi/mysql_thread.h>
#include <mysqld_error.h>
#include <sql/mysqld.h> /* for mysql_tmpdir */
#include <sql/sql_parse.h>
#include "json_serializer.h"
#include "plain_serializer.h"

/*
  This Audit Log Plugin implementation is based on RDS MySQL Audit
  Log (Dingqi) and PolarDB Audit Log (Hanyi).

  The design is based on our requirements about SQL auditing, and
  official Oracle MySQL manual of Enterprise Audit Log Plugin.
  If a system varaible/status has the same name with Enterpise
  Audit Log, its usage or purpose will likely be same.

  Audit Log Format
  ===============
  Audit Log can be recorded in multiple formats. Serializer is used
  to convert audit event to audit log records, so for each format
  there is a corresponding serializer. Currently only PLAIN format
  is implemented, this is a format compatible with RDS MySQL 5.6
  MYSQL_V1 format.

  TODO In the future, will support JSON format.

  A readonly global variable, rds_audit_log_format, is used to control
  to log format, this variable will support dynamic change when we
  support JSON format.

  Audit Log Strategy
  =================
  Audit log records can be write to log file in multiple strategy.
  This is totally same with upstream log strategy:

  ASYNCHRONOUS:    Log asynchronously. Wait for space in the output buffer.
  PERFORMANCE:     Log asynchronously. Drop requests for which there is
                   insufficient space in the output buffer.
  SEMISYNCHRONOUS: Log synchronously. Permit caching by the operating system.
  SYNCHRONOUS:     Log synchronously. Call sync() after each request.

  For ASYNCHRONOUS and PERFORMANCE strategy, a global log_buf and a
  background log flush thread are used. The user thread will write log
  records into log_buf, which will be asynchronously flushed to log file
  by log flushing thread.

  The difference betwen ASYNCHRONOUS and PERFORMANCE is how log records
  is handled when log_buf is full. ASYNCHRONOUS will wait for free space,
  PERFORMANCE will simply discard log records.

  For SEMISYNCHRONOUS and SYNCHRONOUS, log_buf is not used, and user thread
  write log records to file directly. The difference is for SEMISYNCHRONOUS,
  user thread just write log records to file system cache (pwrite()), and
  denpend on the OS to persist to disk. While for SYNCHRONOUS, user thread
  will make sure log records is persisted (pwrite() + fsync()).

  The variable rds_audit_log_strategy is used to control log strategy, and
  can be changed dynamically. Changing strategy is a critical operation,
  log writting of all user threads will be blocked, so it's better to do this
  in a low workload period.

  No audit logs will be lost during switching algorithm.

  Audit log buffer size is controlled by variable
  rds_audit_log_buffer_size, which supports changing dynamically.
  It is recommented to configure 1% of specification memory to
  audit log buffer. For example, if we have 470GB memory, it is
  better to make audit log buffer larger than 4GB.

  We will lost some audit logs if log_buf is too small compared to
  the audit log generating speed when PERFORMANCE strategy is used.
  One specified message will be printed to error-log when we find the
  first lost audit log and another message will be printed to error-log
  when rotating log file. We will notice that some audit logs must be lost
  between these two error log timestamps.
  For example:
  2018-05-04 16:18:30 21839 [ERROR] Some audit logs are lost because audit log
  ......
  ......
  2018-05-04 16:19:15 21839 [ERROR] Some audit logs have lost before this
  timestamp Thus, we can infer that audit logs between 2018-05-04 16:18:30 and
  2018-05-04 16:19:15 are not complete.

  When audit buffer logs is full, we will make flushing thread to do
  more work by:
  a) Signal the flushing thread if it is sleeping
  b) Change sleep time among each flushing batch to the
  minimal value.

  We can use command 'show status like '%audit%'' to monitor audit
  log flushing status.

  Lock Rationale
  ===============
  Two Partitioned_rwlocks are used:
  LOCK_log, to synchronize critical operation and log writting.
  LOCK_file, to synchronize log rotation and log file writting.

  We use RDS_AUDIT_LOG_LOCK_PARTITIONS = 32 partitions for each
  Partitioned_rwlock.

  LOCK_log
  --------
  Every user thread is assigned one partition lock based on its thread_id.
  Read lock is needed for user thread log writting, for both buffered and
  unbuffered write. So user thread can do log writting concurrently.

  For critical operations, such as change log strategy, Write locks on all
  partitions are needed. User thread will be blocked for writting. Log file
  rotation is handled differently, and check LOCK_file subsection for more
  details.

  Normally, the backgroud log flush thread also requests one partition lock,
  to make flushing mutually exclusive with critical operation.

  LOCK_file
  ---------
  In our design, log rotation is a frequent operation, and we don't want it
  block front user writting (perf drop). LOCK_file lock is introduced for
  this purpose.

  LOCK_file is needed to protect log_fd, log_name and other log file
  related members.

  When rotate a new log file, a READ lock of LOCK_log is held, and all
  WRITE locks of LOCK_file are held.

  For buffered-write, user threads needn't write to log file, there is
  no need to hold LOCK_file, so user threads can write to log_buf even if
  there is a concurrent log file rotation (This is target scenario that
  we expect improvement).

  For unbuffered-write, user threads write to log file directly, a READ
  lokc of LOCK_file is needed, so user threads writting is blocked while
  there a log file rotation. But user threads themselves can write
  concurrently if there is no log file rotation.

  The lock order is:
  LOCK_log -> LOCK_file

  Aotimic offset and LOCKs
  ========================
  There are 3 offset that are very important for audit log.

  curr_file_pos
  ------------
  This offset is related to log_fd. It is increased when new log records
  is written to log file.

  In unbuffered-write mode, this offset is increased by user threads
  (write_to_file()).

  In buffered-write mode, this offset is increased by background log flush
  thread, or maintain operations (flush_log_buf()).

  In both cases, atomic operation is used to increase, but a read lock of
  LOG_file is required. The purpose is make curr_file_pos conistent with
  log_fd.

  Rotating log file operation (rotate_log_file() will reset curr_file_pos,
  and create a new log_fd, all WRITE locks of LOG_file is required.

  flushed_offset
  --------------
  This offset is related to log_buf, and only used in buffered-write mode.
  It is increased when content in log_buf is flushed out, background log
  flush thread or maintain operations (flush_log_buf()).

  Atomic operation is used to increse, but a read lock of LOCK_log is
  required. The purpose is make flushed_offset conistent with log_buf.

  Resizing log buf operation (resize_log_buf()) will reset flushed_offset
  and realloc log_buf, all WRITE locks of LOCK_log is required.

  buffered_offset
  --------------
  This offset is related to log_buf, and only used in buffered-write mode.
  It is increased when log buf space is allocated from log_buf, by user
  threads.

  Atomic operation is used to increse, but a read lock of LOCK_log is
  required. The purpose is make buffered_offset conistent with log_buf.

  Resizing log buf operation (resize_log_buf()) will reset buffered_offset
  and realloc log_buf, all WRITE locks of LOCK_log is required.

*/

/* Singleton of MySQL_RDS_AUDIT_LOG */
MYSQL_RDS_AUDIT_LOG *rds_audit_log = NULL;

extern PSI_memory_key key_memory_audit_log_buf;
#ifdef HAVE_PSI_INTERFACE
extern PSI_rwlock_key key_rwlock_audit_lock_log;
extern PSI_rwlock_key key_rwlock_audit_lock_file;
extern PSI_mutex_key key_mutex_audit_flush_sleep;
extern PSI_mutex_key key_mutex_audit_buf_full_sleep;
extern PSI_cond_key key_cond_audit_flush_sleep;
extern PSI_cond_key key_cond_audit_buf_full_sleep;
extern PSI_thread_key key_thread_audit_flush;
#endif

/* Constuctor */
MYSQL_RDS_AUDIT_LOG::MYSQL_RDS_AUDIT_LOG(const char *dir, ulong n_buf_size,
                                         ulong n_row_limit, ulong log_format,
                                         ulong log_strategy, ulong log_policy,
                                         ulong log_conn_policy,
                                         ulong log_stmt_policy,
                                         ulong log_version, bool enable)
    : m_enabled(false),
      log_dir(dir),
      m_file_id(0),
      log_fd(-1),
      log_buf_size(n_buf_size),
      buf_full(false),
      flush_thread_running(false),
      m_aborted(false),
      row_limit(n_row_limit),
      last_row_num(0),
      lost_row_num_by_buf_full(0),
      lost_row_num_by_buf_full_total(0),
      lost_row_num_by_row_limit(0),
      lost_row_num_by_row_limit_total(0),
      last_flush_len(0),
      last_sleep_time(0) {
  buffered_offset.store(0);
  flushed_offset.store(0);
  curr_file_pos.store(0);
  row_num.store(0);

  log_total_size.store(0);
  log_event_max_drop_size.store(0);
  log_events.store(0);
  log_events_filtered.store(0);
  log_events_lost.store(0);
  log_events_written.store(0);
  log_write_waits.store(0);
  log_file_writes.store(0);
  log_file_syncs.store(0);

  memset(log_name, 0, FN_REFLEN);

  m_format = (enum_log_format)log_format;

  if (m_format == PLAIN) {
    m_serializer = new Plain_serializer();
  } else if (m_format == JSON) {
    m_serializer = new JSON_serializer();
  }

  log_buf = (uchar *)my_malloc(key_memory_audit_log_buf, log_buf_size,
                               MYF(MY_FAE | MY_ZEROFILL));

  LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_INIT_LOG_BUF,
               log_buf_size / 1024 / 1024);

  LOCK_log.init(RDS_AUDIT_LOG_LOCK_PARTITIONS
#ifdef HAVE_PSI_INTERFACE
                ,
                key_rwlock_audit_lock_log
#endif
  );

  LOCK_file.init(RDS_AUDIT_LOG_LOCK_PARTITIONS
#ifdef HAVE_PSI_INTERFACE
                 ,
                 key_rwlock_audit_lock_file
#endif
  );

  mysql_cond_init(key_cond_audit_flush_sleep, &flush_thread_sleep_cond);
  mysql_mutex_init(key_mutex_audit_flush_sleep, &flush_thread_sleep_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_cond_audit_buf_full_sleep, &buf_full_sleep_cond);
  mysql_mutex_init(key_mutex_audit_buf_full_sleep, &buf_full_sleep_mutex,
                   MY_MUTEX_INIT_FAST);

  m_strategy = (enum_log_strategy)log_strategy;
  m_policy = (enum_log_policy)log_policy;
  m_conn_policy = (enum_log_connection_policy)log_conn_policy;
  m_stmt_policy = (enum_log_statement_policy)log_stmt_policy;
  m_version = (enum_log_version)log_version;

  /* Open audit log file if necessary. */
  set_enable(enable);
}

/* Destructor */
MYSQL_RDS_AUDIT_LOG::~MYSQL_RDS_AUDIT_LOG() {
  delete m_serializer;
  m_serializer = NULL;

  if (log_fd != -1) mysql_file_close(log_fd, MYF(0));

  LOCK_log.destroy();
  LOCK_file.destroy();
  mysql_cond_destroy(&flush_thread_sleep_cond);
  mysql_mutex_destroy(&flush_thread_sleep_mutex);
  mysql_cond_destroy(&buf_full_sleep_cond);
  mysql_mutex_destroy(&buf_full_sleep_mutex);
  my_free(log_buf);
  log_buf = NULL;
}

/* Open new audit log file. */
void MYSQL_RDS_AUDIT_LOG::rotate_log_file_low() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::rotate_log_file_low");

  File old_log_fd = -1;
  File new_log_fd = -1;
  char new_log_name[FN_REFLEN];
  uint len = 0;

  /* Open the new log file, no need to hold LOCK_file */
  len = snprintf(new_log_name, sizeof(new_log_name), "%s/%lu_%d.alog",
                 log_dir ? log_dir : mysql_tmpdir, time(0), m_file_id++);

  if (unlikely(len >= sizeof(new_log_name))) {
    len = sizeof(new_log_name) - 1;
    new_log_name[len] = '\0';
  }

  new_log_fd = mysql_file_open(PSI_NOT_INSTRUMENTED, new_log_name,
                               O_CREAT | O_WRONLY | O_EXCL, MYF(0));

  if (new_log_fd == -1) {
    char errbuf[MYSQL_ERRMSG_SIZE];
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_CREATE_FILE, new_log_name,
                 my_errno(),
                 my_strerror(errbuf, MYSQL_ERRMSG_SIZE, my_errno()));
    DBUG_VOID_RETURN;
  }

  /*
    Change log_fd, curr_file_pos, row_num and log_name while holding LOCK_file.

    The reason for LOCK_file lock is:
    1. log_fd, apparently we don't want write to invalid fd
    2. curr_file_pos, we can't use previous log file's curr_file_pos for newly
       created log file, imagine this:
       a) writting thread (user or background flush) get the new fd, but with
          old curr_file_pos, the record will be written to a pretty far offset
       b) reset curr_file_pos back to 0 by rotating
       c) afterwards writting request will begin from 0, and eventually
          overwrite record written in a)
    3. row_num, same like 3, row_num is used to check whether row_limit is
    reached, we don't want get a false negative result and skip records.

    4. there could be concurrent log rotating, and we should keep log_name
    consistent with being-writting fd.
  */
  LOCK_file.wrlock();

  old_log_fd = log_fd;
  log_fd = new_log_fd;
  curr_file_pos.store(0);
  last_row_num = row_num.load();
  row_num.store(0);
  memcpy(log_name, new_log_name, len + 1);

  LOCK_file.wrunlock();

  /* Print message to indicate timestamp. */
  if (lost_row_num_by_buf_full.load()) {
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_END_LOST_LOG);
    lost_row_num_by_buf_full.store(0);
  }

  lost_row_num_by_row_limit.store(0);

  /* Close the old log file */
  if (old_log_fd != -1) {
    mysql_file_close(old_log_fd, MYF(0));
  }

  DBUG_VOID_RETURN;
}

/*
  Reset information when rotating log file or turning on/off
  audit log recording.
*/
void MYSQL_RDS_AUDIT_LOG::reset_file_info() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::reset_file_info");

  LOCK_file.wrlock();

  if (log_fd != -1) {
    mysql_file_close(log_fd, MYF(0));
    log_fd = -1;
  }

  last_row_num = row_num.load();
  row_num.store(0);
  memset(log_name, 0, FN_REFLEN);
  curr_file_pos.store(0);

  LOCK_file.wrunlock();

  /* Print message to indicate timestamp. */
  if (lost_row_num_by_buf_full.load()) {
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_END_LOST_LOG);
    lost_row_num_by_buf_full.store(0);
  }
  lost_row_num_by_row_limit.store(0);

  DBUG_VOID_RETURN;
}

/* Reset information when changing audit log buffer size. */
void MYSQL_RDS_AUDIT_LOG::reset_offset_info() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::reset_offset_info");

  buffered_offset.store(0);
  flushed_offset.store(0);
  buf_full = false;
  last_flush_len = 0;

  DBUG_VOID_RETURN;
}

/*
  Rotate to a new audit log file
*/
void MYSQL_RDS_AUDIT_LOG::rotate_log_file() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::rotate_log_file");

  /*
    As rotate log is a relative heavy operation, for exmaple could become
    slow under busy IO device, so we release the global sys var mutex
    here and reacquire it before returning.
  */
  mysql_mutex_unlock(&LOCK_global_system_variables);

  /*
    Make rotate operation mutually exclusive with other maintain operation,
    but not mutually excluesive with user writting thread in buffered-write
    mode.
  */
  LOCK_log.rdlock(0);

  if (!m_enabled) goto finish;

  /*
    No flushing operation is needed as we just rotate log file,
    unflushed logs will be flushed to next log file.
  */
  rotate_log_file_low();

finish:
  LOCK_log.rdunlock(0);

  mysql_mutex_lock(&LOCK_global_system_variables);

  DBUG_VOID_RETURN;
}

/* Realloc the log_buf */
void MYSQL_RDS_AUDIT_LOG::resize_log_buf_low(ulong new_buf_size) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::resize_log_buf_low");

  LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_RESIZE_LOG_BUF,
               log_buf_size / 1024 / 1024, new_buf_size / 1024 / 1024);

  log_buf_size = new_buf_size;

  log_buf = (uchar *)my_realloc(key_memory_audit_log_buf, log_buf, log_buf_size,
                                MYF(MY_FAE));
  /* my_realloc do not support MY_ZEROFILL. */
  memset(log_buf, 0, log_buf_size);

  DBUG_VOID_RETURN;
}

/*
  Change audit log buffer size dynamically.
  As we hold exclusive lock here, all DDL/DML will be blocked.
  QPS/TPS will drop to zero sometimes. Please do not change
  it under heavy load.
*/
void MYSQL_RDS_AUDIT_LOG::resize_log_buf(ulong new_buf_size) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::resize_log_buf");

  LOCK_log.wrlock();

  if (log_buf_size == new_buf_size) {
    LOCK_log.wrunlock();
    DBUG_VOID_RETURN;
  }

  /* Make sure all logs have been flushed to disk. */
  flush_log_buf(true);
  assert(flushed_offset.load() == buffered_offset.load());

  reset_offset_info();
  /* It is safe to clear log buffer now. */
  resize_log_buf_low(new_buf_size);

  LOCK_log.wrunlock();

  DBUG_VOID_RETURN;
}

/*
  Switch audit log recording according to variables
  'rds_audit_log_enabled'.
*/
void MYSQL_RDS_AUDIT_LOG::set_enable(bool enable) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::set_enable");

  LOCK_log.wrlock();

  /* Return if current state is expected */
  if (m_enabled == enable) goto finish;

  m_enabled = enable;

  if (enable) {
    assert(log_fd == -1);
    /* Audit log has not been turned on, open it now. */
    rotate_log_file_low();
  } else {
    assert(log_fd != -1);

    /*
      We should make sure all audit logs have been flushed to
      disk before turning off.
      This will cost some times under heavy workload, but it will
      not affect front thread as we have disabled audit log recording
      now (m_enabled is false).
    */
    flush_log_buf(true);
    assert(flushed_offset.load() == buffered_offset.load());
    reset_file_info();
  }

  LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_ENABLE_OR_DISABLE,
               enable ? "enabled" : "disabled");

finish:
  LOCK_log.wrunlock();

  DBUG_VOID_RETURN;
}

extern const char *log_strategy_names[];
/*
  Change log strategy. log_buf will be forced flushint to log file, if
  change from buffred write to unbuffered.
*/
void MYSQL_RDS_AUDIT_LOG::set_log_strategy(enum_log_strategy strategy) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::set_log_strategy");

  LOCK_log.wrlock();

  if (m_strategy == strategy) {
    LOCK_log.wrunlock();
    DBUG_VOID_RETURN;
  }

  if (!is_buffered_write(m_strategy) && is_buffered_write(strategy)) {
    /* Turn on buffered write. */
    assert(flushed_offset.load() == buffered_offset.load());
  } else if (is_buffered_write(m_strategy) && !is_buffered_write(strategy)) {
    /* Turn off buffered write. */

    /* Make sure all logs have been flushed to disk. */
    flush_log_buf(true);
    assert(flushed_offset.load() == buffered_offset.load());
  }

  /*
    Signal all potentail user threads, which are waitting for space in
    log_buf, that we change strategy.
  */
  if (m_strategy == ASYNCHRONOUS) {
    wakeup_user();
  }

  LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_SWITCH_LOG_STRATEGY,
               log_strategy_names[m_strategy], log_strategy_names[strategy]);

  m_strategy = strategy;

  LOCK_log.wrunlock();

  DBUG_VOID_RETURN;
}

extern const char *log_version_names[];
/** Change log version. */
void MYSQL_RDS_AUDIT_LOG::set_log_version(enum_log_version version) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::set_log_version");

  LOCK_log.wrlock();
  LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_SWITCH_LOG_VERSION,
               log_version_names[m_version], log_version_names[version]);

  if (m_enabled) {
    if (is_buffered_write(m_strategy)) {
      /* Make sure all logs have been flushed to disk. */
      flush_log_buf(true);
      assert(flushed_offset.load() == buffered_offset.load());
    }

    /* Rotate to a new log file.  */
    rotate_log_file_low();
  }

  m_version = version;

  LOCK_log.wrunlock();

  DBUG_VOID_RETURN;
}

extern const char *log_policy_names[];
extern const char *log_connection_policy_names[];
extern const char *log_statement_policy_names[];
/* Change log policy. */
void MYSQL_RDS_AUDIT_LOG::set_log_policy(enum_log_policy policy) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::set_log_policy");

  LogPluginErr(
      INFORMATION_LEVEL, ER_AUDIT_LOG_SWITCH_LOG_POLICY,
      log_policy_names[m_policy], log_connection_policy_names[m_conn_policy],
      log_statement_policy_names[m_stmt_policy], log_policy_names[policy],
      log_connection_policy_names[m_conn_policy],
      log_statement_policy_names[m_stmt_policy]);

  m_policy = policy;
  DBUG_VOID_RETURN;
}

/* Change log format */
void MYSQL_RDS_AUDIT_LOG::set_log_format(
    enum_log_format format MY_ATTRIBUTE((unused))) {
  // TODO: support JSON format and log format switch
}

/* Change log connection policy. */
void MYSQL_RDS_AUDIT_LOG::set_log_connection_policy(
    enum_log_connection_policy conn_policy) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::set_log_connection_policy");

  LogPluginErr(
      INFORMATION_LEVEL, ER_AUDIT_LOG_SWITCH_LOG_POLICY,
      log_policy_names[m_policy], log_connection_policy_names[m_conn_policy],
      log_statement_policy_names[m_stmt_policy], log_policy_names[m_policy],
      log_connection_policy_names[conn_policy],
      log_statement_policy_names[m_stmt_policy]);

  m_conn_policy = conn_policy;
  DBUG_VOID_RETURN;
}

/* Change log statement policy. */
void MYSQL_RDS_AUDIT_LOG::set_log_statement_policy(
    enum_log_statement_policy stmt_policy) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::set_log_statement_policy");

  LogPluginErr(
      INFORMATION_LEVEL, ER_AUDIT_LOG_SWITCH_LOG_POLICY,
      log_policy_names[m_policy], log_connection_policy_names[m_conn_policy],
      log_statement_policy_names[m_stmt_policy], log_policy_names[m_policy],
      log_connection_policy_names[m_conn_policy],
      log_statement_policy_names[stmt_policy]);

  m_stmt_policy = stmt_policy;
  DBUG_VOID_RETURN;
}

/*
  Copy the audit log to log_buf.

  @param[IN] curr_buf_offset copy log from this offset
  @param[IN] log             audit log content
  @param[IN] log_len         audit log length
*/
void MYSQL_RDS_AUDIT_LOG::copy_to_buf(my_off_t curr_buf_offset, const char *log,
                                      uint log_len) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::copy_to_buf");

  my_off_t buf_pos = curr_buf_offset % log_buf_size;

  /* Rotate to log_buf head */
  if (unlikely(buf_pos + log_len > log_buf_size)) {
    uint first_part_len = log_buf_size - buf_pos;
    memcpy(log_buf + buf_pos, log, first_part_len);
    memcpy(log_buf, log + first_part_len, log_len - first_part_len);
  } else {
    memcpy(log_buf + buf_pos, log, log_len);
  }

  DBUG_VOID_RETURN;
}

/*
  Print message to error log if some audit logs will lost duing
  to log_buf full.
*/
void MYSQL_RDS_AUDIT_LOG::buf_full_print_error() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::buf_full_print_error");
  /* Do not print too many warnings to error log. */
  if (unlikely(lost_row_num_by_buf_full++ == 0)) {
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_BEGIN_LOST_LOG);
  }
  lost_row_num_by_buf_full_total++;

  DBUG_VOID_RETURN;
}

/*
  We have two choices when audit log buffer is full:
  a) Discard the following audit logs until audit log buffer has free space.
  b) Block front/user thread until audit log buffer has free space.
  It depends on the employed strategy.

  LOCK_log READ lock is released when return true.

  @param[IN] thread_id       thread on which to release lock
  @param[IN] len             length of current log record str
  @returns false if we just discard audit log.
*/
bool MYSQL_RDS_AUDIT_LOG::buf_full_handle(uint thread_id, ulong len) {
  bool retry = false;

  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::buf_full_handle");

  wakeup_flush_thread();

  assert(is_buffered_write(m_strategy));

  if (m_strategy == ASYNCHRONOUS) {
    /*
      In this case, all audit logs should be recorded,
      so just wait and check flag periodically.
      Note, we should release share lock first.
    */
    LOCK_log.rdunlock(thread_id);
    log_write_waits++;
    buf_full_cond_wait(RDS_AUDIT_BUF_FULL_WAIT_TIME);
    retry = true;
  } else {
    if (log_event_max_drop_size < len) {
      log_event_max_drop_size = len;
    }
    log_events_lost++;
    buf_full_print_error();
    retry = false;
  }

  DBUG_RETURN(retry);
}

/*
  Audit log will write to log file directly without cache.

  @param[IN] log_str    serialized log records string
  @param[IN] len        length of log records
  @param[IN] tid        thread id on which to release lock
  @returns always return false
*/
bool MYSQL_RDS_AUDIT_LOG::write_to_file(const char *log_str, ulong len,
                                        ulong tid) {
  my_off_t file_pos = 0;
  my_off_t plus_file_pos = 0;

  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::write_to_file");

  /* Make sure log_fd is safe */
  LOCK_file.rdlock(tid);

inc_file_pos:
  file_pos = curr_file_pos.load();
  plus_file_pos = file_pos + len;

  /*
    In doc, compare_exchange_weak may be a little faster than
    compare_exchange_strong. But the drawback is that sometimes
    it will return false even when curr_file_pos equals to
    file_pos. Fortunately, our algorithm tolerate this
    problem.
  */
  if (!curr_file_pos.compare_exchange_weak(file_pos, plus_file_pos))
    goto inc_file_pos;

  assert(!is_buffered_write(m_strategy));

  /* Write to audit log directly without cache to memory first. */
  if (unlikely(pwrite_batch((const uchar *)log_str, len, file_pos,
                            m_strategy == SYNCHRONOUS ? true : false))) {
    char errbuf[MYSQL_ERRMSG_SIZE];
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_WRITE_TO_FILE, len,
                 log_name, file_pos, my_errno(),
                 my_strerror(errbuf, MYSQL_ERRMSG_SIZE, my_errno()));
  }

  LOCK_file.rdunlock(tid);

  log_total_size += len;
  log_events_written++;

  DBUG_RETURN(false);
}

/*
  Audit log will cache to memory
  first and write to log file by background thread
  periodically.

  @param[IN] log_str    serialized log records string
  @param[IN] len        length of log records
  @param[IN] tid        thread id on which to release lock
  @returns true if we have to retry.
*/
bool MYSQL_RDS_AUDIT_LOG::write_to_buf(const char *log_str, ulong len,
                                       ulong tid) {
  my_off_t curr_buf_offset = 0;
  my_off_t plus_buf_offset = 0;
  my_off_t curr_flu_offset = 0;

  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::write_to_buf");

retry:
  /*
    It's possible that m_strategy is changed while waiting in
    buf_full_handle() with LOCK_log released.
  */
  if (!is_buffered_write(m_strategy)) {
    /* The caller will retry with with other strategy on true return */
    DBUG_RETURN(true);
  }

  /* Try to make sure there is enough room in log_buf. */
inc_buf_offset:
  if (likely(!buf_full.load())) {
    curr_buf_offset = buffered_offset.load();
    plus_buf_offset = curr_buf_offset + len;
    curr_flu_offset = flushed_offset.load();

    if (unlikely(plus_buf_offset - curr_flu_offset > log_buf_size)) {
      /* log_buf has full */
      buf_full.store(true);

      if (buf_full_handle(tid, len)) {
        /* buf_full_handle() release LOCK_log when return true */
        LOCK_log.rdlock(tid);
        goto retry;
      } else {
        goto finish;
      }
    }

    /*
      In doc, compare_exchange_weak may be a little faster than
      compare_exchange_strong. But the drawback is that sometimes
      it will return false even when buffered_offset equals to
      curr_buf_offset. Fortunately, our algorithm tolerate this
      problem.
    */
    if (!buffered_offset.compare_exchange_weak(curr_buf_offset,
                                               plus_buf_offset))
      goto inc_buf_offset;

    assert(is_buffered_write(m_strategy));
    /* Enough room has been reserved in log_buf, we start copying. */
    copy_to_buf(curr_buf_offset, log_str, len);
    /*
      Note, it may be not accurate to accumalate here, the log is only
      copied to buffer, but the log flush thread don't know the number
      of events in log buffer.
    */
    log_events_written++;

  } else {
    if (buf_full_handle(tid, len)) {
      /* buf_full_handle() release LOCK_log when return true */
      LOCK_log.rdlock(tid);
      goto retry;
    }
  }

finish:
  DBUG_RETURN(false);
}

/*
  Process audit log event, this is the entrance of audit log.

  @param[in] event_class    the type of event
  @param[in] event          a general pointer to the event
  @param[in] buf            the buffer used to store serialized event
  @param[in] buf_len        length of buffer
*/
void MYSQL_RDS_AUDIT_LOG::process_event(mysql_event_class_t event_class,
                                        const void *event, char *buf,
                                        uint buf_len) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::process_event");
  uint serialized_len = 0;
  ulong thread_id = 0;
  bool skipped = false;

  /*
    Using dirty read.
    When m_enabled is changed from true to false, log_buf need to totally
    flushed to log file, this could take some time. If user threads request
    LOCK_log READ lock here, they will be blocked for writting , and
    eventually find it don't need write. It' better to just return with
    dirty read.

    We will check agagin with value of m_enabled in write_log() with
    LOCK_log READ lock being held, so it's OK for false positive result.

    For false negative result, we could potentially lose some records, we just
    tolerate this.
  */
  if (!m_enabled) {
    DBUG_VOID_RETURN;
  }

  log_events++;

  if (m_policy == LOG_NONE) {
    skipped = true;
    goto skip_write_log;
  }

  switch (event_class) {
    case MYSQL_AUDIT_RDS_CONNECTION_CLASS: {
      if (m_policy == LOG_QUERIES) {
        skipped = true;
        goto skip_write_log;
      }

      if (m_conn_policy == CONN_NONE) {
        skipped = true;
        goto skip_write_log;
      }

      const struct mysql_event_rds_connection *event_rds_connection =
          (const struct mysql_event_rds_connection *)event;

      if (m_conn_policy == CONN_ERRORS &&
          event_rds_connection->error_code == 0) {
        skipped = true;
        goto skip_write_log;
      }

      thread_id = event_rds_connection->thread_id;

      switch (m_version) {
        case MYSQL_V1:
          serialized_len = m_serializer->serialize_connection_event_v1(
              event_rds_connection, buf, buf_len);
          break;
        case MYSQL_V3:
          serialized_len = m_serializer->serialize_connection_event_v3(
              event_rds_connection, buf, buf_len);
          break;
        default:
          assert(0);
      }

      break;
    }

    case MYSQL_AUDIT_RDS_QUERY_CLASS: {
      if (m_policy == LOG_LOGINS) {
        skipped = true;
        goto skip_write_log;
      }

      if (m_stmt_policy == STMT_NONE) {
        skipped = true;
        goto skip_write_log;
      }

      const struct mysql_event_rds_query *event_rds_query =
          (const struct mysql_event_rds_query *)event;

      if (m_stmt_policy == STMT_UPDATES &&
          !is_update_query(event_rds_query->sql_command)) {
        skipped = true;
        goto skip_write_log;
      } else if (m_stmt_policy == STMT_UPDATES_OR_ERRORS &&
                 !is_update_query(event_rds_query->sql_command) &&
                 event_rds_query->error_code == 0) {
        skipped = true;
        goto skip_write_log;
      } else if (m_stmt_policy == STMT_ERRORS &&
                 event_rds_query->error_code == 0) {
        skipped = true;
        goto skip_write_log;
      }

      thread_id = event_rds_query->thread_id;

      switch (m_version) {
        case MYSQL_V1:
          serialized_len = m_serializer->serialize_query_event_v1(
              event_rds_query, buf, buf_len);
          break;
        case MYSQL_V3:
          serialized_len = m_serializer->serialize_query_event_v3(
              event_rds_query, buf, buf_len);
          break;
        default:
          assert(0);
      }

      break;
    }

    default:
      /* impossible case, cause we don't subscribe other types of events */
      assert(0);
  }

  write_log(buf, serialized_len, thread_id);

skip_write_log:
  if (skipped) {
    log_events_filtered++;
  }
  DBUG_VOID_RETURN;
}

/*
  General function to write log records.

  @param[IN] log_str    serialized log records string
  @param[IN] len        length of log records
  @param[IN] tid        thread id on which to release lock
*/
void MYSQL_RDS_AUDIT_LOG::write_log(const char *log_str, ulong len, ulong tid) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::write_log");

  /* Always hold LOCK_log READ lock */
  LOCK_log.rdlock(tid);

  bool retry = false;

  /* Check again while holding LOCK_log READ lock */
  if (!m_enabled) goto skip_log;

  /* Note: row_num is not accurate for buffered write. On rotating, records
  remain in log_buf is not forced to be flushed in old file, but it't number
  always account in the old file. */

  if (unlikely(row_num++ >= row_limit)) {
    lost_row_num_by_row_limit++;
    lost_row_num_by_row_limit_total++;
    goto skip_log;
  }

  do {
    if (is_buffered_write(m_strategy)) {
      retry = write_to_buf(log_str, len, tid);
    } else {
      retry = write_to_file(log_str, len, tid);
    }
  } while (unlikely(retry));

skip_log:
  LOCK_log.rdunlock(tid);
  DBUG_VOID_RETURN;
}

/* Stop audit log flushing thread when server stops. */
void MYSQL_RDS_AUDIT_LOG::stop_flush_thread() {
  int max_wait_cnt = 0;
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::stop_flush_thread");
  assert(!is_aborted());

  if (!flush_thread_running) DBUG_VOID_RETURN;

  assert(flush_thread_running);
  /* Tell log flushing thread to exit loop. */
  set_abort();

  /*
    We must sleep enough time too make sure all audit logs
    have been flush to disk successfully.
  */
wait:
  my_sleep(RDS_AUDIT_STOP_THREAD_SLEEP_INTERVAL); /* 500ms */
  max_wait_cnt++;
  if (flush_thread_running && max_wait_cnt <= RDS_AUDIT_MAX_THREAD_WAIT_CNT)
    goto wait;

  if (!flush_thread_running)
    LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_DEINIT_FLUSH_THREAD);
  else
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_DEINIT_FLUSH_THREAD);

  DBUG_VOID_RETURN;
}

/* Start audit log flushing thread when server startups. */
bool MYSQL_RDS_AUDIT_LOG::create_flush_thread() {
  my_thread_handle th;
  int max_wait_cnt = 0;

  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::create_flush_thread");
  assert(!flush_thread_running);

  /* Create background thread to handle logs flushing task. */
  mysql_thread_create(key_thread_audit_flush, &th, &connection_attrib,
                      audit_log_flush_thread, NULL);

wait:
  my_sleep(RDS_AUDIT_CREATE_THREAD_SLEEP_INTERVAL); /* 200ms */
  max_wait_cnt++;

  if (flush_thread_running == false &&
      max_wait_cnt <= RDS_AUDIT_MAX_THREAD_WAIT_CNT)
    goto wait;

  if (flush_thread_running == false) {
    assert(max_wait_cnt > RDS_AUDIT_MAX_THREAD_WAIT_CNT);
    LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_INIT_FLUSH_THREAD);
    DBUG_RETURN(true);
  } else {
    LogPluginErr(INFORMATION_LEVEL, ER_AUDIT_LOG_INIT_FLUSH_THREAD);
  }

  DBUG_RETURN(false);
}

/*
  Wakeup the flushing thread sleeping condition.
  This is used when log_buf is full.
*/
void MYSQL_RDS_AUDIT_LOG::wakeup_flush_thread() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::wakeup_flush_thread");
  mysql_cond_signal(&flush_thread_sleep_cond);
  DBUG_VOID_RETURN;
}

/*
  Front/user thread will sleep until audit log buffer
  has enough room for current audit log when
  rds_audit_log_reserve_all turns on.

  @param[IN] millisecond    Maximum sleep time.
  @returns true if it is timed out
*/
bool MYSQL_RDS_AUDIT_LOG::buf_full_cond_wait(time_t millisecond) {
  int ret = 0;
  struct timespec abstime;

  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::buf_full_cond_wait");

  set_timespec_nsec(&abstime, millisecond * 1000000ULL);

  mysql_mutex_lock(&buf_full_sleep_mutex);
  ret = mysql_cond_timedwait(&buf_full_sleep_cond, &buf_full_sleep_mutex,
                             &abstime);
  mysql_mutex_unlock(&buf_full_sleep_mutex);

  DBUG_RETURN(ret == ETIMEDOUT);
}

/*
  Flushing thread sleep to wait until
  a) timed out b) signaled

  @param[IN] millisecond    Maximum sleep time.
  @returns true if it is timed out
*/
bool MYSQL_RDS_AUDIT_LOG::flush_sleep_cond_wait(time_t millisecond) {
  int ret = 0;
  struct timespec abstime;

  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::flush_sleep_cond_wait");

  set_timespec_nsec(&abstime, millisecond * 1000000ULL);

  mysql_mutex_lock(&flush_thread_sleep_mutex);
  ret = mysql_cond_timedwait(&flush_thread_sleep_cond,
                             &flush_thread_sleep_mutex, &abstime);
  mysql_mutex_unlock(&flush_thread_sleep_mutex);

  DBUG_RETURN(ret == ETIMEDOUT);
}

/*
  Wakeup the front/user thread when one flush batch has finished.
  This is used when log_buf has free space for new audit logs.
*/
void MYSQL_RDS_AUDIT_LOG::wakeup_user() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::wakeup_user");
  mysql_cond_broadcast(&buf_full_sleep_cond);
  DBUG_VOID_RETURN;
}

/*
  Audit log flushing thread should be sleep a moment between
  two log flushing batch.
*/
void MYSQL_RDS_AUDIT_LOG::flush_thread_sleep_if_need() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::flush_thread_sleep_if_need");

  /* We skip sleeping if server starts to shutdown. */
  if (unlikely(is_aborted())) DBUG_VOID_RETURN;

  ulong diff = RDS_AUDIT_MAX_FLUSH_SLEEP_TIME - RDS_AUDIT_MIN_FLUSH_SLEEP_TIME;
  ulong sleep_time = RDS_AUDIT_MAX_FLUSH_SLEEP_TIME;

  double ratio = (double)last_flush_len / log_buf_size;
  /*
    We should accelerate logs flushing under some situations:
    a) If log_buf_size is small, it is better to leave more space
    in log_buf to handle the loading peak.
    b) If log_buf_size is large, it is better to flush more logs
    if log generating is very fast.
  */
  if (ratio > RDS_AUDIT_FAST_FLUSH_RATIO_THRESHOLD ||
      last_flush_len > RDS_AUDIT_FAST_FLUSH_LEN_THRESHOLD)
    ratio = 1;
  assert(ratio >= 0 && ratio <= 1);

  sleep_time -= (ulong)(ratio * diff);
  flush_sleep_cond_wait(sleep_time);
  last_sleep_time = sleep_time;

  DBUG_VOID_RETURN;
}

/*
  Do one batch flushing.

  @param[IN] pwrite_buf     flush log from this buf
  @param[IN] pwrite_len     number of logs to be flush
  @param[IN] pwrite_offset  flush to file from this offset
  @param[IN] sync_to_disk   whether to flush log file
  @returns true if error occurs during flushing
*/
bool MYSQL_RDS_AUDIT_LOG::pwrite_batch(const uchar *pwrite_buf,
                                       my_off_t pwrite_len,
                                       my_off_t pwrite_offset,
                                       bool sync_to_disk) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::pwrite_batch");

  my_off_t already_written = 0;

  /* In most case, following loop will execute only once. */
  do {
    already_written += mysql_file_pwrite(
        log_fd, pwrite_buf + already_written, pwrite_len - already_written,
        pwrite_offset + already_written, MYF(0));
    log_file_writes++;
  } while (unlikely(already_written < pwrite_len));

  /* Currently, we only flush if we use write-to-memory algorithm.
  Because it will hurt performance too much in write-to-file algorithm. */
  if (sync_to_disk) {
    log_file_syncs++;
    mysql_file_sync(log_fd, MYF(0));
  }

  DBUG_RETURN(already_written != pwrite_len);
}

/*
  Flush the audit log from memory to disk.

  @param[IN] curr_flu_offset flush log from this offset
  @param[IN] curr_buf_offset flush log up to this offset
*/
void MYSQL_RDS_AUDIT_LOG::flush_from_buf(my_off_t curr_flu_offset,
                                         my_off_t curr_buf_offset) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::flush_from_buf");

  my_off_t flu_pos = curr_flu_offset % log_buf_size;
  my_off_t buf_pos = curr_buf_offset % log_buf_size;
  my_off_t file_pos = curr_file_pos.load();
  my_off_t len = curr_buf_offset - curr_flu_offset;
  last_flush_len = len;

  /*
    Flush two part separately:
    a) flu_pos to log_buf end
    b) log_buf head to buf_pos.
  */
  if (unlikely(flu_pos >= buf_pos)) {
    ulonglong first_part_len = log_buf_size - flu_pos;

    if (unlikely(
            pwrite_batch(log_buf + flu_pos, first_part_len, file_pos, true))) {
      char errbuf[MYSQL_ERRMSG_SIZE];
      LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_WRITE_TO_FILE,
                   first_part_len, log_name, file_pos, my_errno(),
                   my_strerror(errbuf, MYSQL_ERRMSG_SIZE, my_errno()));
    }
    /*
      if flu_pos == 0 && buf_pos == log_buf + log_buf_size, buf_pos will be
      zero, we must handle this case.
    */
    if (likely(buf_pos) &&
        unlikely(
            pwrite_batch(log_buf, buf_pos, file_pos + first_part_len, true))) {
      char errbuf[MYSQL_ERRMSG_SIZE];
      LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_WRITE_TO_FILE, buf_pos,
                   log_name, file_pos + first_part_len, my_errno(),
                   my_strerror(errbuf, MYSQL_ERRMSG_SIZE, my_errno()));
    }

  } else {
    if (unlikely(pwrite_batch(log_buf + flu_pos, len, file_pos, true))) {
      char errbuf[MYSQL_ERRMSG_SIZE];
      LogPluginErr(ERROR_LEVEL, ER_AUDIT_LOG_FAILED_TO_WRITE_TO_FILE, len,
                   log_name, file_pos, my_errno(),
                   my_strerror(errbuf, MYSQL_ERRMSG_SIZE, my_errno()));
    }
  }

  curr_file_pos.store(file_pos + len);
  log_total_size += len;

  DBUG_VOID_RETURN;
}

/*
  Flush limited audit logs to disk in one flush batch.

  @param[IN] curr_flu_offset flush log from this offset
  @param[IN] curr_buf_offset flush log up to this offset
*/
void MYSQL_RDS_AUDIT_LOG::flow_control(my_off_t curr_flu_offset,
                                       my_off_t &curr_buf_offset) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::flow_control");

  my_off_t len = curr_buf_offset - curr_flu_offset;

  assert(len > 0);

  /* In one flush batch, we write at most RDS_AUDIT_FLUSH_MAX_LEN bytes. */
  if (likely(len <= RDS_AUDIT_FLUSH_MAX_LEN)) DBUG_VOID_RETURN;

  /* If we reach here, it means we have to do flow control. */
  curr_buf_offset = curr_flu_offset + RDS_AUDIT_FLUSH_MAX_LEN;
  my_off_t buf_pos = curr_buf_offset % log_buf_size;

  char pre_char = ' ';
  int tail_cnt = -1;

  /* Loop to find uncompleted log bound. */
  while (true) {
    if (unlikely(buf_pos == 0))
      buf_pos = log_buf_size - 1;
    else
      buf_pos--;

    if (log_buf[buf_pos] == '\1' && pre_char == '\n') break;

    tail_cnt++;
    pre_char = log_buf[buf_pos];

    /* It will not enter this code branch in normal case. */
    if (unlikely(tail_cnt >= RDS_AUDIT_FLUSH_MAX_LEN)) {
      LogPluginErr(WARNING_LEVEL, ER_AUDIT_LOG_AUDIT_LOG_TOO_LONG);
      tail_cnt = 0;
      break;
    }
  }

  curr_buf_offset -= tail_cnt;

  assert(curr_buf_offset > curr_flu_offset);

  DBUG_VOID_RETURN;
}

/*
  Flush audit logs from memory to disk.

  @param[in] flush_all true if we want to flush all logs.
*/
void MYSQL_RDS_AUDIT_LOG::flush_log_buf(bool flush_all) {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::flush_log_buf");

  my_off_t curr_buf_offset;
  my_off_t curr_flu_offset;

  if (flush_all) {
    curr_buf_offset = buffered_offset.load();
    curr_flu_offset = flushed_offset.load();
  } else {
    /*
      Use LOCK_log READ lock to make curr_buf_offset and curr_flush_offset
      safe from reseting by resize_log_buf().
    */
    LOCK_log.rdlock(0);
    curr_buf_offset = buffered_offset.load();
    curr_flu_offset = flushed_offset.load();
    LOCK_log.rdunlock(0);
  }

  /* resize_log_buf() may happen now when flush_all is false */

  /*
    No need to obtain lock if we want to flush all audit
    logs as the caller will hold it.
  */
  if (!flush_all) {
    /*
      Make sure all audit logs before curr_buf_offset have been copied to
      audit log buffer successfully.

      TODO
      This is a performance drop point, and make code logic become obscure,
      we should find a better way to get the buffered_offset, before which
      all audit log have been copied to log_buf.
    */
    LOCK_log.wrlock_and_unlock();
    /*
      We must do serializable opeartion between:
      a) resize_log_buf()
      b) set_enable()
      c) set_log_strategy()
    */
    LOCK_log.rdlock(0);
  }

  /* Make sure log_fd is safe */
  LOCK_file.rdlock(0);

  /*
    We don't use m_enabled here, because
    1. when turn off audit log, flush_log_buf(true) will be invoked,
       and m_enabled is set before this invoking.
    2. m_enabled is relative higher level switch, when flushing log_buf to
       file, we should only consider whether there is a valid fd to write to.
  */
  if (log_fd == -1) goto finish;

  /*
    This means we are using write-to-file algorithm.
    No need to flush from background thread.
  */
  if (!is_buffered_write(m_strategy)) goto finish;

  /*
    This means we have reset audit log buffer size.

    FIXME
    It cloud happens that curr_flu_offset == 0 && flushed_offset.load() == 0,
    in such case, we don't know whether flushed_offset has been reset many
    times throughresize_log_buf(). In such case the log_buf will be flush to
    log file multiple times. This rarely happens, we tolerate this.
  */
  if (!flush_all && curr_flu_offset > flushed_offset.load()) {
    /*
      flushed_offset will be reset to zero if audit log buffer size
      changes. And only advanced by us, so it's reasonable to put
      such assertion.
    */
    assert(flushed_offset.load() == 0);
    goto finish;
  }

  /* Nothing to flush */
  if (curr_buf_offset == curr_flu_offset) goto finish;

  assert(curr_buf_offset > curr_flu_offset);

  /*
    This is flow controlling as we do not want to flush too
    many audit logs in one flushing batch.
    However, we try to flush all logs when current log file is to
    be closed or the log buffer will be reset later.
  */
  if (!flush_all) flow_control(curr_flu_offset, curr_buf_offset);

  /* It is safe to flush logs to disk now. */
  flush_from_buf(curr_flu_offset, curr_buf_offset);

  /* Advance the offset and we have room to store more audit logs now. */
  flushed_offset.store(curr_buf_offset);
  buf_full.store(false);

  if (m_strategy == ASYNCHRONOUS) wakeup_user();

finish:
  LOCK_file.rdunlock(0);

  if (!flush_all) {
    LOCK_log.rdunlock(0);
  }

  DBUG_VOID_RETURN;
}

/*
  Whether the flushing thread should exit when server shutdown.

  @returns true if abort flag is set and all audit logs
  have been flushed successfully
*/
bool MYSQL_RDS_AUDIT_LOG::flush_thread_should_exit() {
  DBUG_ENTER("MYSQL_RDS_AUDIT_LOG::flush_thread_should_exit");

  /* Only when server shutdown, abort flag will be set. */
  if (!is_aborted()) DBUG_RETURN(false);

  /* Do one more flush */
  LOCK_log.wrlock();
  flush_log_buf(true);
  LOCK_log.wrunlock();
  assert(flushed_offset.load() == buffered_offset.load());

  DBUG_RETURN(true);
}

/*
  Background audit log flushing thread which is created when server startups
  and destoryed when server stops.
*/
extern "C" void *audit_log_flush_thread(void *arg MY_ATTRIBUTE((unused))) {
  my_thread_init();

  rds_audit_log->set_flush_thread_status(true);

  while (!rds_audit_log->flush_thread_should_exit()) {
    rds_audit_log->flush_thread_sleep_if_need();
    rds_audit_log->flush_log_buf(false);
  }

  rds_audit_log->set_flush_thread_status(false);

  my_thread_end();
  pthread_exit(0);
}
