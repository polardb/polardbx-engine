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

#ifndef AUDIT_LOG_H
#define AUDIT_LOG_H

#include <mysql/plugin_audit.h>
#include <atomic>
#include "serializer.h"
#include "sql/auth/partitioned_rwlock.h"

/*
  Audit log total_length - sql_length, i.e. the sum of all other fields'
  length in audit log.
  Note: If new field is add to audit log, please remember to update this.
*/
#define RDS_AUDIT_LOG_EXTRA_SIZE 659

/*
  The maximum loops we will wait before aborting when
  starting/stopping audit log flushing thread.
*/
#define RDS_AUDIT_MAX_THREAD_WAIT_CNT 20

/*
  The sleep time(microsecond) in each loop when starting audit
  log flushing thread.
*/
#define RDS_AUDIT_CREATE_THREAD_SLEEP_INTERVAL 200000

/*
  The sleep time(microsecond) in each loop when stopping audit
  log flushing thread. This value should be large enough so
  that all buffered audit logs can be flushed to disk before
  server shutdown.
*/
#define RDS_AUDIT_STOP_THREAD_SLEEP_INTERVAL 500000

/* RW lock partition number for reducing the contention. */
#define RDS_AUDIT_LOG_LOCK_PARTITIONS 32

/*
  The maximum time(millisecond) flushing thread will sleep
  before starting one flushing batch.
*/
#define RDS_AUDIT_MAX_FLUSH_SLEEP_TIME 50

/*
  The minimal time(millisecond) flushing thread will sleep
  before starting one flushng batch. We do not want to put
  so much pressure on disk even when lots of audit logs need
  to be flushed so that this value is not set to zero.
*/
#define RDS_AUDIT_MIN_FLUSH_SLEEP_TIME 10

#if RDS_AUDIT_MAX_FLUSH_SLEEP_TIME < RDS_AUDIT_MIN_FLUSH_SLEEP_TIME
#error RDS_AUDIT_MAX_FLUSH_SLEEP_TIME           \
  smaller than RDS_AUDIT_MIN_FLUSH_SLEEP_TIME
#endif

/*
  Audit log flushing thread will sleep the minimal
  time(RDS_AUDIT_MIN_FLUSH_SLEEP_TIME) if last flushing batch
  flush more than this ratio(last_flush_len/log_buf_size).
  It will help to flush more logs if rds_audit_log_buffer_size
  is very small.
*/
#define RDS_AUDIT_FAST_FLUSH_RATIO_THRESHOLD 0.5

/*
  Audit log flushing thread will sleep the minimal
  time(RDS_AUDIT_MIN_FLUSH_SLEEP_TIME) if last flushing batch
  flush more than this many bytes.
  It will help to flush more logs if log generating is very fast.
*/
#define RDS_AUDIT_FAST_FLUSH_LEN_THRESHOLD (100 * 1024 * 1024ULL)

/*
  The maximum bytes flushing thread can flush in one flushing batch.
  It is used to release disk pressure under heavy load.
*/
#define RDS_AUDIT_FLUSH_MAX_LEN (500 * 1024 * 1024LL)

/*
  The maximum time front/user thread will sleep before rechecking
  audit log buffer status when audit log buffer is full. Note, it
  is only valid when rds_audit_log_reserve_all turns on.
*/
#define RDS_AUDIT_BUF_FULL_WAIT_TIME 100

/*
  RDS audit log, different from query_log and slow_log
  we only support file format here.
*/
class MYSQL_RDS_AUDIT_LOG {
 public:
  /*
    The audit log format:
    PALIN, compatible with RDS MySQL 5.6 MYSQL_V1 format
    JSON, JSON format
  */
  enum enum_log_format { PLAIN, JSON };

  /* PolorDB 8.0 supports MYSQL_V2, we don't */
  enum enum_log_version { MYSQL_V1, MYSQL_V3 };

  /*
    The strategy used when write to audit log file.
    ASYNCHRONOUS:    Log asynchronously. Wait for space in the output buffer.
    PERFORMANCE:     Log asynchronously. Drop requests for which there is
                     insufficient space in the output buffer.
    SEMISYNCHRONOUS: Log synchronously. Permit caching by the operating system.
    SYNCHRONOUS:     Log synchronously. Call sync() after each request.
  */
  enum enum_log_strategy {
    ASYNCHRONOUS,
    PERFORMANCE,
    SEMISYNCHRONOUS,
    SYNCHRONOUS
  };

  /*
    The policy controls which type of events is written to log file.
    LOG_ALL     :    Log all events
    LOG_LOGINS  :    Log only login events (login and logout)
    LOG_QUERIES :    Log only query events
    LOG_NONE    :    Log nothing (disable the audit stream)
  */
  enum enum_log_policy { LOG_ALL, LOG_LOGINS, LOG_QUERIES, LOG_NONE };

  /*
  This policy controls how connection event is written to log file
  CONN_ALL    : Log all connection events
  CONN_ERRORS : Log only failed connection events
  CONN_NONE   : Do not log connection events
*/
  enum enum_log_connection_policy { CONN_ALL, CONN_ERRORS, CONN_NONE };

  /*
  This policy controls how query event is written to log file
  STMT_ALL               : Log all statement events
  STMT_UPDATES           : Log only update statement events
  STMT_UPDATES_OR_ERRORS : Log only update or failed statement events
  STMT_ERRORS            : Log only failed statement events
  STMT_NONE              : Do not log statement events
*/
  enum enum_log_statement_policy {
    STMT_ALL,
    STMT_UPDATES,
    STMT_UPDATES_OR_ERRORS,
    STMT_ERRORS,
    STMT_NONE,
  };

 private:
  /*
    Whether the whole audit log feature is enabled or not. Protected
    by LOCK_log
  */
  bool m_enabled;

  /*
    The directory where audit log files reside in. This is a read only
    member, initialized along with audit log initialization. */
  const char *log_dir;

  /*
    The current log format. Protected by LOCK_log, should support
    dynamically change when JSON format is added.
  */
  enum_log_format m_format;

  /* The log version */
  enum_log_version m_version;

  /* The log write strategy. Protected by LOCK_log. */
  enum_log_strategy m_strategy;

  /* The log policy */
  enum_log_policy m_policy;

  /* The log connection policy */
  enum_log_connection_policy m_conn_policy;

  /* The log statement policy */
  enum_log_statement_policy m_stmt_policy;

  /*
    Auto_increment id which is part of audit log filename.
    It increases every flushing new audit log file and
    be reset when restarting.
  */
  int m_file_id;

  /*
    Audit log file path which contains:
    a. audit log file directory
    b. audit log file name:
    1): current time in unix timestamp format
    2): Auto increased id: m_file_id
  */
  char log_name[FN_REFLEN];

  /*
    Description of current audit log file.
    It will be reset to new handler when switches to new audit log file.

    Protected by LOCK_file.
  */
  File log_fd;

  /*
    Use to serialize audit event to string, there is one serializer
    corresponding to each log format.

    Proteced by LOCK_log.
  */
  Serializer *m_serializer;

  /*
    File offset used by pwrite system call. It will be reset when starting
    to write new audit log file.

    Protected by LOCK_file
    Atomic operation is not enough, because this member is bound with
    log_fd, and log_fd can be changed by rotation. So a lock is needed
    to keep log_fd and curr_file_pos consistent, we can tolerate user
    threads get curr_file_pos for previous log file, and log_fd for
    current log file.
  */
  std::atomic<my_off_t> curr_file_pos;

  /*
    The logical size the audit logs that has been already flushed to audit
    log file. This member is updated by log flush thread.

    It is increased after one successful flushing batch, by log flush
    thread. It is reset to zero, only when log_buf is resized by maintain
    operation.
  */
  std::atomic<my_off_t> flushed_offset;

  /*
    The logical size of the audit logs that starting buffering to log_buf.
    This member is updated by user threads.

    Note, it doesn't guarantee all audit logs have been already copied
    to log_buf by this position.

    It will be increased after calcuating audit log len and checking enough
    space in log_buf. It will reset to zero, only when log_buf is resized by
    maintain operation.

    Theoretically, flushed_offset will always smaller or equal
    than flushed_offset.

    Both flushed_offset and buffered_offset are bound to log_buf.
  */
  std::atomic<my_off_t> buffered_offset;

  /*
    Audit log buffer in memory which will buffer unflushed audit logs
    inorder to improve performance under heavy load.
    It will be allocated during initialition of class mysql_rds_audit_log and
    will be rellocated if lob_buf_size has been changed.

    Protected by LOCK_log
  */
  uchar *log_buf;

  /*
    log_buf size which is controlled by variable rds_audit_log_buffer_size.
    We can change it dynamically.
  */
  ulonglong log_buf_size;

  /*
    Indicate if the log_buf is full of unflushed audit log. If this value
    is set to nonzero value, all audit logs afterward will be lost. It will
    be reset to zero if one flushing batch has finished.
  */
  std::atomic<uchar> buf_full;

  /*
    Indicate whether the audit flushing thread is running.
    Note, we will always start audit log flushing thread when server startup
    regardless of the value of rds_audit_log_enabled.
  */
  bool flush_thread_running;

  /*
    Whether main thread has send out the stop signal to audit log flushing
    thread when server shutdowns.
  */
  bool m_aborted;

  /*
    Flushing thread sleep condition variable.
    We have a simple adaptive algorithm for audit log flushing thread.
    Sleep time among each flushing batch depends on ratio between
    last flushing size and log_buf_size.
  */
  mysql_cond_t flush_thread_sleep_cond;

  /* Mutex for flushing thread sleep condtion variable. */
  mysql_mutex_t flush_thread_sleep_mutex;

  /*
    If rds_audit_log_reserve_all turns on, front/user thread will
    wait until audit log buffer has enough space. This is the sleep
    condition variable.
  */
  mysql_cond_t buf_full_sleep_cond;

  /* Mutex for audit log buffer full sleeping condtion variable. */
  mysql_mutex_t buf_full_sleep_mutex;

  /*
   The max num of records that one audit log file can be written, records are
   discarded beyond this limit.
   this.
  */
  ulong row_limit;

  /*
    The total audit log row number that already generated (No matter it
    is flushed to disk or lost) of current file.

    Like curr_file_pos, we use LOCK_log to keep it consistent with log_fd.
  */
  std::atomic<ulong> row_num;

  /*
    The total audit log row number that already generated(No matter it
    is flushed to disk or lost) of the last file.
  */
  ulong last_row_num;

  /*
    It will be increased if one specified audit log will be lost
    duing to log_buf full.
    It will be set to zero when rotating audit log file.
  */
  std::atomic<ulonglong> lost_row_num_by_buf_full;

  /*
    The total number of all the lost records due to log_buf full.
  */
  std::atomic<ulonglong> lost_row_num_by_buf_full_total;

  /*
    The number of lost records in current log file, due to row_limit
    is reached.
    It will be set to zero when rotating audit log file.
  */
  std::atomic<ulonglong> lost_row_num_by_row_limit;

  /*
    The total number of lost records due to row_limit is reached.
  */
  std::atomic<ulonglong> lost_row_num_by_row_limit_total;

  /*
    The total size of events written to all audit log files. This value will
    be increased even when log is rotated.
  */
  std::atomic<ulonglong> log_total_size;

  /* The size of the largest dropped event in performance logging mode. */
  std::atomic<ulonglong> log_event_max_drop_size;

  /*
    The number of events handled by the audit log plugin, whether or not
    filtered.
  */
  std::atomic<ulonglong> log_events;

  /*
    The number of events handled by the audit log plugin that were filtered
    based on the filtering policy.
  */
  std::atomic<ulonglong> log_events_filtered;

  /*
    The number of events lost in performance logging mode because an event
    was larger than than the available audit log buffer space.
  */
  std::atomic<ulonglong> log_events_lost;

  /* The number of events written to the audit log. */
  std::atomic<ulonglong> log_events_written;

  /*
    The number of times an event had to wait for space in the audit log
    buffer in asynchronous logging mode.
  */
  std::atomic<ulonglong> log_write_waits;

  /* The number of times that audit log writes to log file. */
  std::atomic<ulonglong> log_file_writes;

  /* The number of times that audit log sync log file. */
  std::atomic<ulonglong> log_file_syncs;

  /*
    The size of audit logs that has been flushed succeefully in
    last flushing batch.
    It will be used to determine the sleeping time before doing
    flushing batch.
  */
  ulonglong last_flush_len;

  /*
    Last sleep time in audit log flushing thread.
    It is used for monitor purpose.
  */
  ulong last_sleep_time;

  /* Lock to protect most member of mysql_rds_audit_log. */
  Partitioned_rwlock LOCK_log;

  /*
    Lock for log_fd, curr_file_pos and row_num.

    The main purpose of this lock is make log rotation not
    block user thread writting in buffered-write strategy.
  */
  Partitioned_rwlock LOCK_file;

  /* Issue audit log flushing thread to exit loop. */
  void set_abort() { m_aborted = true; }

  /*
    Whether main thread has issued the stopping signal
    to audit log flushing thread.

    @returns true if aborting singal has been sent
  */
  bool is_aborted() { return m_aborted; }

  /*
    Flushing thread sleep to wait until
    a) timed out b) signaled

    @param[IN] millisecond    Maximum sleep time.
    @returns true if it is timed out
  */
  bool flush_sleep_cond_wait(time_t millisecond);

  /*
    Wakeup the flushing thread sleeping condition.
    This is used when log_buf is full.
  */
  void wakeup_flush_thread();

  /*
    Front/user thread will sleep until audit log buffer
    has enough room for current audit log when
    rds_audit_log_reserve_all turns on.

    @param[IN] millisecond    Maximum sleep time.
    @returns true if it is timed out
  */
  bool buf_full_cond_wait(time_t millisecond);

  /*
    Wakeup the front/user thread when one flush batch has finished.
    This is used when log_buf has free space for new audit logs.
  */
  void wakeup_user();

  /*
    We have two choices when audit log buffer is full:
    a) Discard the following audit logs until audit log buffer
    has free space.
    b) Block front/user thread until audit log buffer has free space.
    It depends on the employed strategy.

    @param[IN] thread_id       thread on which to release lock
    @param[IN] len             length of current log record str
    @returns false if we just discard audit log.
  */
  bool buf_full_handle(uint thread_id, ulong len);

  /*
    Print message to error log if some audit logs will lost duing
    to log_buf full.
  */
  void buf_full_print_error();

  /*
    Copy the audit log to log_buf.

    @param[IN] curr_buf_offset copy log from this offset
    @param[IN] log             audit log content
    @param[IN] log_len         audit log length
  */
  void copy_to_buf(my_off_t curr_buf_offset, const char *log, uint log_len);

  /*
    Flush the audit log from memory to disk.

    @param[IN] curr_flu_offset flush log from this offset
    @param[IN] curr_buf_offset flush log up to this offset
  */
  void flush_from_buf(my_off_t curr_flu_offset, my_off_t curr_buf_offset);

  /*
    Do one batch flushing.

    @param[IN] pwrite_buf     flush log from this buf
    @param[IN] pwrite_len     number of logs to be flush
    @param[IN] pwrite_offset  flush to file from this offset
    @param[IN] sync_to_disk   whether to flush log file
    @returns true if error occurs during flushing
  */
  bool pwrite_batch(const uchar *pwrite_buf, my_off_t pwrite_len,
                    my_off_t pwrite_offset, bool sync_to_disk);

  /*
    Realloc the log_buf if it is necessary, this function
    is invoked when we change rds_audit_log_buffer_size.
  */
  void resize_log_buf_low(ulong new_buf_size);

  /* Open new audit log file. */
  void rotate_log_file_low();

  /*
    Reset information when rotating log file or turning on/off
    audit log recording.
  */
  void reset_file_info();

  /* Reset information when changing audit log buffer size. */
  void reset_offset_info();

  /*
    Flush limited audit logs to disk in one flush batch.

    @param[IN] curr_flu_offset flush log from this offset
    @param[IN] curr_buf_offset flush log up to this offset
  */
  void flow_control(my_off_t curr_flu_offset, my_off_t &curr_buf_offset);

  /*
    Audit log will write to log file directly without cache.

    @param[IN] log_str    serialized log records string
    @param[IN] len        length of log records
    @param[IN] tid        thread id on which to release lock
    @returns true if we have to retry.
  */
  bool write_to_file(const char *log_str, ulong len, ulong tid);

  /*
    Audit log will cache to log_buf
    first and write to log file by background thread
    periodically.

    @param[IN] log_str    serialized log records string
    @param[IN] len        length of log records
    @param[IN] tid        thread id on which to release lock
    @returns true if we have to retry.
  */
  bool write_to_buf(const char *log_str, ulong len, ulong tid);

  /* Whether current strategy using log buffer */
  inline bool is_buffered_write(enum_log_strategy strategy) {
    return strategy <= PERFORMANCE ? true : false;
  }

  /*
    Make sure all user thread has copied it's log_str, before
    some point, has be copied to the global log_buf.
  */
  void wait_user_write();

 public:
  /* Constuctor */
  MYSQL_RDS_AUDIT_LOG(const char *dir, ulong n_buf_size, ulong n_row_limit,
                      ulong log_format, ulong log_strategy, ulong log_policy,
                      ulong log_conn_policy, ulong log_stmt_policy,
                      ulong log_version, bool enable);

  /* Destructor */
  ~MYSQL_RDS_AUDIT_LOG();

  /*
    Enable or disable audit log.
  */
  void set_enable(bool enable);

  /*
    Rotate audit log file when command
    'flush rds_audit logs' issued.
  */
  void rotate_log_file();

  /*
    Change audit log buffer size dynamically.
    As we hold exclusive lock here, all DDL/DML will be blocked.
    QPS/TPS will drop to zero sometimes. Please do not change
    it under heavy load.
  */
  void resize_log_buf(ulong new_buf_size);

  /*
    Change log strategy. log_buf will be forced flushint to log file, if
    change from buffred write to unbuffered.
  */
  void set_log_strategy(enum_log_strategy strategy);

  /* Change log version. */
  void set_log_version(enum_log_version version);

  /* Change log policy. */
  void set_log_policy(enum_log_policy policy);

  /* Change log connection policy. */
  void set_log_connection_policy(enum_log_connection_policy conn_policy);

  /* Change log statement policy. */
  void set_log_statement_policy(enum_log_statement_policy stmt_policy);

  /*
    Process audit log event, this is the entrance of audit log.

    @param[in] event_class    the type of event
    @param[in] event          a general pointer to the event
    @param[in] buf            the buffer used to store serialized event
    @param[in] buf_len        length of buffer
  */
  void process_event(mysql_event_class_t event_class, const void *event,
                     char *buf, uint buf_len);
  /*
    General function to write log records.

    @param[IN] log_str    serialized log records string
    @param[IN] len        length of log records
    @param[IN] tid        thread id on which to release lock
  */
  void write_log(const char *log_str, ulong len, ulong tid);

  /* Start audit log flushing thread when server startups. */
  bool create_flush_thread();

  /* Stop audit log flushing thread when server stops. */
  void stop_flush_thread();

  /*
    Set audit log flushing thread status.

    @param[IN] status new flushing thread status
  */
  void set_flush_thread_status(bool status) { flush_thread_running = status; }

  /*
    Audit log flushing thread should be sleep a moment between
    two log flushing batch.
  */
  void flush_thread_sleep_if_need();

  /*
    Flush audit logs from memory to disk.

    @param[in] flush_all true if we want to flush all logs.
  */
  void flush_log_buf(bool flush_all);

  /*
    Whether the flushing thread should exit when server shutdown.

    @returns true if abort flag is set and all audit logs
    have been flushed successfully
  */
  bool flush_thread_should_exit();

  inline void set_row_limit(ulong new_limit) { row_limit = new_limit; }

  inline bool is_enabled() { return m_enabled; }

  void set_log_format(enum_log_format format);

  /* Return buffered_offset for monitor purpose. */
  inline ulonglong get_buffered_offset() { return buffered_offset.load(); }

  /* Return flushed_offset for monitor purpose. */
  inline ulonglong get_flushed_offset() { return flushed_offset.load(); }

  /* Return current file offset for monitor purpose. */
  inline ulonglong get_file_pos() { return curr_file_pos.load(); }

  /*
    Return log_filename so that we can notice which audit log file is
    active.
  */
  inline char *get_log_filename() { return log_name; }

  /* Return sleep_time among log flushing batch for monitor purpose. */
  inline ulong get_last_sleep_time() { return last_sleep_time; }

  /*
    Return the number of audit logs in current audit log file for monitor
    purpose.
  */
  inline ulong get_row_num() { return row_num.load(); }

  /*
    Return the number of audit logs in last audit log file for monitor
    purpose.
  */
  inline ulong get_last_row_num() { return last_row_num; }

  inline ulonglong get_lost_row_num_by_buf_full() {
    return lost_row_num_by_buf_full.load();
  }

  inline ulonglong get_lost_row_num_by_buf_full_total() {
    return lost_row_num_by_buf_full_total.load();
  }

  inline ulonglong get_lost_row_num_by_row_limit() {
    return lost_row_num_by_row_limit.load();
  }

  inline ulonglong get_lost_row_num_by_row_limit_total() {
    return lost_row_num_by_row_limit_total.load();
  }

  inline ulonglong get_log_total_size() { return log_total_size.load(); }

  inline ulonglong get_log_event_max_drop_size() {
    return log_event_max_drop_size.load();
  }

  inline ulonglong get_log_events() { return log_events.load(); }

  inline ulonglong get_log_events_filtered() {
    return log_events_filtered.load();
  }

  inline ulonglong get_log_events_lost() { return log_events_lost.load(); }

  inline ulonglong get_log_events_written() {
    return log_events_written.load();
  }

  inline ulonglong get_log_write_waits() { return log_write_waits.load(); }

  inline ulonglong get_log_file_writes() { return log_file_writes.load(); }

  inline ulonglong get_log_file_syncs() { return log_file_syncs.load(); }
};

/* Singleton of MySQL_RDS_AUDIT_LOG */
extern MYSQL_RDS_AUDIT_LOG *rds_audit_log;

/*
  Background audit log flushing thread which is created when server startups
  and destoryed when server stops.
*/
extern "C" void *audit_log_flush_thread(void *arg);

#endif /* AUDIT_LOG_H */
