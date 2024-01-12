/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PERFORMANCE_POINT_PPS_STAT_H_INCLUDED
#define PERFORMANCE_POINT_PPS_STAT_H_INCLUDED

#include <climits>

#include "my_inttypes.h"  // ulonglong
#include "my_sys.h"       // my_micro_time
#include "my_systime.h"
#include "sql/sql_const.h"  // ULLONG_MAX

#include "ppi/ppi_statement.h"
#include "ppi/ppi_transaction.h"

class THD;
extern bool opt_performance_point_dbug_enabled;

/* Base statistics */
class PPS_base_stat {
 public:
  explicit PPS_base_stat()
      : m_count(0), m_sum(0), m_min(ULLONG_MAX), m_max(0) {}
  virtual ~PPS_base_stat() {}

  void reset() {
    m_count = 0;
    m_sum = 0;
    m_min = ULLONG_MAX;
    m_max = 0;
  }

  /* Aggregate the sum and increase 1 */
  void aggregate_value(ulonglong value) {
    m_count++;
    m_sum += value;
    if (m_min > value) m_min = value;
    if (m_max < value) m_max = value;
  }

  /* Aggregate the sum and increase count */
  void aggregate_value(const ulonglong count, const ulonglong value) {
    m_count += count;
    m_sum += value;
    if (m_min > value) m_min = value;
    if (m_max < value) m_max = value;
  }

  /* Aggregate from other base statistics */
  void aggregate_from(const PPS_base_stat &rhs) {
    m_count += rhs.m_count;
    m_sum += rhs.m_sum;

    if (m_min > rhs.m_min) m_min = rhs.m_min;

    if (m_max < rhs.m_max) m_max = rhs.m_max;
  }
  /* Get the sum */
  ulonglong get_sum() const { return m_sum; }
  /* Get the count */
  ulonglong get_count() const { return m_count; }
  /* Get the max */
  ulonglong get_max() const { return m_max; }

 private:
  ulonglong m_count;
  ulonglong m_sum;
  ulonglong m_min;
  ulonglong m_max;
};

/* Lock related statistics */
class PPS_lock_stat {
 public:
  explicit PPS_lock_stat()
      : m_server_lock_time(0),
        m_transaction_lock_time(0),
        m_mutex_spins(0),
        m_mutex_waits(0),
        m_rw_spin_waits(0),
        m_rw_spin_rounds(0),
        m_rw_os_waits(0) {}

  void reset() {
    m_server_lock_time = 0;
    m_transaction_lock_time = 0;
    m_mutex_spins = 0;
    m_mutex_waits = 0;
    m_rw_spin_waits = 0;
    m_rw_spin_rounds = 0;
    m_rw_os_waits = 0;
  }

  void aggregate_mutex(const ulonglong spins, const ulonglong waits) {
    m_mutex_spins += spins;
    m_mutex_waits += waits;
  }
  void aggregate_server_lock(const ulonglong value) {
    m_server_lock_time += value;
#ifndef DBUG_OFF
    if (opt_performance_point_dbug_enabled)
      fprintf(stderr, "%s\t: %lld us;\n", "server_lock", value);
#endif
  }

  void assign_server_lock(const ulonglong value) {
    m_server_lock_time = value;
#ifndef DBUG_OFF
    if (opt_performance_point_dbug_enabled)
      fprintf(stderr, "%s\t: %lld us;\n", "server_lock", value);
#endif
  }

  void aggregate_transaction_lock(const ulonglong time) {
    m_transaction_lock_time += time;
#ifndef DBUG_OFF
    if (opt_performance_point_dbug_enabled)
      fprintf(stderr, "%s\t: %lld us;\n", "transaction_lock", time);
#endif
  }

  void aggregate_rwlock(const ulonglong spin_waits, const ulonglong spin_rounds,
                        const ulonglong os_waits) {
    m_rw_spin_waits += spin_waits;
    m_rw_spin_rounds += spin_rounds;
    m_rw_os_waits += os_waits;
  }

  void aggregate_from(const PPS_lock_stat &rhs) {
    m_server_lock_time += rhs.m_server_lock_time;
    m_transaction_lock_time += rhs.m_transaction_lock_time;
    m_mutex_spins += rhs.m_mutex_spins;
    m_mutex_waits += rhs.m_mutex_waits;
    m_rw_spin_waits += rhs.m_rw_spin_waits;
    m_rw_spin_rounds += rhs.m_rw_spin_rounds;
    m_rw_os_waits += rhs.m_rw_os_waits;
  }

  void copy_statistics_to(PPI_stat *lhs) const {
    lhs->server_lock_time = m_server_lock_time;
    lhs->transaction_lock_time = m_transaction_lock_time;
    lhs->mutex_spins = m_mutex_spins;
    lhs->mutex_waits = m_mutex_waits;
    lhs->rw_spin_waits = m_rw_spin_waits;
    lhs->rw_spin_rounds = m_rw_spin_rounds;
    lhs->rw_os_waits = m_rw_os_waits;
  }

 private:
  /* Server MDL lock time */
  ulonglong m_server_lock_time;
  /* Storage transaction lock time */
  ulonglong m_transaction_lock_time;

  /* mutex stats only add up when DEBUG */
  ulonglong m_mutex_spins;
  ulonglong m_mutex_waits;

  /* rwlock demonstrate the data page and index lock conflict */
  ulonglong m_rw_spin_waits;
  ulonglong m_rw_spin_rounds;
  ulonglong m_rw_os_waits;
};

class PPS_cpu_stat {
 public:
  explicit PPS_cpu_stat()
      : m_clock_time_enabled(false), m_cpu_time(0), m_elapsed_time(0) {}

  void reset() {
    m_clock_time_enabled = false;
    m_cpu_time = 0;
    m_elapsed_time = 0;
  }

  void aggregate_from(const PPS_cpu_stat &rhs) {
    m_cpu_time += rhs.m_cpu_time;
    m_elapsed_time += rhs.m_elapsed_time;
  }

  void copy_statistics_to(PPI_stat *lhs) const {
    lhs->cpu_time = m_cpu_time;
    lhs->elapsed_time = m_elapsed_time;
  }

  /* Snapshot current thread clock time */
  void begin_clock_time() {
#ifdef HAVE_CLOCK_GETTIME
    if ((pthread_getcpuclockid(pthread_self(), &m_cid) == 0) &&
        (clock_gettime(m_cid, &m_start_time) == 0))
      m_clock_time_enabled = true;
    else
      m_clock_time_enabled = false;
#else
    m_clock_time_enabled = false;
#endif
  }

  /* Get current thread clock time delta. */
  void clock_time() {
    if (!m_clock_time_enabled) m_cpu_time = 0;
#ifdef HAVE_CLOCK_GETTIME
    struct timespec end_time;
    if (clock_gettime(m_cid, &end_time) == 0)
      m_cpu_time = diff_timespec(&end_time, &m_start_time) / 1000;
    else
      m_cpu_time = 0;
#else
    m_cpu_time = 0;
#endif

#ifndef DBUG_OFF
    if (opt_performance_point_dbug_enabled)
      fprintf(stderr, "%s\t: %lld us;\n", "clock_lock", m_cpu_time);
#endif
  }

  /* Get current measured time from start_time */
  void elapsed_time(const ulonglong query_start_time) {
    long long diff = my_micro_time() - query_start_time;
    if (diff > 0) m_elapsed_time = diff;

#ifndef DBUG_OFF
    if (opt_performance_point_dbug_enabled)
      fprintf(stderr, "%s\t: %lld us;\n", "elapsed_time", diff);
#endif
  }

 private:
  bool m_clock_time_enabled;
  clockid_t m_cid;
  struct timespec m_start_time;
  ulonglong m_cpu_time;
  ulonglong m_elapsed_time;
};

/* All the IO operation statistics */
class PPS_io_stat {
 public:
  explicit PPS_io_stat()
      : m_df_read_time(),
        m_df_write_time(),
        m_lf_write_time(),
        m_logical_reads(0),
        m_physical_reads(0),
        m_physical_async_reads(0),
        m_start_time(0),
        m_io_type(PPI_IO_NONE) {}

  virtual ~PPS_io_stat() {}

  void reset() {
    m_df_read_time.reset();
    m_df_write_time.reset();
    m_lf_write_time.reset();
    m_logical_reads = 0;
    m_physical_reads = 0;
    m_physical_async_reads = 0;
    m_start_time = 0;
    m_io_type = PPI_IO_NONE;
  }

  /* Aggregate from other IO statistics */
  void aggregate_from(const PPS_io_stat &rhs) {
    m_df_read_time.aggregate_from(rhs.m_df_read_time);
    m_df_write_time.aggregate_from(rhs.m_df_write_time);
    m_lf_write_time.aggregate_from(rhs.m_lf_write_time);
    m_logical_reads += rhs.m_logical_reads;
    m_physical_reads += rhs.m_physical_reads;
    m_physical_async_reads += rhs.m_physical_async_reads;
  }

  /* Aggregate logical read */
  void aggregate_logical_read(const ulonglong value) {
    m_logical_reads += value;
  }

  /* Aggregate physical read */
  void aggregate_physical_read(const ulonglong value) {
    m_physical_reads += value;
  }

  /* Aggregate physical async read*/
  void aggregate_physical_async_read(const ulonglong value) {
    m_physical_async_reads += value;
  }

  /* Get the base statistics pointer */
  PPS_base_stat *get_stat(const PPI_IO_TYPE io_type) {
    switch (io_type) {
      case PPI_IO_DATAFILE_READ:
        return &m_df_read_time;
      case PPI_IO_DATAFILE_WRITE:
        return &m_df_write_time;
      case PPI_IO_LOG_WRITE:
        return &m_lf_write_time;
      case PPI_IO_NONE:
        return nullptr;
    }
    return nullptr;
  }

#ifndef DBUG_OFF
  static const char *IO_type_str(const PPI_IO_TYPE io_type) {
    switch (io_type) {
      case PPI_IO_DATAFILE_READ:
        return "Data file read";
      case PPI_IO_DATAFILE_WRITE:
        return "Data file write";
      case PPI_IO_LOG_WRITE:
        return "Log file write";
      case PPI_IO_NONE:
        return "None";
    }
    return "None";
  }
#endif
  /* Copy current io statistics to stat. */
  void copy_statistics_to(PPI_stat *lhs) const {
    lhs->data_reads = m_df_read_time.get_count();
    lhs->data_read_time = m_df_read_time.get_sum();

    lhs->data_writes = m_df_write_time.get_count();
    lhs->data_write_time = m_df_write_time.get_sum();

    lhs->redo_writes = m_lf_write_time.get_count();
    lhs->redo_write_time = m_lf_write_time.get_sum();

    lhs->logical_reads = m_logical_reads;
    lhs->physical_reads = m_physical_reads;
    lhs->physical_async_reads = m_physical_async_reads;
  }

  /* Begin a IO operation */
  void start_io(const PPI_IO_data *io_data) {
    assert(m_start_time == 0);
    m_io_type = io_data->io_type;
    m_start_time = io_data->start_time;
  }

  void end_io(ulonglong count, longlong delta) {
    PPS_base_stat *stat = get_stat(m_io_type);
    if (delta > 0) stat->aggregate_value(count, (ulonglong)delta);
    m_start_time = 0;
    m_io_type = PPI_IO_NONE;
#ifndef DBUG_OFF
    if (opt_performance_point_dbug_enabled)
      fprintf(stderr, "%s\t: %lld us;\n", IO_type_str(m_io_type), delta);
#endif
  }

  ulonglong get_time() const { return m_start_time; }

  PPI_IO_TYPE get_io_type() const { return m_io_type; }

 private:
  /* Data file read IO wait time */
  PPS_base_stat m_df_read_time;
  /* Data file write IO wait time */
  PPS_base_stat m_df_write_time;
  /* Log file write IO wait time */
  PPS_base_stat m_lf_write_time;

  /* Different type of data file read operation */
  ulonglong m_logical_reads;
  ulonglong m_physical_reads;
  ulonglong m_physical_async_reads;

  ulonglong m_start_time;
  PPI_IO_TYPE m_io_type;
};

class PPS_rows_stat {
 public:
  explicit PPS_rows_stat() : m_rows_read_delete_mark(0) {}

  void reset() { m_rows_read_delete_mark = 0; }

  void aggregate_rows_read_delete_mark(ulonglong rows) {
    m_rows_read_delete_mark += rows;
  }

  void aggregate_from(const PPS_rows_stat &rhs) {
    m_rows_read_delete_mark += rhs.m_rows_read_delete_mark;
  }

  void copy_statistics_to(PPI_stat *lhs) const {
    lhs->rows_read_delete_mark = m_rows_read_delete_mark;
  }

 private:
  ulonglong m_rows_read_delete_mark;
};

/* Statement statistics */
class PPS_statement {
 public:
  explicit PPS_statement()
      : m_cpu_stat(),
        m_io_stat(),
        m_lock_stat(),
        m_rows_stat(),
        prev(nullptr) {}

  void reset() {
    m_cpu_stat.reset();
    m_io_stat.reset();
    m_lock_stat.reset();
    m_rows_stat.reset();
  }
  /* Current statement cpu statistic */
  PPS_cpu_stat &cpu_stat() { return m_cpu_stat; }

  /* Current statement IO operation statistic */
  PPS_io_stat &io_stat() { return m_io_stat; }

  /* Current statement lock operation statistic */
  PPS_lock_stat &lock_stat() { return m_lock_stat; }

  /* Current statement rows operation statistic */
  PPS_rows_stat &rows_stat() { return m_rows_stat; }

  bool validate_IO_state() const {
    if (m_io_stat.get_time() == 0) return true;

    return false;
  }

  /* Begin a IO operation */
  void begin_IO_operation(const PPI_IO_data *io_data) {
    m_io_stat.start_io(io_data);
  }

  /* Aggregate IO operation diagnose*/
  void aggregate_IO_operation(const ulonglong count, const longlong delta) {
    m_io_stat.end_io(count, delta);
  }

  PPI_IO_TYPE get_io_operation_type() const { return m_io_stat.get_io_type(); }

  ulonglong get_io_start_time() const { return m_io_stat.get_time(); }

  /* Copy statement statistics */
  void copy_statistics_to(PPI_stat *lhs) {
    m_io_stat.copy_statistics_to(lhs);
    m_lock_stat.copy_statistics_to(lhs);
    m_cpu_stat.copy_statistics_to(lhs);
    m_rows_stat.copy_statistics_to(lhs);
  }

 private:
  /* CPU time */
  PPS_cpu_stat m_cpu_stat;

  /* IO related statistics for statement */
  PPS_io_stat m_io_stat;

  /* Lock related statistics for statement */
  PPS_lock_stat m_lock_stat;

  /* Rows read delete mark count */
  PPS_rows_stat m_rows_stat;

 public:
  /* For sub statement */
  PPS_statement *prev;
};

/* Transaction statistics */
class PPS_transaction {
 public:
  explicit PPS_transaction()
      : m_start_time(0),
        m_state(PPI_TRANSACTION_NONE),
        m_binlog_size(),
        m_statement_count(0) {}

  void start_transaction() {
    assert(m_state != PPI_TRANSACTION_ACTIVE);

    if (m_state != PPI_TRANSACTION_ACTIVE) {
      m_start_time = my_micro_time();
      m_state = PPI_TRANSACTION_ACTIVE;
      m_binlog_size.reset();
      m_statement_count = 0;
    }
  }
  void end_transaction() { m_state = PPI_TRANSACTION_IDLE; }

  void inc_binlog_size(ulonglong bytes) {
    m_binlog_size.aggregate_value(bytes);
  }

  void inc_statement_count(ulonglong count) { m_statement_count += count; }

  ulonglong get_binlog_size() { return m_binlog_size.get_sum(); }

  void copy_statistics_to(PPI_transaction_stat *stat) {
    stat->start_time = m_start_time;
    stat->state = m_state;
    stat->binlog_size = m_binlog_size.get_sum();
    stat->statement_count = m_statement_count;
  }
  bool is_active() { return m_state == PPI_TRANSACTION_ACTIVE; }

 private:
  ulonglong m_start_time;
  PPI_TRANSACTION_STATE m_state;
  PPS_base_stat m_binlog_size;
  ulonglong m_statement_count;
};

/* Thread statistics */
class PPS_thread {
 public:
  explicit PPS_thread()
      : m_cpu_stat(),
        m_io_stat(),
        m_lock_stat(),
        m_main_statement(),
        m_statement(nullptr),
        m_main_transaction(),
        m_transaction(nullptr) {}

  virtual ~PPS_thread() {}

  void begin_statement();
  void end_statement();

  bool statement_executing() const {
    if (m_statement != nullptr) return true;
    return false;
  }

  void cutoff_time(const ulonglong query_start_time) {
    if (m_statement) {
      m_statement->cpu_stat().clock_time();
      m_statement->cpu_stat().elapsed_time(query_start_time);
    }
  }

  void startoff_time() {
    if (m_statement) m_statement->cpu_stat().begin_clock_time();
  }

  /**
    Copy current statement statistics into PPI struture.

    Note: should be called before end_statement();
  */
  void copy_statement_stat_to(PPI_stat *lhs) const {
    assert(m_statement);
    assert(m_statement->io_stat().get_time() == 0);

    if (m_statement && lhs) m_statement->copy_statistics_to(lhs);
  }

  /**
    Copy thread statistics into PPI struture.
  */
  void copy_thread_stat_to(PPI_stat *lhs) const {
    m_cpu_stat.copy_statistics_to(lhs);
    m_io_stat.copy_statistics_to(lhs);
    m_lock_stat.copy_statistics_to(lhs);
    m_rows_stat.copy_statistics_to(lhs);
  }

  /* Current statement, maybe nullptr */
  PPS_statement *statement() { return m_statement; }

  /**
    Transaction didn't support nest, so here didn't have
    start/end function on thread level.

    Attached DD transaction or sequence autonomous transaction will
    lose some statistics since we reset thd->ppi_transaction = nullptr
    if nest those type transaction.
  */
  void init_transaction_context() { m_transaction = &m_main_transaction; }

  PPS_transaction *get_transaction_context() const { return m_transaction; }

  void backup_transaction() {
    // TODO:
  }

  void restore_transaction() {
    // TODO:
  }

 private:
  /**
     For Future:
     The aggregated cpu/io/lock statistics on thread
  */
  PPS_cpu_stat m_cpu_stat;
  PPS_io_stat m_io_stat;
  PPS_lock_stat m_lock_stat;
  PPS_rows_stat m_rows_stat;

  /* The statement */
  PPS_statement m_main_statement;
  /* Current statement */
  PPS_statement *m_statement;

  /**
    TODO: Now doesn't support nested transaction.
          We don't think it is neccessary right now.
  */
  /* The transaction */
  PPS_transaction m_main_transaction;
  /* Current transaction */
  PPS_transaction *m_transaction;
};

#endif
