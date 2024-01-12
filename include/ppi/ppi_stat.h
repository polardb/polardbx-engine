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

#ifndef PPI_PPI_STAT_H_INCLUDED
#define PPI_PPI_STAT_H_INCLUDED

#include "my_dbug.h"

/**
  The type of Storage Engine Layer IO operation

  Didn't record log read operation since it make no sense
  for foreground user statement.
*/
enum PPI_IO_TYPE {
  PPI_IO_NONE,
  /* Data file read */
  PPI_IO_DATAFILE_READ,
  /* Data file write */
  PPI_IO_DATAFILE_WRITE,
  /* Log file write */
  PPI_IO_LOG_WRITE
};

/**
  IO operation context
*/
struct PPI_IO_data {
  enum PPI_IO_TYPE io_type;
  unsigned long long start_time;
};

typedef enum PPI_IO_TYPE PPI_IO_TYPE;

/**
  Satistic for statement or thread, it will be used by PFS and THD.
*/
struct PPI_stat {
  /* Whole measured time */
  unsigned long long elapsed_time;
  /* CPU related */
  unsigned long long cpu_time;

  /* Lock related */
  /* MDL lock and table lock time  */
  unsigned long long server_lock_time;
  /* Storage transaction lock lock time */
  unsigned long long transaction_lock_time;

  /**
    Attention:
    mutex stats only affect when DEBUG mode since maybe have performance
    regression
  */

  /* mutex spin and wait */
  unsigned long long mutex_spins;
  unsigned long long mutex_waits;

  /* Index or block rwlock spin and wait */
  unsigned long long rw_spin_waits;
  unsigned long long rw_spin_rounds;
  unsigned long long rw_os_waits;

  /* Data file read count */
  unsigned long long data_reads;
  /* Data file read time */
  unsigned long long data_read_time;

  /* Data file write count */
  unsigned long long data_writes;
  /* Data file write time */
  unsigned long long data_write_time;

  /* Log file write count */
  unsigned long long redo_writes;
  /* Log file write time */
  unsigned long long redo_write_time;

  /* Logical page read */
  unsigned long long logical_reads;
  /* Physical page read */
  unsigned long long physical_reads;
  /* Physical async page read triggered by read-ahead */
  unsigned long long physical_async_reads;
  /* rows read delete mark count */
  unsigned long long rows_read_delete_mark;

  PPI_stat() { reset(); }

  /* Reset all the metrics value */
  void reset() {
    elapsed_time = 0;
    cpu_time = 0;
    server_lock_time = 0;
    transaction_lock_time = 0;

    mutex_spins = 0;
    mutex_waits = 0;

    rw_spin_waits = 0;
    rw_spin_rounds = 0;
    rw_os_waits = 0;

    data_reads = 0;
    data_read_time = 0;

    data_writes = 0;
    data_write_time = 0;

    redo_writes = 0;
    redo_write_time = 0;

    logical_reads = 0;
    physical_reads = 0;
    physical_async_reads = 0;
    rows_read_delete_mark = 0;
  }

  /* Agrregate the rhsment rhsistics */
  void aggregate(const PPI_stat *rhs) {
    if (rhs) {
      elapsed_time += rhs->elapsed_time;
      cpu_time += rhs->cpu_time;

      server_lock_time += rhs->server_lock_time;
      transaction_lock_time += rhs->transaction_lock_time;

      mutex_spins += rhs->mutex_spins;
      mutex_waits += rhs->mutex_waits;

      rw_spin_waits += rhs->rw_spin_waits;
      rw_spin_rounds += rhs->rw_spin_rounds;
      rw_os_waits += rhs->rw_os_waits;

      data_reads += rhs->data_reads;
      data_read_time += rhs->data_read_time;

      data_writes += rhs->data_writes;
      data_write_time += rhs->data_write_time;

      redo_writes += rhs->redo_writes;
      redo_write_time += rhs->redo_write_time;

      logical_reads += rhs->logical_reads;
      physical_reads += rhs->physical_reads;
      physical_async_reads += rhs->physical_async_reads;
      rows_read_delete_mark += rhs->rows_read_delete_mark;
    }
  }

  /* Copy values */
  void copy(const PPI_stat *rhs) {
    if (rhs) {
      elapsed_time = rhs->elapsed_time;
      cpu_time = rhs->cpu_time;

      server_lock_time = rhs->server_lock_time;
      transaction_lock_time = rhs->transaction_lock_time;

      mutex_spins = rhs->mutex_spins;
      mutex_waits = rhs->mutex_waits;

      rw_spin_waits = rhs->rw_spin_waits;
      rw_spin_rounds = rhs->rw_spin_rounds;
      rw_os_waits = rhs->rw_os_waits;

      data_reads = rhs->data_reads;
      data_read_time = rhs->data_read_time;

      data_writes = rhs->data_writes;
      data_write_time = rhs->data_write_time;

      redo_writes = rhs->redo_writes;
      redo_write_time = rhs->redo_write_time;

      logical_reads = rhs->logical_reads;
      physical_reads = rhs->physical_reads;
      physical_async_reads = rhs->physical_async_reads;
      rows_read_delete_mark = rhs->rows_read_delete_mark;
    }
  }
};

/* Make sure get_stat_value() can work correct. */
static_assert(sizeof(struct PPI_stat) == sizeof(unsigned long long) * 19,
              "get_stat_value() can not work correct");

/* Get the metric value according to the position */
inline unsigned long long get_stat_value(PPI_stat *stat, unsigned int i) {
  assert(i < (sizeof(struct PPI_stat) / sizeof(unsigned long long)));
  unsigned char *ptr = (unsigned char *)(stat);
  unsigned long long *element =
      (unsigned long long *)(ptr + i * sizeof(unsigned long long));

  return *element;
}

/* IO STATISTICS data */
struct PPI_iostat_data {
  /* Interval time end (second) */
  unsigned long long time;
  /* Data read count */
  unsigned long long data_read;
  /* The sum time of data read */
  unsigned long long data_read_time;
  /* The max time of data read */
  unsigned long long data_read_max_time;
  /* The sum bytes of data read */
  unsigned long long data_read_bytes;
  /* Data write count */
  unsigned long long data_write;
  /* The sum time of data write */
  unsigned long long data_write_time;
  /* The max time of data write */
  unsigned long long data_write_max_time;
  /* The sum bytes of data write */
  unsigned long long data_write_bytes;
};

/* Transaction state */
enum PPI_TRANSACTION_STATE {
  PPI_TRANSACTION_NONE,
  PPI_TRANSACTION_ACTIVE,
  PPI_TRANSACTION_IDLE
};

typedef enum PPI_TRANSACTION_STATE PPI_TRANSACTION_STATE;

/* transaction statistics */
struct PPI_transaction_stat {
  /* Transaction start time (my_micro_time)*/
  unsigned long long start_time;
  /* Transaction state */
  PPI_TRANSACTION_STATE state;
  /* Transaction binlog size */
  unsigned long long binlog_size;
  /* Statement count */
  unsigned long long statement_count;

  void reset() {
    start_time = 0;
    state = PPI_TRANSACTION_NONE;
    binlog_size = 0;
    statement_count = 0;
  }
};

typedef struct PPI_transaction_stat PPI_transaction_stat;

#endif
