/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
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
  DBUG_ASSERT(i < (sizeof(struct PPI_stat) / sizeof(unsigned long long)));
  unsigned char *ptr = (unsigned char *)(stat);
  unsigned long long *element =
      (unsigned long long *)(ptr + i * sizeof(unsigned long long));

  return *element;
}
#endif
