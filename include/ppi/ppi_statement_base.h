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

#ifndef PPI_PPI_STATEMENT_H_INCLUDED
#define PPI_PPI_STATEMENT_H_INCLUDED

#include "ppi/ppi_stat.h"

/**
  Performance Point statement structure declaration

  It will be reinterpret_casted within Performance Point plugin.

  Pls include ppi_statement.h file instead of ppi_statement_base.h
*/
struct PPI_statement;
struct PPI_thread;

class THD;

typedef struct PPI_statement PPI_statement;

/* Start the statement. */
typedef void (*start_statement_func)(PPI_thread *ppi_thread, PPI_stat *stat);
/* End the statement. */
typedef void (*end_statement_func)(THD *thd, PPI_thread *, PPI_stat *);

typedef void (*startoff_statement_time_func)(PPI_thread *ppi_thread);

/* Aggregate the MDL and table lock time */
typedef void (*inc_statement_server_lock_time_func)(
    const unsigned long long value);
/* Aggregate transaction lock time */
typedef void (*inc_statement_transaction_lock_time_func)(
    const unsigned long long value);

typedef void (*inc_statement_mutex_wait_func)(const unsigned long long spins,
                                              const unsigned long long waits);
typedef void (*inc_statement_rwlock_wait_func)(
    const unsigned long long spin_waits, const unsigned long long spin_rounds,
    const unsigned long long os_waits);
/* Increase the logical read count */
typedef void (*inc_statement_logical_read_func)(const unsigned long long value);

/* Increase the physical read count */
typedef void (*inc_statement_physical_read_func)(
    const unsigned long long value);

/* Increase the physical async read count */
typedef void (*inc_statement_physical_async_read_func)(
    const unsigned long long value);

/* Begin the IO operation */
typedef PPI_statement *(*start_statement_IO_operation_func)(PPI_IO_data *data);

/* End the IO operation */
typedef void (*end_statement_IO_operation_func)(PPI_statement *ppi_statement,
                                                PPI_IO_data *data,
                                                const unsigned long long count,
                                                const unsigned long long bytes);

typedef void (*get_statement_statistic_func)(const PPI_thread *ppi_thread,
                                             PPI_stat *stat);

/* Increase the number of rows read with delete_mark */
typedef void (*inc_rows_read_delete_mark_func)(unsigned long long rows);

/* Handler for the statement service */
struct PPI_statement_service_t {
  /* Start statement */
  start_statement_func start_statement;
  /* End statement */
  end_statement_func end_statement;
  /* Start the time */
  startoff_statement_time_func startoff_statement_time;
  /* Aggregate server lock time */
  inc_statement_server_lock_time_func inc_statement_server_lock_time;
  /* Aggregate transaction lock time */
  inc_statement_transaction_lock_time_func inc_statement_transaction_lock_time;
  /* Aggregate mutex wait */
  inc_statement_mutex_wait_func inc_statement_mutex_wait;
  /* Aggregate rwlock wait */
  inc_statement_rwlock_wait_func inc_statement_rwlock_wait;
  /* Aggregate data file logical read count */
  inc_statement_logical_read_func inc_statement_logical_read;
  /* Aggregate data file physical read count */
  inc_statement_physical_read_func inc_statement_physical_read;
  /* Aggregate data file physical async read count */
  inc_statement_physical_async_read_func inc_statement_physical_async_read;
  /* Begin the file IO operation */
  start_statement_IO_operation_func start_statement_IO_operation;
  /* End the file IO operation */
  end_statement_IO_operation_func end_statement_IO_operation;
  /* Get the statement statistic */
  get_statement_statistic_func get_statement_statistic;
  /* Increase the number of rows read with delete_mark */
  inc_rows_read_delete_mark_func inc_rows_read_delete_mark;
};

typedef struct PPI_statement_service_t PPI_statement_service_t;

extern PPI_statement_service_t *PPI_statement_service;

#endif
