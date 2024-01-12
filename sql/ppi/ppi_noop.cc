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

#include "ppi/ppi_global.h"
#include "ppi/ppi_statement.h"
#include "ppi/ppi_thread.h"
#include "ppi/ppi_transaction.h"

class THD;

static void get_global_io_statistic_noop(PPI_iostat_data *, bool, int *index) {
  *index = -1;
  return;
}

static PPI_global_service_t PPI_global_service_noop = {
    get_global_io_statistic_noop};

static PPI_thread *create_thread_noop() { return nullptr; }
static void destroy_thread_noop(PPI_thread *) { return; }
static void get_thread_statistic_noop(const PPI_thread *, PPI_stat *) {
  return;
}
static PPI_transaction *get_transaction_context_noop(const PPI_thread *) {
  return NULL;
}

/*  The noop definition for thread service */
static PPI_thread_service_t PPI_thread_service_noop = {
    create_thread_noop, destroy_thread_noop, get_thread_statistic_noop,
    get_transaction_context_noop};

static void start_statement_noop(PPI_thread *, PPI_stat *) { return; }

static void end_statement_noop(THD *, PPI_thread *, PPI_stat *) { return; }

static void startoff_statement_time_noop(PPI_thread *) { return; }

static void inc_statement_server_lock_time_noop(const unsigned long long) {
  return;
}
static void inc_statement_transaction_lock_time_noop(const unsigned long long) {
  return;
}

static void inc_statement_mutex_wait_noop(const unsigned long long,
                                          const unsigned long long) {
  return;
}
static void inc_statement_rwlock_wait_noop(const unsigned long long,
                                           const unsigned long long,
                                           const unsigned long long) {
  return;
}

static void inc_statement_logical_read_noop(const unsigned long long) {
  return;
}
static void inc_statement_physical_read_noop(const unsigned long long) {
  return;
}
static void inc_statement_physical_async_read_noop(const unsigned long long) {
  return;
}

static PPI_statement *start_statement_IO_operation_noop(PPI_IO_data *) {
  return nullptr;
}

static void end_statement_IO_operation_noop(PPI_statement *, PPI_IO_data *,
                                            const unsigned long long,
                                            const unsigned long long) {
  return;
}

static void get_statement_statistic_noop(const PPI_thread *, PPI_stat *) {
  return;
}
/* Noop functions for transaction service */
static void start_transaction_noop(PPI_transaction *) { return; }
static void end_transaction_noop(PPI_transaction *) { return; }
static void inc_transaction_binlog_size_noop(PPI_transaction *,
                                             unsigned long long) {
  return;
}
static void backup_transaction_noop(PPI_thread *) { return; }
static void restore_transaction_noop(PPI_thread *) { return; }

static void get_transaction_stat_noop(PPI_thread *, PPI_transaction_stat *) {
  return;
}

static void rows_read_delete_mark_noop(unsigned long long) { return; }

/* The noop definition for thread service */
static PPI_statement_service_t PPI_statement_service_noop = {
    start_statement_noop,
    end_statement_noop,
    startoff_statement_time_noop,
    inc_statement_server_lock_time_noop,
    inc_statement_transaction_lock_time_noop,
    inc_statement_mutex_wait_noop,
    inc_statement_rwlock_wait_noop,
    inc_statement_logical_read_noop,
    inc_statement_physical_read_noop,
    inc_statement_physical_async_read_noop,
    start_statement_IO_operation_noop,
    end_statement_IO_operation_noop,
    get_statement_statistic_noop,
    rows_read_delete_mark_noop};

/* Transaction noop service */
static PPI_transaction_service_t PPI_transaction_service_noop = {
    start_transaction_noop,           end_transaction_noop,
    inc_transaction_binlog_size_noop, backup_transaction_noop,
    restore_transaction_noop,         get_transaction_stat_noop};

/* Thread service handler */
PPI_thread_service_t *PPI_thread_service = &PPI_thread_service_noop;

/* Statement service handler*/
PPI_statement_service_t *PPI_statement_service = &PPI_statement_service_noop;

/* Global service handler*/
PPI_global_service_t *PPI_global_service = &PPI_global_service_noop;

/* Transaction service handler */
PPI_transaction_service_t *PPI_transaction_service =
    &PPI_transaction_service_noop;
