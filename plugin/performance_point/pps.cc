/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.
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

/**
  @file

  Performance Point plugin implementation
*/

#include "plugin/performance_point/pps.h"
#include "plugin/performance_point/plugin_pps.h"
#include "plugin/performance_point/pps_stat.h"
#include "plugin/performance_point/pps_table_iostat.h"

#include "mysql/psi/mysql_memory.h"
#include "ppi/ppi_disable.h"
#include "ppi/ppi_global.h"
#include "ppi/ppi_statement.h"
#include "ppi/ppi_thread.h"
#include "sql/current_thd.h"  // current_thd
#include "sql/current_thd.h"
#include "sql/mysqld.h"
#include "sql/psi_memory_key.h"  // PSI_memory_key
#include "sql/sql_class.h"       // THD
#include "sql/sys_vars.h"

/**
  @addtogroup Performance Point plugin

  Implementation of Performance Point plugin.

  @{
*/

/** The performance point variables */
bool opt_performance_point_lock_rwlock_enabled = true;
bool opt_performance_point_enabled = true;
bool opt_performance_point_dbug_enabled = false;

/**
  Create ppi_thread handler when new THD()

  @retval     PPI_thread
*/
static PPI_thread *PPS_create_thread() {
  std::unique_ptr<PPS_thread, PPS_element_deleter<PPS_thread>> m_PPS_thread(
      allocate_object<PPS_thread>());

  if (m_PPS_thread) {
    m_PPS_thread->init_transaction_context();
    return reinterpret_cast<PPI_thread *>(m_PPS_thread.release());
  }
  return nullptr;
}

/**
  Call destroy_thread when destroy THD
*/
static void PPS_destroy_thread(PPI_thread *ppi_thread) {
  if (ppi_thread == nullptr) return;

  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  assert(!pps_thread->statement_executing());

  destroy_object<PPS_thread>(pps_thread);
}

/**
  Retrieve the thread statistics from current thread

  @param[in]    ppi_thread
  @param[out]   ppi_stat
*/
static void PPS_get_thread_statistic(const PPI_thread *ppi_thread,
                                     PPI_stat *lhs) {
  const PPS_thread *pps_thread =
      reinterpret_cast<const PPS_thread *>(ppi_thread);

  assert(pps_thread && lhs);

  pps_thread->copy_thread_stat_to(lhs);
}

/**
  Get the thread transaction context

  @param[in]    ppi_thread

  @retval       ppi_transaction
*/
static PPI_transaction *PPS_get_transaction_context(
    const PPI_thread *ppi_thread) {
  const PPS_thread *pps_thread =
      reinterpret_cast<const PPS_thread *>(ppi_thread);
  assert(pps_thread);

  PPS_transaction *pps_transaction = pps_thread->get_transaction_context();

  return reinterpret_cast<PPI_transaction *>(pps_transaction);
}

/* Performance Point system thread service handler */
static PPI_thread_service_t PPS_thread_service = {
    PPS_create_thread, PPS_destroy_thread, PPS_get_thread_statistic,
    PPS_get_transaction_context};

/* Get current thread object */
static PPS_thread *PPS_current_thread() {
  THD *thd;
  if (!(thd = current_thd)) return nullptr;

  return reinterpret_cast<PPS_thread *>(thd->ppi_thread);
}

/* Get current executing statement, Maybe nullptr. */
static PPS_statement *PPS_current_statement() {
  PPS_thread *pps_thread;
  if (!(pps_thread = PPS_current_thread())) return nullptr;

  return pps_thread->statement();
}

static void PPS_start_transaction(PPI_transaction *ppi_transaction) {
  PPS_transaction *pps_transaction =
      reinterpret_cast<PPS_transaction *>(ppi_transaction);

  if (pps_transaction && !pps_transaction->is_active()) {
    pps_transaction->start_transaction();
  }
}

static void PPS_end_transaction(PPI_transaction *ppi_transaction) {
  PPS_transaction *pps_transaction =
      reinterpret_cast<PPS_transaction *>(ppi_transaction);

  if (pps_transaction) {
    pps_transaction->end_transaction();
  }
}

static void PPS_inc_transaction_binlog_size(PPI_transaction *ppi_transaction,
                                            ulonglong bytes) {
  PPS_transaction *pps_transaction =
      reinterpret_cast<PPS_transaction *>(ppi_transaction);

  if (pps_transaction) {
    pps_transaction->inc_binlog_size(bytes);
  }
}

static void PPS_backup_transaction(PPI_thread *ppi_thread) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  if (pps_thread) pps_thread->backup_transaction();
}

static void PPS_restore_transaction(PPI_thread *ppi_thread) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  if (pps_thread) pps_thread->restore_transaction();
}

static void PPS_get_transaction_stat(PPI_thread *ppi_thread,
                                     PPI_transaction_stat *stat) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  if (pps_thread) {
    PPS_transaction *pps_transaction = pps_thread->get_transaction_context();
    /* Reset stat if we are in nested transaction */
    if (pps_transaction)
      pps_transaction->copy_statistics_to(stat);
    else
      stat->reset();
  }
}

static PPI_transaction_service_t PPS_transaction_service = {
    PPS_start_transaction,           PPS_end_transaction,
    PPS_inc_transaction_binlog_size, PPS_backup_transaction,
    PPS_restore_transaction,         PPS_get_transaction_stat};

/**
  Start a new statement. it will clear thd->ppi_statement_stat

  @param[in]        ppi_thread
  @param[out]       ppi_statement_stat
*/
static void PPS_start_statement(PPI_thread *ppi_thread, PPI_stat *stat) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  /* Clear the statistics when statement begin. */
  if (stat) stat->reset();
  if (pps_thread) {
    pps_thread->begin_statement();

    /* Aggregate the statement count into current transaction */
    PPS_transaction *pps_transaction = pps_thread->get_transaction_context();
    if (pps_transaction) {
      pps_transaction->inc_statement_count(1);
    }
  }
}

/**
  End current statement.

    1. Calculate the elapsed time
    2. Copy statement statistic to stat
    3. Clear statement context

  @param[in]    thd
  @param[in]    ppi_thread
  @param[out]   ppi_statement_stat
*/
static void PPS_end_statement(THD *thd, PPI_thread *ppi_thread,
                              PPI_stat *stat) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);
  if (pps_thread && pps_thread->statement()) {
    assert(pps_thread->statement_executing());
    pps_thread->cutoff_time(thd->start_utime);
    pps_thread->copy_statement_stat_to(stat);
    pps_thread->end_statement();
  }
}

/**
  Snapshot all times
*/
static void PPS_startoff_statement_time(PPI_thread *ppi_thread) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  if (pps_thread) pps_thread->startoff_time();
}

static void PPS_assign_statement_server_lock_time(const ulonglong value) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement) pps_statement->lock_stat().assign_server_lock(value);
}

static void PPS_inc_statement_transaction_lock_time(const ulonglong value) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement)
    pps_statement->lock_stat().aggregate_transaction_lock(value);
}

static void PPS_inc_statement_mutex_wait(const ulonglong spins,
                                         const ulonglong waits) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement) pps_statement->lock_stat().aggregate_mutex(spins, waits);
}

static void PPS_inc_statement_rwlock_wait(const ulonglong spin_waits,
                                          const ulonglong spin_rounds,
                                          const ulonglong os_waits) {
  if (!opt_performance_point_lock_rwlock_enabled) return;

  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement)
    pps_statement->lock_stat().aggregate_rwlock(spin_waits, spin_rounds,
                                                os_waits);
}

/* Increase the logical read count */
static void PPS_inc_statement_logical_read(const ulonglong count) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement) pps_statement->io_stat().aggregate_logical_read(count);
}

/* Increase the physical read count */
static void PPS_inc_statement_physical_read(const ulonglong count) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement) pps_statement->io_stat().aggregate_physical_read(count);
}

/* Increase the physical async read count */
static void PPS_inc_statement_physical_async_read(const ulonglong count) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement)
    pps_statement->io_stat().aggregate_physical_async_read(count);
}

/* Begin the IO operation */
static PPI_statement *PPS_start_statement_IO_operation(PPI_IO_data *io_data) {
  PPS_statement *pps_statement = PPS_current_statement();

  if (!pps_statement) return nullptr;
  if (ppi_current_disable && ppi_current_disable->disable_ppi_statement_stat())
    return nullptr;

  assert(pps_statement->validate_IO_state());

  pps_statement->begin_IO_operation(io_data);

  return reinterpret_cast<PPI_statement *>(pps_statement);
}

/* End the IO operation */
static void PPS_end_statement_IO_operation(PPI_statement *ppi_statement,
                                           PPI_IO_data *io_data,
                                           const ulonglong count,
                                           const ulonglong bytes) {
  ulonglong start_time;
  ulonglong end_time;

  PPI_IO_TYPE io_type = io_data->io_type;
  start_time = io_data->start_time;
  end_time = my_micro_time();

  /* Step 1: aggregate global level IO statistics */

  /* Only Aggregate the data file read and write on global level */
  if (io_type == PPI_IO_DATAFILE_READ || io_type == PPI_IO_DATAFILE_WRITE)
    global_table_iostat_instance->find_and_aggregate(
        io_type, count, end_time - start_time, bytes, end_time / 1000000);

  /* Step 2: aggregate statement level IO statistics */
  PPS_statement *pps_statement =
      reinterpret_cast<PPS_statement *>(ppi_statement);

  if (!pps_statement) return;
  if (ppi_current_disable && ppi_current_disable->disable_ppi_statement_stat())
    return;

  assert(!pps_statement->validate_IO_state());
  pps_statement->aggregate_IO_operation(count, end_time - start_time);
}

/**
  For Future:
  Copy the statement statistics back to ppi_statement_stat

  Usage:
   PPS_get_statement_statistic(thd->ppi_thread, thd->ppi_statement_stat);

   Call it to copy aggregated statement statistic to Server Layer
   thd->ppi_statement_stat before end_statement(),  since end_statement() will
   reset it.
*/
static void PPS_get_statement_statistic(const PPI_thread *ppi_thread,
                                        PPI_stat *stat) {
  const PPS_thread *pps_thread =
      reinterpret_cast<const PPS_thread *>(ppi_thread);
  assert(pps_thread);
  pps_thread->copy_statement_stat_to(stat);
}

static void PPS_inc_rows_read_delete_mark(unsigned long long rows) {
  PPS_statement *pps_statement = PPS_current_statement();

  if (pps_statement)
    pps_statement->rows_stat().aggregate_rows_read_delete_mark(rows);
}

/* Performance Point statement service handler */
static PPI_statement_service_t PPS_statement_service = {
    PPS_start_statement,
    PPS_end_statement,
    PPS_startoff_statement_time,
    PPS_assign_statement_server_lock_time,
    PPS_inc_statement_transaction_lock_time,
    PPS_inc_statement_mutex_wait,
    PPS_inc_statement_rwlock_wait,
    PPS_inc_statement_logical_read,
    PPS_inc_statement_physical_read,
    PPS_inc_statement_physical_async_read,
    PPS_start_statement_IO_operation,
    PPS_end_statement_IO_operation,
    PPS_get_statement_statistic,
    PPS_inc_rows_read_delete_mark};

/**
  Retrieve the io statistics data

  @param[out]       stat          PPI_iostat_data
  @param[in]        first_read    Whether it is the first read
  @param[in/out]    index         Array index
*/
static void PPS_get_io_statistic(PPI_iostat_data *stat, bool first_read,
                                 int *index) {
  if (!global_table_iostat_instance->enabled()) *index = -1;

  if (first_read) *index = 0;

  if (global_table_iostat_instance->get_data(stat, *index))
    *index = -1;
  else
    (*index)++;
}

/* Performance point plugin global service definition */
static PPI_global_service_t PPS_global_service = {PPS_get_io_statistic};

static SYS_VAR *PPS_system_vars[] = {
    NULL,
};

static SHOW_VAR PPS_status_vars[] = {
    {"performance_point_iostat_retry_count",
     (char *)&performance_point_iostat_retry_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {NULL, NULL, SHOW_LONG, SHOW_SCOPE_GLOBAL},
};

/**
  Performance Point plugin init function.
*/
static int PPS_initialize(void *) {
  if (opt_initialize || !opt_performance_point_enabled) return 0;

  /* Register the PPS global service */
  PPI_register_global_service(&PPS_global_service);

  /* Register the PPS thread service */
  PPI_register_thread_service(&PPS_thread_service);

  /* Register the PPS transaction service */
  PPI_register_transaction_service(&PPS_transaction_service);

  /* Register the PPS statement service */
  PPI_register_statement_service(&PPS_statement_service);
  return 0;
}

static struct st_mysql_performance_point PPS_plugin = {
    MYSQL_PERFORMANCE_POINT_INTERFACE_VERSION};

mysql_declare_plugin(performance_point){
    MYSQL_PERFORMANCE_POINT_PLUGIN, /* type               */
    &PPS_plugin,                    /* decriptor          */
    "performance_point",            /* name               */
    "Jianwei.zhao",                 /* author             */
    "Performance Point plugin",     /* description        */
    PLUGIN_LICENSE_GPL,
    PPS_initialize,  /* Plugin init        */
    NULL,            /* uninstall func     */
    NULL,            /* deinit func        */
    0x0100,          /* version            */
    PPS_status_vars, /* status variables   */
    PPS_system_vars, /* system variables   */
    NULL,
    0,
} mysql_declare_plugin_end;

/// @} (end of group Performance Point plugin)
