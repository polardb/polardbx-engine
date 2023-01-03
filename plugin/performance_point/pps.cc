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


/**
  @file

  Performance Point plugin implementation
*/

#include "plugin/performance_point/pps.h"
#include "mysql/psi/mysql_memory.h"
#include "plugin/performance_point/plugin_pps.h"
#include "plugin/performance_point/pps_stat.h"
#include "ppi/ppi_statement.h"
#include "ppi/ppi_thread.h"
#include "sql/current_thd.h"  // current_thd
#include "sql/mysqld.h"
#include "sql/psi_memory_key.h"  // PSI_memory_key
#include "sql/sql_class.h"       // THD

/**
  @addtogroup Performance Point plugin

  Implementation of Performance Point plugin.

  @{
*/

PSI_memory_key key_memory_PPS;

/** The performance point variables */
bool opt_performance_point_lock_rwlock_enabled = true;
bool opt_performance_point_enabled = true;
bool opt_performance_point_dbug_enabled = false;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info PPS_all_memory[] = {
    {&key_memory_PPS, "Performance_point_system", PSI_FLAG_THREAD, 0,
     PSI_DOCUMENT_ME}};

static void init_pps_ppi_key() {
  const char *category = "pps";
  int count = static_cast<int>(array_elements(PPS_all_memory));
  mysql_memory_register(category, PPS_all_memory, count);
}
#endif

/**
  Create ppi_thread handler when new THD()

  @retval     PPI_thread
*/
static PPI_thread *PPS_create_thread() {
  std::unique_ptr<PPS_thread, PPS_element_deleter<PPS_thread>> m_PPS_thread(
      allocate_object<PPS_thread>());

  if (m_PPS_thread)
    return reinterpret_cast<PPI_thread *>(m_PPS_thread.release());

  return nullptr;
}

/**
  Call destroy_thread when destroy THD
*/
static void PPS_destroy_thread(PPI_thread *ppi_thread) {
  if (ppi_thread == nullptr) return;

  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  DBUG_ASSERT(!pps_thread->statement_executing());

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

  DBUG_ASSERT(pps_thread && lhs);

  pps_thread->copy_thread_stat_to(lhs);
}

/* Performance Point system thread service handler */
static PPI_thread_service_t PPS_thread_service = {
  PPS_create_thread,
  PPS_destroy_thread,
  PPS_get_thread_statistic };

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

/**
  Start a new statement. it will clear thd->ppi_statement_stat

  @param[in]        ppi_thread
  @param[out]       ppi_statement_stat
*/
static void PPS_start_statement(PPI_thread *ppi_thread, PPI_stat *stat) {
  PPS_thread *pps_thread = reinterpret_cast<PPS_thread *>(ppi_thread);

  /* Clear the statistics when statement begin. */
  if (stat) stat->reset();
  if (pps_thread) pps_thread->begin_statement();
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
    DBUG_ASSERT(pps_thread->statement_executing());
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
static void PPS_inc_statement_logical_read(const ulonglong value) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement) pps_statement->io_stat().aggregate_logical_read(value);
}

/* Increase the physical read count */
static void PPS_inc_statement_physical_read(const ulonglong value) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement) pps_statement->io_stat().aggregate_physical_read(value);
}

/* Increase the physical async read count */
static void PPS_inc_statement_physical_async_read(const ulonglong value) {
  PPS_statement *pps_statement = PPS_current_statement();
  if (pps_statement)
    pps_statement->io_stat().aggregate_physical_async_read(value);
}

/* Begin the IO operation */
static PPI_statement *PPS_start_statement_IO_operation(
    const PPI_IO_TYPE io_type) {
  PPS_statement *pps_statement = PPS_current_statement();

  if (!pps_statement) return nullptr;

  DBUG_ASSERT(pps_statement->validate_IO_state());

  pps_statement->begin_IO_operation(io_type);

  return reinterpret_cast<PPI_statement *>(pps_statement);
}

/* End the IO operation */
static void PPS_end_statement_IO_operation(PPI_statement *ppi_statement,
                                           const ulonglong value) {
  PPS_statement *pps_statement =
      reinterpret_cast<PPS_statement *>(ppi_statement);

  if (!pps_statement) return;

  DBUG_ASSERT(!pps_statement->validate_IO_state());

  pps_statement->aggregate_IO_operation(value);
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
  DBUG_ASSERT(pps_thread);
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

static MYSQL_SYSVAR_BOOL(
    lock_rwlock_enabled, opt_performance_point_lock_rwlock_enabled,
    PLUGIN_VAR_OPCMDARG,
    "Enable Performance Point statement level rwlock aggregation (enabled by default). ",
    NULL, NULL, true);

static MYSQL_SYSVAR_BOOL(
    dbug_enabled, opt_performance_point_dbug_enabled,
    PLUGIN_VAR_OPCMDARG,
    "Enable Performance Point debug mode. ",
    NULL, NULL, false);

static SYS_VAR *PPS_system_vars[] = {
  MYSQL_SYSVAR(lock_rwlock_enabled),
  MYSQL_SYSVAR(dbug_enabled),
  NULL,
};
/**
  Performance Point plugin init function.
*/
static int PPS_initialize(void *) {
  if (opt_initialize || !opt_performance_point_enabled) return 0;

#ifdef HAVE_PSI_INTERFACE
  /* Register the PFS memory monitor */
  init_pps_ppi_key();
#endif

  /* Register the PPS thread service */
  PPI_register_thread_service(&PPS_thread_service);

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
  PPS_initialize,                 /* Plugin init        */
  NULL,                           /* uninstall func     */
  NULL,                           /* deinit func        */
  0x0100,                         /* version            */
  NULL,                           /* status variables   */
  PPS_system_vars,                /* system variables   */
  NULL,
  0,
} mysql_declare_plugin_end;



/// @} (end of group Performance Point plugin)
