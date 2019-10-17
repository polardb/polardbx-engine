/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "my_dbug.h"
#include "my_macros.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_stage.h"
#include "mysql/psi/mysql_thread.h"
#include "sql/recycle_bin/recycle_scheduler.h"

namespace im {

namespace recycle_bin {

/* Recycle bin memory instrument */
PSI_memory_key key_memory_recycle;

PSI_mutex_key key_LOCK_state_recycle_scheduler;
PSI_cond_key key_COND_state_recycle_scheduler;
PSI_mutex_key key_LOCK_sleep_recycle_scheduler;
PSI_cond_key key_COND_sleep_recycle_scheduler;

PSI_thread_key key_thread_recycle_scheduler;

Recycle_scheduler *Recycle_scheduler::m_recycle_scheduler = nullptr;

PSI_stage_info waiting_for_recycle_scheduler_thread_stop = {
    0, "Waiting for the recycle scheduler to stop", 0, PSI_DOCUMENT_ME};

PSI_stage_info stage_waiting_for_next_round = {0, "Waiting for next round", 0,
                                               PSI_DOCUMENT_ME};
#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info recycle_memory[] = {
    {&key_memory_recycle, "im::recycle_bin", 0, 0, PSI_DOCUMENT_ME}};

static PSI_mutex_info recycle_mutexes[] = {
    {&key_LOCK_state_recycle_scheduler, "LOCK_state_recycle_scheduler", 0, 0,
     PSI_DOCUMENT_ME},
    {&key_LOCK_sleep_recycle_scheduler, "LOCK_sleep_recycle_scheduler", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_cond_info recycle_conds[] = {
    {&key_COND_state_recycle_scheduler, "COND_state_recycle_scheduler", 0, 0,
     PSI_DOCUMENT_ME},
    {&key_COND_sleep_recycle_scheduler, "COND_sleep_recycle_scheduler", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_thread_info all_recycle_threads[] = {
    {&key_thread_recycle_scheduler, "recycle_scheduler", "thread_recycle",
     PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME}};

PSI_stage_info *all_events_stages[] = {
    &waiting_for_recycle_scheduler_thread_stop, &stage_waiting_for_next_round};
/**
  Init all the recycle psi keys
*/
static void init_recycle_psi_key() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(recycle_memory));
  mysql_memory_register(category, recycle_memory, count);

  count = static_cast<int>(array_elements(recycle_mutexes));
  mysql_mutex_register(category, recycle_mutexes, count);

  count = static_cast<int>(array_elements(recycle_conds));
  mysql_cond_register(category, recycle_conds, count);

  count = static_cast<int>(array_elements(all_recycle_threads));
  mysql_thread_register(category, all_recycle_threads, count);

  count = static_cast<int>(array_elements(all_events_stages));
  mysql_stage_register(category, all_events_stages, count);
}
#endif

/* Init recycle bin context */
void recycle_init() {
  DBUG_ENTER("recycle_bin_init");

#ifdef HAVE_PSI_INTERFACE
  init_recycle_psi_key();
#endif
  Recycle_scheduler::m_recycle_scheduler =
      allocate_recycle_object<Recycle_scheduler>();

  DBUG_VOID_RETURN;
}

/* Deinit recycle bin context */
void recycle_deinit() {
  DBUG_ENTER("recycle_bin_deinit");
  assert(Recycle_scheduler::m_recycle_scheduler != nullptr);
  destroy_object<Recycle_scheduler>(Recycle_scheduler::m_recycle_scheduler);
  DBUG_VOID_RETURN;
}

/** Start the recycle_scheduler thread */
bool recycle_scheduler_start(bool bootstrap) {
  DBUG_ENTER("recycle_scheduler_start");
  assert(Recycle_scheduler::m_recycle_scheduler != nullptr);

  if (opt_recycle_scheduler && !bootstrap) {
    DBUG_RETURN(Recycle_scheduler::instance()->start());
  }
  DBUG_RETURN(false);
}

} /* namespace recycle_bin */

} /* namespace im */
