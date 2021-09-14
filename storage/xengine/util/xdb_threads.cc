/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Portions Copyright (c) 2015-Present, Facebook, Inc.
   Portions Copyright (c) 2012, Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

/* The C++ file's header */
#include "./xdb_threads.h"
#include <sql_class.h>

namespace myx {

void *Xdb_thread::thread_func(void *const thread_ptr) {
  DBUG_ASSERT(thread_ptr != nullptr);
  //create and attach thd to current thread
  //for async commit context switch
  //backgroun thread may do async_commit for forground threads
  //so we need to create THD and set to treads attr
  my_thread_init();
  THD* thd =  new (std::nothrow) THD;
  if (nullptr != thd) {
#ifndef NDEBUG
    set_my_thread_var_id(thd->thread_id());
#endif
    thd->thread_stack=(char*)&thd;
    thd->store_globals();
  }
  Xdb_thread *const thread = static_cast<Xdb_thread *const>(thread_ptr);
  if (!thread->m_run_once.exchange(true)) {
    thread->setname();
    thread->run();
    thread->uninit();
  }
  if (nullptr != thd) 
    delete thd;
  my_thread_end();
  return nullptr;
}

void Xdb_thread::init(
#ifdef HAVE_PSI_INTERFACE
    my_core::PSI_mutex_key stop_bg_psi_mutex_key,
    my_core::PSI_cond_key stop_bg_psi_cond_key
#endif
    ) {
  DBUG_ASSERT(!m_run_once);
  mysql_mutex_init(stop_bg_psi_mutex_key, &m_signal_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(stop_bg_psi_cond_key, &m_signal_cond);
}

void Xdb_thread::uninit() {
  mysql_mutex_destroy(&m_signal_mutex);
  mysql_cond_destroy(&m_signal_cond);
}

int Xdb_thread::create_thread(const std::string &thread_name
#ifdef HAVE_PSI_INTERFACE
                              ,
                              PSI_thread_key background_psi_thread_key
#endif
                              ) {
  // Make a copy of the name so we can return without worrying that the
  // caller will free the memory
  m_name = thread_name;

  return mysql_thread_create(background_psi_thread_key, &m_handle, nullptr,
                             thread_func, this);
}

void Xdb_thread::signal(const bool &stop_thread) {
  XDB_MUTEX_LOCK_CHECK(m_signal_mutex);

  if (stop_thread) {
    m_stop = true;
  }

  mysql_cond_signal(&m_signal_cond);

  XDB_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

} // namespace myx
