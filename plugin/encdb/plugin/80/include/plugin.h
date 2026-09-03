/* Copyright (c) 2014, 2022, Oracle and/or its affiliates.

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
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#ifndef PLUGIN_INCLUDE
#define PLUGIN_INCLUDE

#include "encdb_error.h"
#include "mysql/plugin.h"
#include "mysql/plugin_group_replication.h"
#include "plugin_server_include.h"

/* ENCDB plugin handle, set in plugin_encdb_init(). */
extern MYSQL_PLUGIN encdb_plugin_ptr;

/* Swap thread-local plugin used by Srv_session::open(); see srv_session.h. */
const void *srv_session_set_thread_plugin(const void *new_plugin);

namespace encdb {

/* RAII: swap thread-local plugin on ctor, restore on dtor. */
class Scoped_thread_plugin {
 public:
  explicit Scoped_thread_plugin(const void *new_plugin)
      : old_plugin_(srv_session_set_thread_plugin(new_plugin)) {}
  ~Scoped_thread_plugin() { srv_session_set_thread_plugin(old_plugin_); }
  Scoped_thread_plugin(const Scoped_thread_plugin &) = delete;
  Scoped_thread_plugin &operator=(const Scoped_thread_plugin &) = delete;

 private:
  const void *old_plugin_;
};

}  // namespace encdb

#define CHECK_ERROR_AND_RETURN(e)                                           \
  if ((e)) {                                                                \
    encdb_log_message(LL_INFO, "[EncDB] error %d encountered in %s, %s:%d", \
                      (e), __FUNCTION__, __FILE__, __LINE__);               \
    return e;                                                               \
  }

#endif /* PLUGIN_INCLUDE */
