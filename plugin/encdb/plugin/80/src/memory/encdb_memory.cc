/* Copyright (c) 2016, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#include "m_ctype.h"
// #include "my_global.h"
#include "my_thread.h"
#include "sql/mysqld.h"

#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_rwlock.h"

namespace encdb {
PSI_rwlock_key key_LOCK_encdb_rds;
mysql_rwlock_t LOCK_encdb_rds;

PSI_memory_key key_memory_ENCDB_rds;

#ifdef HAVE_PSI_INTERFACE

static PSI_rwlock_info all_encdb_rds_rwlocks[] = {
    {&key_LOCK_encdb_rds, "LOCK_encdb_rds", 0, 0, PSI_DOCUMENT_ME}};

static PSI_memory_info all_encdb_rds_memory[] = {
    {&key_memory_ENCDB_rds, "ENCDB_RDS", 0, 0, PSI_DOCUMENT_ME}};

/* Register all the PSI */
static void encdb_rds_init_psi_keys(void) {
  const char *category = "encdb_rds";
  int count;

  count = static_cast<int>(array_elements(all_encdb_rds_memory));
  mysql_memory_register(category, all_encdb_rds_memory, count);

  count = static_cast<int>(array_elements(all_encdb_rds_rwlocks));
  mysql_rwlock_register(category, all_encdb_rds_rwlocks, count);
}

#endif

static volatile bool initialized = false;

bool encdb_rds_memory_init(void) {
  if (initialized) return true;

#ifdef HAVE_PSI_INTERFACE
  encdb_rds_init_psi_keys();
#endif

  if (mysql_rwlock_init(key_LOCK_encdb_rds, &LOCK_encdb_rds)) return true;

  initialized = true;
  return false;
}

void encdb_rds_memory_deinit() {
  if (!initialized) return;
  mysql_rwlock_destroy(&LOCK_encdb_rds);
  initialized = false;
}

}  // namespace encdb
