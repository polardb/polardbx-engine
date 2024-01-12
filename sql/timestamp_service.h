/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef TIMESTAMP_SERVICE_INCLUDED
#define TIMESTAMP_SERVICE_INCLUDED

#include "my_inttypes.h"
#include "sql_class.h"

/* error number sent to client */
#define GTS_SUCCESS 0
#define GTS_INIT_FAILED 1
#define GTS_SERVICE_ERROR 2

class TimestampService {
 public:
  TimestampService(THD *thd, const char *db_name, const char *table_name)
      : m_thd(thd),
        m_db_name(db_name),
        m_table_name(table_name),
        m_db_name_len(strlen(db_name)),
        m_table_name_len(strlen(table_name)),
        m_table(NULL),
        m_initialized(false) {}

  /* Initialization for getting timestamp value */
  bool init();

  /* Release resource and state cleanup */
  void deinit();

  /* Open the base SEQUENCE table */
  bool open_base_table(thr_lock_type lock_type);

  /* Get next timestamp value */
  bool get_timestamp(uint64_t &ts, const uint64_t batch = 1);

  bool is_initialized() { return m_initialized; }

  ~TimestampService() {}

 private:
  /* Used to open the base table */
  THD *m_thd;

  /* Name of base table and db */
  const char *m_db_name;
  const char *m_table_name;

  uint m_db_name_len;
  uint m_table_name_len;

  /* Handler of the base table */
  TABLE *m_table;

  bool m_initialized;

  /* Backup open tables */
  Open_tables_backup m_state_backup;
};

#endif
