/* Copyright (c) 2010, 2017, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA
  */

#ifndef TABLE_ESMS_BY_USER_BY_EVENT_NAME_H
#define TABLE_ESMS_BY_USER_BY_EVENT_NAME_H

/**
  @file storage/perfschema/table_esms_by_user_by_event_name.h
  Table EVENTS_STATEMENTS_SUMMARY_BY_USER_BY_EVENT_NAME (declarations).
*/

#include <sys/types.h>

#include "pfs_column_types.h"
#include "pfs_engine_table.h"
#include "pfs_instr.h"
#include "pfs_instr_class.h"
#include "pfs_user.h"
#include "table_helper.h"

/**
  @addtogroup performance_schema_tables
  @{
*/

class PFS_index_esms_by_user_by_event_name : public PFS_engine_index
{
public:
  PFS_index_esms_by_user_by_event_name()
    : PFS_engine_index(&m_key_1, &m_key_2),
      m_key_1("USER"),
      m_key_2("EVENT_NAME")
  {
  }

  ~PFS_index_esms_by_user_by_event_name()
  {
  }

  virtual bool match(PFS_user *pfs);
  virtual bool match(PFS_instr_class *instr_class);

private:
  PFS_key_user m_key_1;
  PFS_key_event_name m_key_2;
};

/**
  A row of table
  PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_USER_BY_EVENT_NAME.
*/
struct row_esms_by_user_by_event_name
{
  /** Column USER. */
  PFS_user_row m_user;
  /** Column EVENT_NAME. */
  PFS_event_name_row m_event_name;
  /** Columns COUNT_STAR, SUM/MIN/AVG/MAX TIMER_WAIT. */
  PFS_statement_stat_row m_stat;
};

/**
  Position of a cursor on
  PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_USER_BY_EVENT_NAME.
  Index 1 on user (0 based)
  Index 2 on statement class (1 based)
*/
struct pos_esms_by_user_by_event_name : public PFS_double_index
{
  pos_esms_by_user_by_event_name() : PFS_double_index(0, 1)
  {
  }

  inline void
  reset(void)
  {
    m_index_1 = 0;
    m_index_2 = 1;
  }

  inline void
  next_user(void)
  {
    m_index_1++;
    m_index_2 = 1;
  }
};

/** Table PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_USER_BY_EVENT_NAME. */
class table_esms_by_user_by_event_name : public PFS_engine_table
{
public:
  /** Table share */
  static PFS_engine_table_share m_share;
  static PFS_engine_table *create(PFS_engine_table_share *);
  static int delete_all_rows();
  static ha_rows get_row_count();

  virtual void reset_position(void);

  virtual int rnd_init(bool scan);
  virtual int rnd_next();
  virtual int rnd_pos(const void *pos);

  virtual int index_init(uint idx, bool sorted);
  virtual int index_next();

protected:
  virtual int read_row_values(TABLE *table,
                              unsigned char *buf,
                              Field **fields,
                              bool read_all);

  table_esms_by_user_by_event_name();

public:
  ~table_esms_by_user_by_event_name()
  {
  }

protected:
  int make_row(PFS_user *user, PFS_statement_class *klass);

private:
  /** Table share lock. */
  static THR_LOCK m_table_lock;
  /** Table definition. */
  static Plugin_table m_table_def;

  /** Current row. */
  row_esms_by_user_by_event_name m_row;
  /** Current position. */
  pos_esms_by_user_by_event_name m_pos;
  /** Next position. */
  pos_esms_by_user_by_event_name m_next_pos;

  PFS_index_esms_by_user_by_event_name *m_opened_index;
};

/** @} */
#endif
