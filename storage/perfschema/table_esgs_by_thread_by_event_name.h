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
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef TABLE_ESGS_BY_THREAD_BY_EVENT_NAME_H
#define TABLE_ESGS_BY_THREAD_BY_EVENT_NAME_H

/**
  @file storage/perfschema/table_esgs_by_thread_by_event_name.h
  Table EVENTS_STAGES_SUMMARY_BY_THREAD_BY_EVENT_NAME (declarations).
*/

#include <sys/types.h>

#include "my_inttypes.h"
#include "pfs_column_types.h"
#include "pfs_engine_table.h"
#include "pfs_instr.h"
#include "pfs_instr_class.h"
#include "table_helper.h"

/**
  @addtogroup performance_schema_tables
  @{
*/

class PFS_index_esgs_by_thread_by_event_name : public PFS_engine_index
{
public:
  PFS_index_esgs_by_thread_by_event_name()
    : PFS_engine_index(&m_key_1, &m_key_2),
      m_key_1("THREAD_ID"),
      m_key_2("EVENT_NAME")
  {
  }

  ~PFS_index_esgs_by_thread_by_event_name()
  {
  }

  bool match(PFS_thread *pfs);
  bool match(PFS_stage_class *klass);

private:
  PFS_key_thread_id m_key_1;
  PFS_key_event_name m_key_2;
};

/**
  A row of table
  PERFORMANCE_SCHEMA.EVENTS_STAGES_SUMMARY_BY_THREAD_BY_EVENT_NAME.
*/
struct row_esgs_by_thread_by_event_name
{
  /** Column THREAD_ID. */
  ulonglong m_thread_internal_id;
  /** Column EVENT_NAME. */
  PFS_event_name_row m_event_name;
  /** Columns COUNT_STAR, SUM/MIN/AVG/MAX TIMER_WAIT. */
  PFS_stage_stat_row m_stat;
};

/**
  Position of a cursor on
  PERFORMANCE_SCHEMA.EVENTS_STAGES_SUMMARY_BY_THREAD_BY_EVENT_NAME.
  Index 1 on thread (0 based).
  Index 2 on stage class (1 based).
*/
struct pos_esgs_by_thread_by_event_name : public PFS_double_index
{
  pos_esgs_by_thread_by_event_name() : PFS_double_index(0, 1)
  {
  }

  inline void
  reset(void)
  {
    m_index_1 = 0;
    m_index_2 = 1;
  }

  inline void
  next_thread(void)
  {
    m_index_1++;
    m_index_2 = 1;
  }

  inline void
  next_stage(void)
  {
    m_index_2++;
  }
};

/** Table PERFORMANCE_SCHEMA.EVENTS_STAGES_SUMMARY_BY_THREAD_BY_EVENT_NAME. */
class table_esgs_by_thread_by_event_name : public PFS_engine_table
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

  table_esgs_by_thread_by_event_name();

public:
  ~table_esgs_by_thread_by_event_name()
  {
  }

protected:
  int make_row(PFS_thread *thread, PFS_stage_class *klass);

private:
  /** Table share lock. */
  static THR_LOCK m_table_lock;
  /** Table definition. */
  static Plugin_table m_table_def;

  /** Current row. */
  row_esgs_by_thread_by_event_name m_row;
  /** Current position. */
  pos_esgs_by_thread_by_event_name m_pos;
  /** Next position. */
  pos_esgs_by_thread_by_event_name m_next_pos;

  PFS_index_esgs_by_thread_by_event_name *m_opened_index;
};

/** @} */
#endif
