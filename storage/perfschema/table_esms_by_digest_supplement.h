/* Copyright (c) 2010, 2018, Alibaba and/or its affiliates. All rights reserved.

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
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
  */

#ifndef TABLE_ESMS_BY_DIGEST_SUPPLEMENT_H
#define TABLE_ESMS_BY_DIGEST_SUPPLEMENT_H

/**
  @file storage/perfschema/table_esms_by_digest_supplement.h
  Table EVENTS_STATEMENTS_SUMMARY_BY_DIGEST_SUPPLEMENT (declarations).
*/

#include <sys/types.h>

#include "my_inttypes.h"
#include "storage/perfschema/pfs_digest.h"
#include "storage/perfschema/table_esms_by_digest.h"
#include "storage/perfschema/table_helper.h"

/**
  @addtogroup performance_schema_tables
  @{
*/

class PFS_index_esms_by_digest_supplement : public PFS_index_esms_by_digest {
 public:
  PFS_index_esms_by_digest_supplement() : PFS_index_esms_by_digest() {}

  virtual ~PFS_index_esms_by_digest_supplement() {}
};

struct row_esms_by_digest_supplement {
  /** Columns DIGEST/DIGEST_TEXT. */
  PFS_digest_row m_digest;
  /** Columns io_stat/lock_stat. */
  PPI_stat m_stat;
};

/** Table PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST_SUPPLEMENT. */
class table_esms_by_digest_supplement : public PFS_engine_table {
 public:
  /** Table share */
  static PFS_engine_table_share m_share;
  static PFS_engine_table *create(PFS_engine_table_share *);
  static int delete_all_rows();
  static ha_rows get_row_count();

  virtual void reset_position(void) override;

  virtual int rnd_next() override;
  virtual int rnd_pos(const void *pos) override;

  virtual int index_init(uint idx, bool sorted) override;
  virtual int index_next() override;

 protected:
  virtual int read_row_values(TABLE *table, unsigned char *buf, Field **fields,
                              bool read_all) override;

  table_esms_by_digest_supplement();

 public:
  ~table_esms_by_digest_supplement() override {}

 protected:
  int make_row(PFS_statements_digest_stat *);

 public:
  /** Table share lock. */
  static THR_LOCK m_table_lock;
  /** Table definition. */
  static Plugin_table m_table_def;

  /** Current row. */
  row_esms_by_digest_supplement m_row;
  /** Current position. */
  PFS_simple_index m_pos;
  /** Next position. */
  PFS_simple_index m_next_pos;

  PFS_index_esms_by_digest_supplement *m_opened_index;
};

/** @} */
#endif
