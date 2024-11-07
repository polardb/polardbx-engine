/* Copyright (c) 2010, 2018, Oracle and/or its affiliates. Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All rights reserved.

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

/**
  @file storage/perfschema/table_esms_by_digest_supplement.cc
  Table EVENTS_STATEMENTS_SUMMARY_GLOBAL_BY_DIGEST_SUPPLEMENT (implementation).
*/

#include "storage/perfschema/table_esms_by_digest_supplement.h"

#include <stddef.h>

#include "my_dbug.h"
#include "my_thread.h"
#include "sql/field.h"
#include "sql/plugin_table.h"
#include "sql/table.h"
#include "storage/perfschema/pfs_column_types.h"
#include "storage/perfschema/pfs_column_values.h"
#include "storage/perfschema/pfs_digest.h"
#include "storage/perfschema/pfs_global.h"
#include "storage/perfschema/pfs_instr.h"
#include "storage/perfschema/pfs_instr_class.h"
#include "storage/perfschema/pfs_timer.h"
#include "storage/perfschema/pfs_visitor.h"

THR_LOCK table_esms_by_digest_supplement::m_table_lock;

Plugin_table table_esms_by_digest_supplement::m_table_def(
    /* Schema name */
    "performance_schema",
    /* Name */
    "events_statements_summary_by_digest_supplement",
    /* Definition */
    "  SCHEMA_NAME VARCHAR(64),\n"
    "  DIGEST VARCHAR(64),\n"
    "  DIGEST_TEXT LONGTEXT,\n"
    "  ELAPSED_TIME               BIGINT unsigned not null,\n"
    "  CPU_TIME               BIGINT unsigned not null,\n"
    "  SERVER_LOCK_TIME       BIGINT unsigned not null,\n"
    "  TRANSACTION_LOCK_TIME       BIGINT unsigned not null,\n"
    "  MUTEX_SPINS       BIGINT unsigned not null,\n"
    "  MUTEX_WAITS       BIGINT unsigned not null,\n"
    "  RWLOCK_SPIN_WAITS   BIGINT unsigned not null,\n"
    "  RWLOCK_SPIN_ROUNDS  BIGINT unsigned not null,\n"
    "  RWLOCK_OS_WAITS     BIGINT unsigned not null,\n"
    "  DATA_READS       BIGINT unsigned not null,\n"
    "  DATA_READ_TIME   BIGINT unsigned not null,\n"
    "  DATA_WRITES      BIGINT unsigned not null,\n"
    "  DATA_WRITE_TIME  BIGINT unsigned not null,\n"
    "  REDO_WRITES      BIGINT unsigned not null,\n"
    "  REDO_WRITE_TIME  BIGINT unsigned not null,\n"
    "  LOGICAL_READS    BIGINT unsigned not null,\n"
    "  PHYSICAL_READS   BIGINT unsigned not null,\n"
    "  PHYSICAL_ASYNC_READS BIGINT unsigned not null,\n"
    "  ROWS_READ_DELETE_MARK BIGINT unsigned not null,\n"
    "  UNIQUE KEY (SCHEMA_NAME, DIGEST) USING HASH\n",
    /* Options */
    " ENGINE=PERFORMANCE_SCHEMA",
    /* Tablespace */
    nullptr);

PFS_engine_table_share table_esms_by_digest_supplement::m_share = {
    &pfs_truncatable_acl,
    table_esms_by_digest_supplement::create,
    NULL, /* write_row */
    table_esms_by_digest_supplement::delete_all_rows,
    table_esms_by_digest_supplement::get_row_count,
    sizeof(PFS_simple_index),
    &m_table_lock,
    &m_table_def,
    false, /* perpetual */
    PFS_engine_table_proxy(),
    {0},
    false /* m_in_purgatory */
};

PFS_engine_table *table_esms_by_digest_supplement::create(
    PFS_engine_table_share *) {
  return new table_esms_by_digest_supplement();
}

int table_esms_by_digest_supplement::delete_all_rows(void) {
  reset_esms_by_digest();
  return 0;
}

ha_rows table_esms_by_digest_supplement::get_row_count(void) {
  return digest_max;
}

table_esms_by_digest_supplement::table_esms_by_digest_supplement()
    : PFS_engine_table(&m_share, &m_pos), m_pos(0), m_next_pos(0) {
  m_normalizer = time_normalizer::get_statement();
}

void table_esms_by_digest_supplement::reset_position(void) {
  m_pos = 0;
  m_next_pos = 0;
}

int table_esms_by_digest_supplement::rnd_next(void) {
  PFS_statements_digest_stat *digest_stat;

  if (statements_digest_stat_array == NULL) {
    return HA_ERR_END_OF_FILE;
  }

  for (m_pos.set_at(&m_next_pos); m_pos.m_index < digest_max; m_pos.next()) {
    digest_stat = &statements_digest_stat_array[m_pos.m_index];
    if (digest_stat->m_lock.is_populated()) {
      if (digest_stat->m_first_seen != 0) {
        m_next_pos.set_after(&m_pos);
        return make_row(digest_stat);
      }
    }
  }

  return HA_ERR_END_OF_FILE;
}

int table_esms_by_digest_supplement::rnd_pos(const void *pos) {
  PFS_statements_digest_stat *digest_stat;

  if (statements_digest_stat_array == NULL) {
    return HA_ERR_END_OF_FILE;
  }

  set_position(pos);
  digest_stat = &statements_digest_stat_array[m_pos.m_index];

  if (digest_stat->m_lock.is_populated()) {
    if (digest_stat->m_first_seen != 0) {
      return make_row(digest_stat);
    }
  }

  return HA_ERR_RECORD_DELETED;
}

int table_esms_by_digest_supplement::index_init(uint idx MY_ATTRIBUTE((unused)),
                                                bool) {
  PFS_index_esms_by_digest_supplement *result = NULL;
  assert(idx == 0);
  result = PFS_NEW(PFS_index_esms_by_digest_supplement);
  m_opened_index = result;
  m_index = result;
  return 0;
}

int table_esms_by_digest_supplement::index_next(void) {
  PFS_statements_digest_stat *digest_stat;

  if (statements_digest_stat_array == NULL) {
    return HA_ERR_END_OF_FILE;
  }

  for (m_pos.set_at(&m_next_pos); m_pos.m_index < digest_max; m_pos.next()) {
    digest_stat = &statements_digest_stat_array[m_pos.m_index];
    if (digest_stat->m_first_seen != 0) {
      if (m_opened_index->match(digest_stat)) {
        if (!make_row(digest_stat)) {
          m_next_pos.set_after(&m_pos);
          return 0;
        }
      }
    }
  }

  return HA_ERR_END_OF_FILE;
}
/**
  Generate the record element from digest stat.
*/
int table_esms_by_digest_supplement::make_row(
    PFS_statements_digest_stat *digest_stat) {
  m_row.m_digest.make_row(digest_stat);
  m_row.m_stat.copy(&(digest_stat->m_ppi_stat));

  return 0;
}

int table_esms_by_digest_supplement::read_row_values(TABLE *table,
                                                     unsigned char *buf,
                                                     Field **fields,
                                                     bool read_all) {
  Field *f;

  /*
    Set the null bits. It indicates how many fields could be null
    in the table.
  */
  assert(table->s->null_bytes == 1);
  buf[0] = 0;

  for (; (f = *fields); fields++) {
    if (read_all || bitmap_is_set(table->read_set, f->field_index())) {
      switch (f->field_index()) {
        case 0: /* SCHEMA_NAME */
        case 1: /* DIGEST */
        case 2: /* DIGEST_TEXT */
          m_row.m_digest.set_field(f->field_index(), f);
          break;
        default: /* statement statistics */
          set_field_ulonglong(
              f, get_stat_value(&(m_row.m_stat), f->field_index() - 3));
          break;
      }
    }
  }

  return 0;
}
