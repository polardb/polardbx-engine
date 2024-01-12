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

#include <assert.h>

#include "field.h"
#include "ha_sequence.h"
#include "handler.h"  // table->file->xxx()
#include "log.h"      // sql_print_error
#include "mysqld.h"
#include "sequence_common.h"
#include "sql_base.h"  // Table_ref
#include "table.h"
#include "timestamp_service.h"
#include "transaction.h"  // trans_commit_stmt

/**
  Initialize timestamp service

  @retval         false if success, true otherwise
*/

bool TimestampService::init() {
  assert(!m_initialized);

  /*
    Ideally, this class can be used in any level of the code path when
    needed, before it's used, some tables may be already opened, and
    some MDL locks may be already taken, in this case, it's necessary
    to backup these opened tables and MDL locks and restore them later.

    Note, if it's found later that above case never happen, there is
    no need to do this.
  */
  m_thd->reset_n_backup_open_tables_state(&m_state_backup,
                                          Open_tables_state::SYSTEM_TABLES);

  /* Open the base table */
  if (open_base_table(TL_WRITE)) {
    m_thd->restore_backup_open_tables_state(&m_state_backup);
    return true;
  }

  m_initialized = true;

  return false;
}

bool TimestampService::open_base_table(thr_lock_type lock_type) {
  bool ret = false;
  char db_name_buf[NAME_LEN + 1];
  char table_name_buf[NAME_LEN + 1];
  const char *db = m_db_name;
  const char *table = m_table_name;

  assert(m_db_name && m_table_name);

  if (lower_case_table_names) {
    snprintf(db_name_buf, sizeof(db_name_buf) - 1, "%s", m_db_name);
    my_casedn_str(system_charset_info, db_name_buf);
    db = db_name_buf;

    snprintf(table_name_buf, sizeof(table_name_buf) - 1, "%s", m_table_name);
    my_casedn_str(system_charset_info, table_name_buf);
    table = table_name_buf;
  }

  Table_ref tables(db, table, lock_type);

  tables.sequence_scan.set(Sequence_scan_mode::ITERATION_SCAN);
  tables.open_strategy = Table_ref::OPEN_IF_EXISTS;

  if (!open_n_lock_single_table(m_thd, &tables, lock_type, 0)) {
    /*
      Even target table does not exist, low level function open_tables()
      does not return TRUE, as a result, lock will still be taken, see
      open_and_lock_tables(), in this case, it's still necessary to call
      close_thread_tables() which will release the lock.
    */
    close_thread_tables(m_thd);
    m_thd->get_stmt_da()->set_overwrite_status(true);
    char errmsg[256] = {0};
    sprintf(errmsg, "Can not open table [%s.%s]", m_db_name, m_table_name);
    my_error(ER_TIMESTAMP_SERVICE_ERROR, MYF(0), errmsg);
    m_thd->get_stmt_da()->set_overwrite_status(false);
    ret = true;
  } else {
    m_table = tables.table;
    m_table->use_all_columns();
  }

  return ret;
}

/**
  Get next timestamp by calling the low level sequence engine APIs

  @param[out]     ts         timestamp value
  @param[in]      batch      # timestamp requested

  @retval         FALSE if success, TRUE otherwise
*/
bool TimestampService::get_timestamp(uint64_t &ts, const uint64_t batch) {
  bool ret = false;
  int error = 0;

  assert(is_initialized());
  assert(m_table->file);

  /* Check the number of timestamp value requested */
  if (batch < TIMESTAMP_SEQUENCE_MIN_BATCH_SIZE ||
      batch > TIMESTAMP_SEQUENCE_MAX_BATCH_SIZE) {
    char errmsg[256] = {0};
    sprintf(errmsg,
            "Can not reserve %lu timestamp value, valid range is [%lu, %lu]",
            batch, (uint64_t)TIMESTAMP_SEQUENCE_MIN_BATCH_SIZE,
            (uint64_t)TIMESTAMP_SEQUENCE_MAX_BATCH_SIZE);
    m_thd->get_stmt_da()->set_overwrite_status(true);
    my_error(ER_TIMESTAMP_SERVICE_ERROR, MYF(0), errmsg);
    m_thd->get_stmt_da()->set_overwrite_status(false);
    return true;
  }

  /* Set the number of timestamp value requested (default 1) */
  m_table->sequence_scan.set_batch(batch);

  bitmap_set_bit(m_table->read_set, Sequence_field::FIELD_NUM_NEXTVAL);

  if ((error = m_table->file->ha_rnd_init(true))) {
    m_table->file->print_error(error, MYF(0));
    ret = true;
  } else {
    error = m_table->file->ha_rnd_next(m_table->record[0]);
    if (error) {
      m_table->file->print_error(error, MYF(0));
      ret = true;
    } else {
      ts = m_table->field[Sequence_field::FIELD_NUM_NEXTVAL]->val_int();
    }

    m_table->file->ha_rnd_end();
  }

  return ret;
}

/**
  Close opened tables and release the resource (MDL locks)
*/
void TimestampService::deinit() {
  assert(is_initialized());
  assert(m_thd);

  /**
    Commit current statement. Error status may already be set in diagnostic
    area due to previous error print, we should be able to set it here.
  */
  m_thd->get_stmt_da()->set_overwrite_status(true);
  trans_commit_stmt(m_thd);
  m_thd->get_stmt_da()->set_overwrite_status(false);

  /* Close the base table */
  close_thread_tables(m_thd);

  /* Release MDL locks and restore the opened tables */
  m_thd->restore_backup_open_tables_state(&m_state_backup);

  m_table = NULL;
}
