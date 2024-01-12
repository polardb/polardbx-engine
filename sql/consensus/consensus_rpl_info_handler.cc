/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
#include "sql/rpl_info_dummy.h"
#include "sql/rpl_info_file.h"
#include "sql/rpl_info_handler.h"
#include "sql/rpl_info_table.h"
#include "sql/rpl_info_table_access.h"
#include "sql/rpl_table_access.h"

#include "sql/sql_class.h"
#include "sql/table.h"

THD *Rpl_info_table_access::force_create_thd() {
  THD *thd = NULL;

  old_thd = current_thd;

  thd = System_table_access::create_thd();
  thd->system_thread = SYSTEM_THREAD_INFO_REPOSITORY;
  thd_created = true;

  return (thd);
}

int Rpl_info_dummy::do_flush_info_force_new_thd(
    const bool force MY_ATTRIBUTE((unused))) {
  assert(!abort);
  return 0;
}

int Rpl_info_file::do_flush_info_force_new_thd(const bool force) {
  return do_flush_info(force);
}

int Rpl_info_table::do_flush_info_force_new_thd(const bool force) {
  int error = 1;
  enum enum_return_id res = FOUND_ID;
  TABLE *table = NULL;
  sql_mode_t saved_mode;
  Open_tables_backup backup;

  DBUG_TRACE;

  if (!(force || (sync_period && ++(sync_counter) >= sync_period))) return 0;

  THD *thd = NULL;

  thd = access->force_create_thd();

  sync_counter = 0;
  saved_mode = thd->variables.sql_mode;
  ulonglong saved_options = thd->variables.option_bits;
  thd->variables.option_bits &= ~OPTION_BIN_LOG;
  thd->is_operating_substatement_implicitly = true;

  /*
    Opens and locks the rpl_info table before accessing it.
  */
  if (access->open_table(thd, to_lex_cstring(str_schema),
                         to_lex_cstring(str_table), get_number_info(), TL_WRITE,
                         &table, &backup))
    goto end;

  /*
    Points the cursor at the row to be read according to the
    keys. If the row is not found an error is reported.
  */
  if ((res = access->find_info(field_values, table)) == NOT_FOUND_ID) {
    /*
      Prepares the information to be stored before calling ha_write_row.
    */
    empty_record(table);
    if (access->store_info_values(get_number_info(), table->field,
                                  field_values))
      goto end;

    /*
      Inserts a new row into rpl_info table.
    */
    if ((error = table->file->ha_write_row(table->record[0]))) {
      table->file->print_error(error, MYF(0));
      /*
        This makes sure that the error is 1 and not the status returned
        by the handler.
      */
      error = 1;
      goto end;
    }
    error = 0;
  } else if (res == FOUND_ID) {
    /*
      Prepares the information to be stored before calling ha_update_row.
    */
    store_record(table, record[1]);
    if (access->store_info_values(get_number_info(), table->field,
                                  field_values))
      goto end;

    /*
      Updates a row in the rpl_info table.
    */
    if ((error =
             table->file->ha_update_row(table->record[1], table->record[0])) &&
        error != HA_ERR_RECORD_IS_THE_SAME) {
      table->file->print_error(error, MYF(0));
      /*
        This makes sure that the error is 1 and not the status returned
        by the handler.
      */
      error = 1;
      goto end;
    }
    error = 0;
  }

end:
  DBUG_EXECUTE_IF("mta_debug_concurrent_access", {
    while (thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER &&
           mta_debug_concurrent_access < 2 && mta_debug_concurrent_access > 0) {
      DBUG_PRINT("mts", ("Waiting while locks are acquired to show "
                         "concurrency in mts: %u %u\n",
                         mta_debug_concurrent_access, thd->thread_id()));
      my_sleep(6000000);
    }
  };);

  /*
    Unlocks and closes the rpl_info table.
  */
  error = access->close_table(thd, table, &backup, error) || error;
  thd->is_operating_substatement_implicitly = false;
  thd->variables.sql_mode = saved_mode;
  thd->variables.option_bits = saved_options;
  access->drop_thd(thd);
  return error;
}
