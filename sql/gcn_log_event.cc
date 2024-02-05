/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/gcn_log_event.h

  Global Commit Number log event.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#define LOG_SUBSYSTEM_TAG "Repl"

#include "sql/log_event.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "my_config.h"
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "base64.h"
#include "decimal.h"
#include "libbinlogevents/export/binary_log_funcs.h"  // my_timestamp_binary_length
#include "libbinlogevents/include/debug_vars.h"
#include "libbinlogevents/include/table_id.h"
#include "libbinlogevents/include/wrapper_functions.h"
#include "m_ctype.h"
#include "my_bitmap.h"
#include "my_byteorder.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_io.h"
#include "my_loglevel.h"
#include "my_macros.h"
#include "my_systime.h"
#include "my_table_map.h"
#include "my_time.h"  // MAX_DATE_STRING_REP_LENGTH
#include "mysql.h"    // MYSQL_OPT_MAX_ALLOWED_PACKET
#include "mysql/components/services/bits/psi_statement_bits.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/log_shared.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/udf_registration_types.h"
#include "mysql_time.h"
#include "psi_memory_key.h"
#include "query_options.h"
#include "scope_guard.h"
#include "sql/auth/auth_acls.h"
#include "sql/binlog_reader.h"
#include "sql/field_common_properties.h"
#include "sql/my_decimal.h"   // my_decimal
#include "sql/rpl_handler.h"  // RUN_HOOK
#include "sql/rpl_tblmap.h"
#include "sql/sql_show_processlist.h"  // pfs_processlist_enabled
#include "sql/system_variables.h"
#include "sql/tc_log.h"
#include "sql/xa/sql_cmd_xa.h"  // Sql_cmd_xa_*
#include "sql_const.h"
#include "sql_string.h"
#include "template_utils.h"

#ifndef MYSQL_SERVER
#include "client/mysqlbinlog.h"
#include "sql-common/json_binary.h"
#include "sql-common/json_dom.h"  // Json_wrapper
#include "sql/json_diff.h"        // enum_json_diff_operation
#endif

#ifdef MYSQL_SERVER

#include <errno.h>
#include <fcntl.h>

#include <cstdint>
#include <new>

#include "libbinlogevents/include/binary_log.h"  // binary_log
#include "my_base.h"
#include "my_command.h"
#include "my_dir.h"  // my_dir
#include "my_sqlcommand.h"
#include "mysql/plugin.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_stage.h"
#include "mysql/psi/mysql_statement.h"
#include "mysql/psi/mysql_transaction.h"
#include "mysql/psi/psi_statement.h"
#include "mysqld_error.h"
#include "prealloced_array.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/changestreams/misc/replicated_columns_view_factory.h"  // get_columns_view
#include "sql/current_thd.h"
#include "sql/dd/types/abstract_table.h"  // dd::enum_table_type
#include "sql/debug_sync.h"               // debug_sync_set_action
#include "sql/derror.h"                   // ER_THD
#include "sql/enum_query_type.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_func.h"  // Item_func_set_user_var
#include "sql/key.h"
#include "sql/log.h"  // Log_throttle
#include "sql/mdl.h"
#include "sql/mysqld.h"  // lower_case_table_names server_uuid ...
#include "sql/protocol.h"
#include "sql/rpl_msr.h"          // channel_map
#include "sql/rpl_mta_submode.h"  // Mts_submode
#include "sql/rpl_replica.h"      // use_slave_mask
#include "sql/rpl_reporting.h"
#include "sql/rpl_rli.h"      // Relay_log_info
#include "sql/rpl_rli_pdb.h"  // Slave_job_group
#include "sql/sp_head.h"      // sp_name
#include "sql/sql_base.h"     // close_thread_tables
#include "sql/sql_bitmap.h"
#include "sql/sql_class.h"
#include "sql/sql_cmd.h"
#include "sql/sql_data_change.h"
#include "sql/sql_db.h"  // load_db_opt_by_name
#include "sql/sql_digest_stream.h"
#include "sql/sql_error.h"
#include "sql/sql_exchange.h"  // sql_exchange
#include "sql/sql_gipk.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"        // I_List
#include "sql/sql_load.h"        // Sql_cmd_load_table
#include "sql/sql_locale.h"      // my_locale_by_number
#include "sql/sql_parse.h"       // mysql_test_parse_for_slave
#include "sql/sql_plugin.h"      // plugin_foreach
#include "sql/sql_show.h"        // append_identifier
#include "sql/sql_tablespace.h"  // Sql_cmd_tablespace
#include "sql/table.h"
#include "sql/transaction.h"  // trans_rollback_stmt
#include "sql/transaction_info.h"
#include "sql/tztime.h"  // Time_zone
#include "thr_lock.h"
#endif /* MYSQL_SERVER */

#include "libbinlogevents/include/codecs/binary.h"
#include "libbinlogevents/include/codecs/factory.h"
#include "libbinlogevents/include/compression/iterator.h"
#include "mysqld_error.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_record.h"  // enum_row_image_type, Bit_reader
#include "sql/rpl_utility.h"
#include "sql/xa_aux.h"

#include "gcn_log_event.h"

#ifdef MYSQL_SERVER
Gcn_log_event::Gcn_log_event(THD *thd_arg)
    : binary_log::Gcn_event(),
      Log_event(thd_arg, LOG_EVENT_IGNORABLE_F,
                Log_event::EVENT_TRANSACTIONAL_CACHE,
                Log_event::EVENT_NORMAL_LOGGING, header(), footer()) {
  DBUG_TRACE;

  Log_event_type event_type = binary_log::GCN_LOG_EVENT;
  common_header->type_code = event_type;
  common_header->set_is_valid(true);
  common_header->flags |= LOG_EVENT_IGNORABLE_F;
}

int Gcn_log_event::pack_info(Protocol *protocol) {
  String str_buf;
  str_buf.append("SET @@SESSION.INNODB_COMMIT_SEQ=");

  char gcn_buf[64];
  longlong10_to_str(commit_gcn, gcn_buf, 10);
  str_buf.append(gcn_buf);

  protocol->store_string(str_buf.ptr(), str_buf.length(), &my_charset_bin);
  return 0;
}
#endif  // MYSQL_SERVER

#ifdef MYSQL_SERVER
uint32 Gcn_log_event::write_data_header_to_memory(uchar *buffer) {
  DBUG_TRACE;
  uchar *ptr_buffer = buffer;

  if (!thd->owned_commit_gcn.is_empty()) {
    flags |= FLAG_HAVE_COMMITTED_GCN;
    commit_gcn = thd->owned_commit_gcn.get_gcn();

    if (thd->owned_commit_gcn.is_assigned()) {
      flags |= FLAG_GCN_ASSIGNED;
    }
  }

  if (thd->variables.innodb_commit_gcn != MYSQL_GCN_NULL) {
    flags |= FLAG_HAVE_COMMITTED_SEQ;
  }

  if (thd->variables.innodb_snapshot_gcn != MYSQL_GCN_NULL) {
    flags |= FLAG_HAVE_SNAPSHOT_SEQ;
  }

  // DBUG_ASSERT(flags != 0);

  *ptr_buffer = flags;
  ptr_buffer += FLAGS_LENGTH;

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    int8store(ptr_buffer, commit_gcn);
    ptr_buffer += COMMITTED_GCN_LENGTH;
  }

  return ptr_buffer - buffer;
}

bool Gcn_log_event::write_data_header(Basic_ostream *ostream) {
  DBUG_TRACE;
  uchar buffer[POST_HEADER_LENGTH];
  write_data_header_to_memory(buffer);
  return wrapper_my_b_safe_write(ostream, (uchar *)buffer, POST_HEADER_LENGTH);
}

bool Gcn_log_event::write(Basic_ostream *ostream) {
  return (write_header(ostream, get_data_size()) ||
          write_data_header(ostream) || write_footer(ostream));
}

#endif

#ifndef MYSQL_SERVER
void Gcn_log_event::print(FILE *, PRINT_EVENT_INFO *print_event_info) const {
  IO_CACHE *const head = &print_event_info->head_cache;

  if (!print_event_info->short_form) {
    print_header(head, print_event_info, false);
    my_b_printf(
        head,
        "\tGcn\thave_snapshot_seq=%s\thave_commit_seq=%s\tGCN_ASSIGNED=%s\n",
        (flags & FLAG_HAVE_SNAPSHOT_SEQ) ? "true" : "false",
        (flags & FLAG_HAVE_COMMITTED_SEQ) ? "true" : "false",
        (flags & FLAG_GCN_ASSIGNED) ? "ture" : "false");
  }

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    my_b_printf(head, "SET @@session.innodb_commit_seq=%llu%s\n",
                (ulonglong)commit_gcn, print_event_info->delimiter);
  }
}
#endif

#ifdef MYSQL_SERVER
int Gcn_log_event::do_apply_event(Relay_log_info const *rli) {
  DBUG_TRACE;
  MY_UNUSED(rli);
  assert(rli->info_thd == thd);

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    assert(commit_gcn != MYSQL_GCN_NULL);

    thd->variables.innodb_commit_gcn = commit_gcn;

    /** Set owned_commit_gcn in THD. */
    thd->owned_commit_gcn.set(commit_gcn, (flags & FLAG_GCN_ASSIGNED)
                                              ? MYSQL_CSR_ASSIGNED
                                              : MYSQL_CSR_AUTOMATIC);
  }

  return 0;
}

int Gcn_log_event::do_update_pos(Relay_log_info *rli) {
  /*
    This event does not increment group positions. This means
    that if there is a failure after it has been processed,
    it will be automatically re-executed.
  */
  rli->inc_event_relay_log_pos();
  return 0;
}

Log_event::enum_skip_reason Gcn_log_event::do_shall_skip(Relay_log_info *rli) {
  return Log_event::continue_group(rli);
}
#endif  // MYSQL_SERVER
