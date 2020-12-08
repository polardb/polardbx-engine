/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0mon.cc
  Lizard monitor metrics.

 Created 2020-06-03 by Jianwei.zhao
 *******************************************************/

#include "mysql/status_var.h"

#include "lizard0mon.h"
#include "lizard0sys.h"
#include "lizard0undo.h"

namespace lizard {

/** Global monitor metrics export status temporary address */
static lizard_var_t lizard_vars;

/** GLobal monitor metrics */
lizard_stats_t lizard_stats;

static void export_lizard_status(void) {
  if (lizard_sys != nullptr) {
    lizard_vars.txn_undo_log_free_list_len =
        lizard_sys->txn_undo_log_free_list_len;
    lizard_vars.txn_undo_log_cached = lizard_sys->txn_undo_log_cached;
  } else {
    lizard_vars.txn_undo_log_free_list_len = 0;
    lizard_vars.txn_undo_log_cached = 0;
  }

  lizard_vars.txn_undo_log_request = lizard_stats.txn_undo_log_request;
  lizard_vars.txn_undo_log_reuse = lizard_stats.txn_undo_log_reuse;

  lizard_vars.txn_undo_log_free_list_get =
      lizard_stats.txn_undo_log_free_list_get;

  lizard_vars.txn_undo_log_free_list_put =
      lizard_stats.txn_undo_log_free_list_put;

  lizard_vars.txn_undo_log_create = lizard_stats.txn_undo_log_create;

  lizard_vars.txn_undo_log_hash_element =
      lizard_stats.txn_undo_log_hash_element;
  lizard_vars.txn_undo_log_hash_hit = lizard_stats.txn_undo_log_hash_hit;
  lizard_vars.txn_undo_log_hash_miss = lizard_stats.txn_undo_log_hash_miss;

  lizard_vars.txn_undo_lost_page_miss_when_safe =
      lizard_stats.txn_undo_lost_page_miss_when_safe;

  lizard_vars.txn_undo_lost_magic_number_wrong =
      lizard_stats.txn_undo_lost_magic_number_wrong;

  lizard_vars.txn_undo_lost_ext_flag_wrong =
      lizard_stats.txn_undo_lost_ext_flag_wrong;

  lizard_vars.txn_undo_lost_trx_id_mismatch =
      lizard_stats.txn_undo_lost_trx_id_mismatch;

  lizard_vars.txn_undo_lookup_by_uba =
      lizard_stats.txn_undo_lookup_by_uba;

  lizard_vars.cleanout_page_collect = lizard_stats.cleanout_page_collect;

  lizard_vars.cleanout_record_clean = lizard_stats.cleanout_record_clean;

  lizard_vars.cleanout_cursor_collect = lizard_stats.cleanout_cursor_collect;

  lizard_vars.cleanout_cursor_restore_failed =
      lizard_stats.cleanout_cursor_restore_failed;
}

static SHOW_VAR lizard_status_variables[] = {
    {"txn_undo_log_free_list_length",
     (char *)&lizard_vars.txn_undo_log_free_list_len, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_cached", (char *)&lizard_vars.txn_undo_log_cached, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_request", (char *)&lizard_vars.txn_undo_log_request,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_reuse", (char *)&lizard_vars.txn_undo_log_reuse, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_free_list_get",
     (char *)&lizard_vars.txn_undo_log_free_list_get, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_free_list_put",
     (char *)&lizard_vars.txn_undo_log_free_list_put, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_create", (char *)&lizard_vars.txn_undo_log_create, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_element",
     (char *)&lizard_vars.txn_undo_log_hash_element, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_hit", (char *)&lizard_vars.txn_undo_log_hash_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_miss", (char *)&lizard_vars.txn_undo_log_hash_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_page_miss_when_safe",
     (char *)&lizard_vars.txn_undo_lost_page_miss_when_safe, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_magic_number_wrong",
     (char *)&lizard_vars.txn_undo_lost_magic_number_wrong, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_ext_flag_wrong",
     (char *)&lizard_vars.txn_undo_lost_ext_flag_wrong, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_trx_id_mismatch",
     (char *)&lizard_vars.txn_undo_lost_trx_id_mismatch, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lookup_by_uba", (char *)&lizard_vars.txn_undo_lookup_by_uba,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_page_collect", (char *)&lizard_vars.cleanout_page_collect,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_record_clean", (char *)&lizard_vars.cleanout_record_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_cursor_collect", (char *)&lizard_vars.cleanout_cursor_collect,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_cursor_restore_failed",
     (char *)&lizard_vars.cleanout_cursor_restore_failed, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"undo_retention_stats", Undo_retention::status, SHOW_CHAR,
    SHOW_SCOPE_GLOBAL},

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

int show_lizard_vars(THD *thd, SHOW_VAR *var, char *buff) {
  export_lizard_status();

  var->type = SHOW_ARRAY;
  var->value = (char *)&lizard_status_variables;
  var->scope = SHOW_SCOPE_GLOBAL;

  return 0;
}

}  // namespace lizard
