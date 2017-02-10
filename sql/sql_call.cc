/* Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */


/* Execute CALL statement */

#include "sql/sql_call.h"

#include <limits.h>
#include <stddef.h>
#include <sys/types.h>
#include <algorithm>

#include "auth_acls.h"
#include "auth_common.h"        // check_routine_access, check_table_access
#include "item.h"               // class Item
#include "my_base.h"
#include "my_dbug.h"
#include "my_global.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/plugin_audit.h"
#include "mysql_com.h"
#include "mysqld_error.h"
#include "protocol.h"
#include "sp.h"                 // sp_find_routine
#include "sp_head.h"
#include "sp_pcontext.h"        // class sp_variable
#include "sql_audit.h"          // AUDIT_EVENT
#include "sql_class.h"          // class THD
#include "sql_lex.h"
#include "sql_list.h"
#include "sql_plugin.h"
#include "system_variables.h"
#include "template_utils.h"

using std::max;

bool Sql_cmd_call::precheck(THD *thd)
{
  // Check execute privilege on stored procedure
  if (check_routine_access(thd, EXECUTE_ACL, proc_name->m_db.str,
                           proc_name->m_name.str,
                           true, false))
    return true;

  // Check SELECT privileges for any subqueries
  if (check_table_access(thd, SELECT_ACL, lex->query_tables, false,
                         UINT_MAX, false))
    return true;
  return false;
}

bool Sql_cmd_call::prepare_inner(THD *thd)
{
  // All required SPs should be in cache so no need to look into DB. 

  sp_head *sp= sp_find_routine(thd, enum_sp_type::PROCEDURE, proc_name,
                               &thd->sp_proc_cache, true);
  if (sp == NULL)
  {
    my_error(ER_SP_DOES_NOT_EXIST, MYF(0), "PROCEDURE",
             proc_name->m_qname.str);
    return true;
  }

  if (proc_args == NULL)
    return false;

  List_iterator<Item> it(*proc_args);
  Item *item;
  int arg_no= 0;
  while ((item= it++))
  {
    if (item->type() == Item::TRIGGER_FIELD_ITEM)
    {
      Item_trigger_field *itf= down_cast<Item_trigger_field *>(item);
      sp_variable *spvar= sp->get_root_parsing_context()->find_variable(arg_no);
      if (spvar->mode != sp_variable::MODE_IN)
        itf->set_required_privilege(spvar->mode == sp_variable::MODE_INOUT);
    }
    if ((!item->fixed && item->fix_fields(thd, it.ref())) ||
        item->check_cols(1))
      return true;              /* purecov: inspected */
    arg_no ++;
  }

  return false;
}

bool Sql_cmd_call::execute_inner(THD *thd)
{

  // All required SPs should be in cache so no need to look into DB. 

  sp_head *sp= sp_setup_routine(thd, enum_sp_type::PROCEDURE, proc_name,
                                &thd->sp_proc_cache);
  if (sp == NULL)
  {
    my_error(ER_SP_DOES_NOT_EXIST, MYF(0), "PROCEDURE",
             proc_name->m_qname.str);
    return true;
  }

  // bits to be cleared in thd->server_status
  uint bits_to_be_cleared= 0;
  /*
    Check that the stored procedure doesn't contain Dynamic SQL and doesn't
    return result sets: such stored procedures can't be called from
    a function or trigger.
  */
  if (thd->in_sub_stmt)
  {
    const char *where= (thd->in_sub_stmt & SUB_STMT_TRIGGER ?
                        "trigger" : "function");
    if (sp->is_not_allowed_in_function(where))
      return true;
  }

  if (mysql_audit_notify(thd,
                         AUDIT_EVENT(MYSQL_AUDIT_STORED_PROGRAM_EXECUTE),
                         proc_name->m_db.str,
                         proc_name->m_name.str,
                         NULL))
    return true;

  if (sp->m_flags & sp_head::MULTI_RESULTS)
  {
    if (!thd->get_protocol()->has_client_capability(CLIENT_MULTI_RESULTS))
    {
      // Client does not support multiple result sets
      my_error(ER_SP_BADSELECT, MYF(0), sp->m_qname.str);
      return true;
    }
    /*
      If SERVER_MORE_RESULTS_EXISTS is not set,
      then remember that it should be cleared
    */
    bits_to_be_cleared= (~thd->server_status & SERVER_MORE_RESULTS_EXISTS);
    thd->server_status|= SERVER_MORE_RESULTS_EXISTS;
  }

  ha_rows select_limit= thd->variables.select_limit;
  thd->variables.select_limit= HA_POS_ERROR;

  /*
    Never write CALL statements into binlog:
    - If the mode is non-prelocked, each statement will be logged separately.
    - If the mode is prelocked, the invoking statement will care about writing
      into binlog.
    So just execute the statement.
  */
  bool result= sp->execute_procedure(thd, proc_args);

  thd->variables.select_limit= select_limit;

  thd->server_status&= ~bits_to_be_cleared;

  if (result)
  {
    DBUG_ASSERT(thd->is_error() || thd->killed);
    return true;              // Substatement should already have sent error
  }

  my_ok(thd, max(thd->get_row_count_func(), 0LL));

  return false;
}
