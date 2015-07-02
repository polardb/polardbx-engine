/* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

#include "my_global.h"
#include "mysql/plugin_audit.h"
#include "mysql/service_rules_table.h"
#include "sql_cache.h"
#include "sql_error.h"
#include "sql_parse.h"
#include "sql_plugin.h"
#include "sql_query_rewrite.h"
#include "log.h"
#include "sql_base.h"
#include "sql_class.h"
#include "sql_audit.h"

#ifndef EMBEDDED_LIBRARY
void raise_query_rewritten_note(THD *thd,
                                const char *original_query,
                                const char *rewritten_query)
{
  Sql_condition::enum_severity_level sl= Sql_condition::SL_NOTE;
  const char *message= "Query '%s' rewritten to '%s' by a query rewrite plugin";
  push_warning_printf(thd, sl, ER_UNKNOWN_ERROR, message,
                      original_query, rewritten_query);
};


void invoke_pre_parse_rewrite_plugins(THD *thd)
{
  Diagnostics_area *plugin_da= thd->get_query_rewrite_plugin_da();
  if (plugin_da == NULL)
    return;
  plugin_da->reset_condition_info(thd);
  Diagnostics_area *da= thd->get_parser_da();
  thd->push_diagnostics_area(plugin_da, false);
  int flags= 0;
  char *rewritten_query= NULL;
  size_t rewritten_query_length= 0;
  mysql_audit_notify(thd, MYSQL_AUDIT_PARSE_CLASS, MYSQL_AUDIT_PREPARSE,
                     &flags, //flags
                     thd->query().str, // query
                     thd->query().length, // length
                     &rewritten_query,
                     &rewritten_query_length);
  if (flags & FLAG_REWRITE_PLUGIN_QUERY_REWRITTEN)
  {
    // It is a rewrite fulltext plugin and we need a rewrite we must have
    // generated a new query then.
    DBUG_ASSERT(rewritten_query != NULL &&
                rewritten_query_length > 0);
    raise_query_rewritten_note(thd, thd->query().str, rewritten_query);
    alloc_query(thd, rewritten_query, rewritten_query_length);
    thd->m_parser_state->init(thd, thd->query().str, thd->query().length);
    my_free(rewritten_query);
  }

  da->copy_sql_conditions_from_da(thd, plugin_da);

  thd->pop_diagnostics_area();
}


void enable_digest_if_any_plugin_needs_it(THD *thd, Parser_state *ps)
{
  if (is_audit_plugin_class_active(thd, MYSQL_AUDIT_PARSE_CLASS))
    ps->m_input.m_compute_digest= true;
}


bool invoke_post_parse_rewrite_plugins(THD *thd, my_bool is_prepared)
{
  Diagnostics_area *plugin_da= thd->get_query_rewrite_plugin_da();
  plugin_da->reset_diagnostics_area();
  plugin_da->reset_condition_info(thd);

  Diagnostics_area *stmt_da= thd->get_stmt_da();

  /*
    We save the value of keep_diagnostics here as it gets reset by
    push_diagnostics_area(), see below for use.
  */
  bool keeping_diagnostics= thd->lex->keep_diagnostics == DA_KEEP_PARSE_ERROR;

  thd->push_diagnostics_area(plugin_da, false);

  {
    /*
       We have to call a function in rules_table_service.cc, or the service
       won't be visible to plugins.
    */
#ifndef DBUG_OFF
    int dummy= 
#endif
      rules_table_service::
      dummy_function_to_ensure_we_are_linked_into_the_server();
    DBUG_ASSERT(dummy == 1);
  }

  int flags= is_prepared ? FLAG_REWRITE_PLUGIN_IS_PREPARED_STATEMENT : 0;
  bool err= false;
  const LEX_CSTRING original_query= thd->query();
  mysql_audit_notify(thd, MYSQL_AUDIT_PARSE_CLASS, MYSQL_AUDIT_POSTPARSE,
                     &flags,
                     original_query.str,
                     original_query.length,
                     NULL, // rewritten query,
                     NULL // rewritten query length
                     );

  if (flags & FLAG_REWRITE_PLUGIN_QUERY_REWRITTEN)
  {
    raise_query_rewritten_note(thd, original_query.str, thd->query().str);
    thd->lex->safe_to_cache_query= false;
  }

  if (plugin_da->current_statement_cond_count() != 0)
  {
    /*
      A plugin raised at least one condition. At this point these are in the
      plugin DA, and we should copy them to the statement DA. But before we do
      that, we may have to clear it as this DA may contain conditions from the
      previous statement. We have to clear it *unless* the statement is a
      diagnostics statement, in which case we keep everything: conditions from
      previous statements, parser conditions and plugin conditions. If this is
      not a diagnostics statement, parse_sql() has already cleared the
      statement DA, copied the parser condtitions to the statement DA and set
      DA_KEEP_PARSE_ERROR. So we arrive at the below condition for telling us
      when to clear the statement DA.
    */
    if (thd->lex->sql_command != SQLCOM_SHOW_WARNS && !keeping_diagnostics)
      stmt_da->reset_condition_info(thd);

    /* We need to put any errors in the DA as well as the condition list. */
    if (plugin_da->is_error())
      stmt_da->set_error_status(plugin_da->mysql_errno(),
                                plugin_da->message_text(),
                                plugin_da->returned_sqlstate());

    stmt_da->copy_sql_conditions_from_da(thd, plugin_da);

    /*
      Do not clear the condition list when starting execution as it now
      contains not the results of the previous executions, but a non-zero
      number of errors/warnings thrown during parsing or plugin execution.
    */
    thd->lex->keep_diagnostics= DA_KEEP_PARSE_ERROR;
  }

  thd->pop_diagnostics_area();

  return err;
}


#else /* EMBEDDED_LIBRARY */

void invoke_pre_parse_rewrite_plugins(THD *thd) {}

void enable_digest_if_any_plugin_needs_it(THD *thd, Parser_state *ps) {}

bool invoke_post_parse_rewrite_plugins(THD *thd, my_bool is_prepared)
{
  return false;
}

#endif /* EMBEDDED_LIBRARY */
