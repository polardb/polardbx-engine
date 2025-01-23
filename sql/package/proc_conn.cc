/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/package/proc_conn.h"
#include "sql/derror.h"

namespace im {

const LEX_CSTRING PROC_CONN_SCHEMA = {C_STRING_WITH_LEN("dbms_conn")};

Proc *Conn_proc_comment::instance() {
  static Proc *proc = new Conn_proc_comment(key_memory_package);

  return proc;
}

Sql_cmd *Conn_proc_comment::invoke_cmd(THD *thd,
                                      mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_conn_comment::pc_execute(THD *thd) {
  bool error = false;
  String *res = nullptr;
  DBUG_ENTER("Conn_proc_comment::pc_execute");

  char buff[1024];
  String str(buff, sizeof(buff), system_charset_info);
  res = (*m_list)[0]->val_str(&str);
  
  mysql_mutex_lock(&thd->LOCK_thd_query);

  if (thd->conn_comment != nullptr) {
    my_free(thd->conn_comment);
    thd->conn_comment = nullptr;
  }
  size_t alloc_size = res->length() + 1;
  thd->conn_comment = static_cast<char*>(my_malloc(PSI_NOT_INSTRUMENTED, alloc_size, MYF(0)));
  memcpy(thd->conn_comment, res->ptr(), res->length());
  thd->conn_comment[res->length()] = 0;

  mysql_mutex_unlock(&thd->LOCK_thd_query);
  DBUG_RETURN(error);
}

Proc *Conn_proc_show::instance() {
  static Proc *proc = new Conn_proc_show(key_memory_package);

  return proc;
}

Sql_cmd *Conn_proc_show::invoke_cmd(THD *thd,
                                   mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Show the jdbc comment in cache

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_conn_show::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_conn_show::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_conn_show::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("Sql_cmd_conn_show::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;
  protocol->start_row();

  mysql_mutex_lock(&thd->LOCK_thd_query);
  if (thd->conn_comment != nullptr) {
    protocol->store_string(thd->conn_comment, strlen(thd->conn_comment), system_charset_info);
  } else {
    protocol->store_string("", 0, system_charset_info);
  }
  mysql_mutex_unlock(&thd->LOCK_thd_query);

  if (protocol->end_row()) DBUG_VOID_RETURN;
  my_eof(thd);
  DBUG_VOID_RETURN;
}


Proc *Conn_proc_show_client_error_code::instance() {
  static Proc *proc = new Conn_proc_show_client_error_code(key_memory_package);

  return proc;
}

Sql_cmd *Conn_proc_show_client_error_code::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Show the client error code in cache

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_conn_show_client_error_code::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_conn_show_client_error_code::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_conn_show_client_error_code::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("Sql_cmd_conn_show_client_error_code::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  int index_start = errmsg_section_start[errmsg_section_index];
  for (int i = 0; i < errmsg_section_size[errmsg_section_index]; i++) {
    protocol->start_row();

    ulonglong error_code = index_start + i;
    protocol->store(error_code);
    const char *error_message = ER_DEFAULT(error_code);
    protocol->store_string(error_message, strlen(error_message), system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;

  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}


} /* namespace im */