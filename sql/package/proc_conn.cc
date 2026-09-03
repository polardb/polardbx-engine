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
#include "sql/mysqld_thd_manager.h"
#include "mutex_lock.h"

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


struct thread_info {
 public:
  thread_info()
      : thread_id(0),
        user(nullptr),
        host(nullptr),
        db(nullptr),
        comment(nullptr) {}

  my_thread_id thread_id;
  const char *user, *host, *db, *comment;
};

class thread_info_compare {
 public:
  bool operator()(const thread_info *p1, const thread_info *p2) {
    return p1->thread_id < p2->thread_id;
  }
};

typedef Mem_root_array<thread_info *> Thread_info_array;

class Conn_comment_list : public Do_THD_Impl {
  private:
  const char *m_user;
  Thread_info_array *m_thread_infos;
  /* THD of connected client. */
  THD *m_client_thd;

 public:
  explicit Conn_comment_list(const char *user_value,
                             Thread_info_array *thread_infos, THD *thd_value)
      : m_user(user_value),
        m_thread_infos(thread_infos),
        m_client_thd(thd_value) {}

  void operator()(THD *inspect_thd) override {
    thread_info *thd_info = nullptr;

    {
      MUTEX_LOCK(grd_secctx, &inspect_thd->LOCK_thd_security_ctx);

      Security_context *inspect_sctx = inspect_thd->security_context();

      LEX_CSTRING inspect_sctx_user = inspect_sctx->user();
      LEX_CSTRING inspect_sctx_host = inspect_sctx->host();
      LEX_CSTRING inspect_sctx_host_or_ip = inspect_sctx->host_or_ip();

      {
        MUTEX_LOCK(grd, &inspect_thd->LOCK_thd_protocol);

        if ((!(inspect_thd->get_protocol() &&
               inspect_thd->get_protocol()->connection_alive()) &&
             !inspect_thd->system_thread) ||
            (m_user && (inspect_thd->system_thread || !inspect_sctx_user.str ||
                        strcmp(inspect_sctx_user.str, m_user)))) {
          return;
        }
      }

      thd_info = new (m_client_thd->mem_root) thread_info;

      /* ID */
      thd_info->thread_id = inspect_thd->thread_id();

      /* USER */
      if (inspect_sctx_user.str)
        thd_info->user = m_client_thd->mem_strdup(inspect_sctx_user.str);
      else if (inspect_thd->system_thread)
        thd_info->user = "system user";
      else
        thd_info->user = "unauthenticated user";

      /* HOST */
      if (inspect_thd->peer_port &&
          (inspect_sctx_host.length || inspect_sctx->ip().length) &&
          m_client_thd->security_context()->host_or_ip().str[0]) {
        char *host =
            static_cast<char *>(m_client_thd->alloc(HOST_AND_PORT_LENGTH + 1));
        if (host)
          snprintf(host, HOST_AND_PORT_LENGTH + 1, "%s:%u",
                   inspect_sctx_host_or_ip.str, inspect_thd->peer_port);
        thd_info->host = host;
      } else
        thd_info->host = m_client_thd->mem_strdup(
            inspect_sctx_host_or_ip.str[0]
                ? inspect_sctx_host_or_ip.str
                : inspect_sctx_host.length ? inspect_sctx_host.str : "");
    }  // We've copied the security context, so release the lock.

    /* DB */
    mysql_mutex_lock(&inspect_thd->LOCK_thd_data);
    const char *db = inspect_thd->db().str;
    if (db) thd_info->db = m_client_thd->mem_strdup(db);

    /* JDBC_COMMENT */
    if (inspect_thd->conn_comment != nullptr) {
      thd_info->comment = m_client_thd->mem_strdup(inspect_thd->conn_comment);
    } else {
      thd_info->comment = m_client_thd->mem_strdup("");
    }
    mysql_mutex_unlock(&inspect_thd->LOCK_thd_data);

    m_thread_infos->push_back(thd_info);
}
};

void Sql_cmd_conn_show::send_result(THD *thd, bool error) {

  DBUG_ENTER("Sql_cmd_conn_show::send_result");
  Thread_info_array thread_infos(thd->mem_root);
  Protocol *protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  if (!thd->killed) {
    thread_infos.reserve(Global_THD_manager::get_instance()->get_thd_count());
    Conn_comment_list conn_comment_list(
        thd->security_context()->priv_user().str, &thread_infos, thd);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&conn_comment_list);
  }

  std::sort(thread_infos.begin(), thread_infos.end(), thread_info_compare());

  for (size_t ix = 0; ix < thread_infos.size(); ++ix) {
    thread_info *thd_info = thread_infos.at(ix);
    protocol->start_row();
    protocol->store((ulonglong)thd_info->thread_id);
    protocol->store(thd_info->user, system_charset_info);
    protocol->store(thd_info->host, system_charset_info);
    protocol->store(thd_info->db, system_charset_info);
    protocol->store(thd_info->comment, system_charset_info);
    if (protocol->end_row()) break;
  }
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