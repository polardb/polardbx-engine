/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

//
// Created by 0xCC on 2024/11/26.
//

#include <mutex>
#include <string>

#include <openssl/rand.h>

#include "plugin/polarx_rpc/global_defines.h"

#include "mutex_lock.h"
#include "mysql_com.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/derror.h"
#include "sql/sql_class.h"
#include "sql/sql_db.h"

#include "proxy_proc.h"

// proc for proxy
namespace im {

LEX_CSTRING PROXY_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_proxy")};

Proc *Proc_reset_db::instance() {
  static auto *proc = new Proc_reset_db(key_memory_package);
  return proc;
}

#ifdef MYSQL8PLUS
Sql_cmd *Proc_reset_db::invoke_cmd(THD *thd,
                                   mem_root_deque<Item *> *list) const {
#else
Sql_cmd *Proc_reset_db::invoke_cmd(THD *thd, List<Item> *list) const {
#endif
  return new (thd->mem_root) Cmd_reset_db(thd, list, this);
}

bool Cmd_reset_db::pc_execute(THD *thd) {
  LEX_CSTRING null_db{nullptr, 0};
  return mysql_change_db(thd, null_db, true);
}


Proc *Proc_ping::instance() {
  static auto *proc = new Proc_ping(key_memory_package);
  return proc;
}

Sql_cmd *Proc_ping::invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Cmd_ping(thd, list, this);
}

bool Cmd_ping::pc_execute(THD *thd) {
  uint64_t check_result = is_ping_not_matched(
    thd->variables.ping_mode, &thd->status_var.last_cluster_change_version);
  if (!check_result)
    return false;

  my_error(ER_PING_CHECK_FAILED, MYF(0), 
            thd->variables.ping_mode,
            get_ping_mode_name(check_result));
  return true;
}

std::once_flag g_token_once;
std::string g_token;

static const char codes[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

Proc *Proc_get_token::instance() {
  static auto *proc = new Proc_get_token(key_memory_package);
  std::call_once(g_token_once, []() {
    g_token.resize(20);
    auto buffer = g_token.data();
    const int buffer_len = static_cast<int>(g_token.size());
    const auto end = buffer + buffer_len;
    RAND_bytes((unsigned char *)buffer, buffer_len);
    for (; buffer < end; buffer++)
      *buffer = codes[static_cast<uint8_t>(*buffer) % (sizeof(codes) - 1)];
  });
  return proc;
}

#ifdef MYSQL8PLUS
Sql_cmd *Proc_get_token::invoke_cmd(THD *thd,
                                    mem_root_deque<Item *> *list) const {
#else
Sql_cmd *Proc_get_token::invoke_cmd(THD *thd, List<Item> *list) const {
#endif
  return new (thd->mem_root) Cmd_get_token(thd, list, this);
}

void Cmd_get_token::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();

  /* No need to proceed if error occurred */
  if (error) return;

  if (m_proc->send_result_metadata(thd)) return;

  protocol->start_row();
  protocol->store(g_token.c_str(), system_charset_info);
  if (protocol->end_row()) return;

  my_eof(thd);
}

Proc *Proc_switch_user::instance() {
  static auto *proc = new Proc_switch_user(key_memory_package);
  return proc;
}

#ifdef MYSQL8PLUS
Sql_cmd *Proc_switch_user::invoke_cmd(THD *thd,
                                      mem_root_deque<Item *> *list) const {
#else
Sql_cmd *Proc_switch_user::invoke_cmd(THD *thd, List<Item> *list) const {
#endif
  return new (thd->mem_root) Cmd_switch_user(thd, list, this);
}

bool Cmd_switch_user::pc_execute(THD *thd) {
  /*
    LOCK_thd_security_ctx protects the THD's security-context from
    inspection by SHOW PROCESSLIST while we're updating it. Nested
    acquiring of LOCK_thd_data is fine (see below).
  */
  MUTEX_LOCK(grd_secctx, &thd->LOCK_thd_security_ctx);

  Security_context *sctx = thd->security_context();
  if (nullptr == sctx) {
    my_error(ER_NO, MYF(0));
    return true;
  }

#ifdef MYSQL8PLUS
  auto it = m_list->begin();
  auto token_item = dynamic_cast<Item_string *>(*(it++));
  auto user_item = dynamic_cast<Item_string *>(*(it++));
  auto host_item = dynamic_cast<Item_string *>(*(it++));
#else
  List_iterator_fast<Item> it(*m_list);
  auto token_item = dynamic_cast<Item_string *>(it++);
  auto user_item = dynamic_cast<Item_string *>(it++);
  auto host_item = dynamic_cast<Item_string *>(it++);
#endif
  String *token = token_item->val_str(nullptr);
  if (token->is_empty() || ::strcmp(token->ptr(), g_token.c_str()) != 0) {
    my_error(ER_ACCESS_DENIED_ERROR, MYF(0), sctx->priv_user().str,
             sctx->priv_host().str,
             (thd->password ? ER_THD(thd, ER_YES) : ER_THD(thd, ER_NO)));
    return true;
  }

  String *user = user_item->val_str(nullptr);
  String *host = host_item->val_str(nullptr);
  if (user->is_empty() || host->is_empty()) return true;

  sctx->assign_user(user->ptr(), user->length());
  sctx->assign_host(host->ptr(), host->length());
  sctx->assign_ip(nullptr, 0);
  thd->peer_port = 0;  // remove port info
  sctx->set_host_or_ip_ptr();

  // try switch user
  if (acl_getroot(thd, sctx, sctx->user().str, sctx->host().str, nullptr,
                  nullptr)) {
    my_error(ER_ACCESS_DENIED_ERROR, MYF(0), sctx->priv_user().str,
             sctx->priv_host().str,
             (thd->password ? ER_THD(thd, ER_YES) : ER_THD(thd, ER_NO)));
    return true;
  }
  const auto sctx_thd = sctx->get_thd();
  if (sctx_thd != nullptr) {
    set_system_user_flag(sctx_thd);
    set_connection_admin_flag(sctx_thd);
  }

  return false;
}

}  // namespace im
