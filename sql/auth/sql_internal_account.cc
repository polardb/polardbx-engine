/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.
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
/**
  @file

  Implementation of internal account management.

  Include:

  1. Independent xxx_user_list and xxx_max_connections variable for every kind
     of account
  2. Prefined ACLs or need-protect attributes for every kind of account
*/

#include "sql/auth/sql_internal_account.h"
#include "sql/auth/auth_acls.h"                           // NO_ACCESS
#include "sql/auth/sql_security_ctx.h"                    // Security_context
#include "sql/conn_handler/connection_handler_manager.h"  // Connection_handler_manager
#include "sql/mysqld.h"
#include "sql/psi_memory_key.h"  // key_memory_IA_mem
#include "sql/sql_class.h"       // THD

/**
  @addtogroup Internal Account Management

  The internal account management implementation

  @{
*/

namespace im {

static PSI_rwlock_key key_rwlock_IA_lock;
static PSI_mutex_key key_mutex_IA_lock;
static PSI_cond_key key_cond_IA_connection_count;

PSI_mutex_key key_LOCK_internal_account_string;
mysql_mutex_t LOCK_internal_account_string;

/* Global Internal Account configuration object */
Internal_account_config ia_config;

/* The static struct for looping and checking account type */
/**
  Revision history:

   - maintain user has inner DB acl.
*/
static const struct ST_ACCOUNT_INFO internal_accounts_info[] = {
    {"Maintain_user", IA_type::MAINTENACE_USER,
     Internal_account_config::MAINTENACE_ACL |
         Internal_account_config::INNER_DB_ACL,
     true},
    {"Kill_user", IA_type::KILL_USER, Internal_account_config::KILL_ACL, false},
    {"Inner_user", IA_type::INNER_USER, Internal_account_config::INNER_DB_ACL,
     true},
    {"", IA_type::LAST_USER, Internal_account_config::NO_ACL, false}};

/* Begin: Class Internal_account_config member function definition */

/**
  Default Construtor
*/
Internal_account_config::Internal_account_config() {
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    user_set(iter, nullptr);
    conn_set(iter, 0);
  }
}

/**
  How many connection count are left for the normal user
*/
ulonglong Internal_account_config::available_connections() {
  ulonglong sum_count = 0;
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    sum_count += connections[*iter];
  }

  long left;
  if ((left = (max_connections - sum_count)) > 0) return left;

  return max_connections;
}

/**
  Get predefined description for the user type
*/
const char *Internal_account_config::str(User_type type) {
  if (type == UNKNOWN_USER) return "Normal_user";

  return internal_accounts_info[type].name;
}
/* End: Class Internal_account_config member function definition */

/* begin: Class Internal_account_ctx member function definition */

/**
  Singleton internal account management object
*/
Internal_account_ctx *Internal_account_ctx::instance() {
  static Internal_account_ctx IA_ctx;
  return &IA_ctx;
}

/**
  Init the global account management instance.
*/
void Internal_account_ctx::init() {
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    instance()->build_array(*iter);
  }
}

/**
  Constructor: initialize the properties
*/
Internal_account_ctx::Internal_account_ctx() : m_accounts() {
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    m_accounts.push_back(new Account_array(key_memory_IA_mem));
    mysql_rwlock_init(key_rwlock_IA_lock, &m_rwlock[*iter]);
    mysql_mutex_init(key_mutex_IA_lock, &m_mutex[*iter], MY_MUTEX_INIT_FAST);
    mysql_cond_init(key_cond_IA_connection_count, &m_cond[*iter]);
    connections[*iter] = 0;
  }
}

/**
  Destructor: destroy the properties
*/
Internal_account_ctx::~Internal_account_ctx() {
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    delete m_accounts[*iter];
    mysql_rwlock_destroy(&m_rwlock[*iter]);
    mysql_mutex_destroy(&m_mutex[*iter]);
    mysql_cond_destroy(&m_cond[*iter]);
  }
  m_accounts.clear();
}

/**
  Build the user account array by original string.
*/
void Internal_account_ctx::build_array(IA_type type) {
  Account_array *arr;
  std::string::size_type pos1, pos2;
  assert(type != IA_type::UNKNOWN_USER && type != IA_type::LAST_USER);

  IA_iterator iter(type);
  arr = m_accounts[*iter];

  Auto_user_lock lock(this, *iter, true);
  assert(lock.effect());
  arr->clear();

  if (ia_config.user_at(iter) == nullptr || ia_config.user_at(iter)[0] == '\0')
    return;

  std::string t_str(ia_config.user_at(iter));
  pos1 = 0;
  pos2 = t_str.find(SEPARATOR);
  while (pos2 != std::string::npos) {
    arr->push_back(t_str.substr(pos1, pos2 - pos1));

    pos1 = pos2 + strlen(SEPARATOR);
    pos2 = t_str.find(SEPARATOR, pos1);
  }
  arr->push_back(t_str.substr(pos1));
}
/**
  Judge whether the account exist in array
*/
bool Internal_account_ctx::exist_account(IA_type type, const char *account) {
  Account_array *arr;
  assert(type != IA_type::UNKNOWN_USER && type != IA_type::LAST_USER);

  if (account == nullptr || account[0] == '\0') return false;

  arr = m_accounts[type];

  Auto_user_lock lock(this, type, false);
  assert(lock.effect());
  for (Account_array::iterator it = arr->begin(); it != arr->end(); it++) {
    if ((*it).compare(account) == 0) return true;
  }
  return false;
}

/**
  Get active connection count
*/
ulonglong Internal_account_ctx::active_connection_count(IA_type type) {
  IA_iterator iter(type);
  Auto_conn_lock lock(this, *iter);
  assert(lock.effect());
  return connections[*iter];
}
/**
  Judge whether the account need to be protected
*/
bool Internal_account_ctx::protected_account(const char *account) {
  const ST_ACCOUNT_INFO *st = internal_accounts_info;
  while (st->type != IA_type::LAST_USER) {
    if (st->need_protected == true && exist_account(st->type, account)) {
      return true;
    }
    st++;
  }
  return false;
}

/**
  Init the internal account acl after authentication
*/
void Internal_account_ctx::init_account_acl(Security_context *sctx,
                                            const char *account) {
  const ST_ACCOUNT_INFO *st = internal_accounts_info;

  /* Here should reset account context */
  sctx->account_attr.reset();

  while (st->type != IA_type::LAST_USER) {
    if (exist_account(st->type, account)) {
      sctx->account_attr.set_acl(st->type, st->acl);

      /* Maintenance users are grant with all privileges */
      if (st->type == IA_type::MAINTENACE_USER)
        sctx->set_master_access(~NO_ACCESS);
    }
    st++;
  }
}

/**
  Adjust the active connection count according to
  connection group setting after authentication.
*/
bool Internal_account_ctx::adjust_connection(THD *thd,
                                             enum_server_command command,
                                             bool *adjusted) {
  *adjusted = false;
  Security_context *sctx = thd->security_context();
  IA_iterator sa_iter(sctx->account_attr.get_type());
  ulonglong conn_limit;
  assert(thd->conn_attr.get() == IA_type::UNKNOWN_USER);
  assert(command == COM_CONNECT || command == COM_CHANGE_USER);

  /** Do nothing if the account is normal user or hasn't been authenticated */
  if (*sa_iter == IA_type::UNKNOWN_USER) return false;

  /** Do nothing if the account didn't set xx_max_connections */
  if ((conn_limit = ia_config.conn_at(sa_iter)) == 0) return false;

  /**
    KILL_USER can use normal user's connections and
    ia_config[IA_type::KILL_USER] If !exceed_max_connection(thd) which means
    normal user's connections has not been run out, this KILL_USER connection
    will be treated as normal user. If exceed_max_connection(thd) which means
    normal user's connections has been run out, this KILL_USER connection will
    be added to ia_ctx[IA_type::KILL_USER]
  */

  /**
    Revision 1:
      Redesign the kill user connection management:
       a) Security_context attribute still is treated as KILL_USER and have
    KILL_ACL. b) THD connection will be treated as UNKOWN_USER and use the max
    connections. c) Normal user only can use max_connection -
    kill_user_connections. d) Kill user can use max_connection
  */
  if (*sa_iter == IA_type::KILL_USER) return false;

  /** Connection type will be used to decrease connection count */
  Auto_conn_lock lock(this, *sa_iter);
  assert(lock.effect());
  if ((connections[*sa_iter]++) < conn_limit) {
    // Maybe COM_CONNECT or COM_CHANGE_USER
    if (command == COM_CONNECT) {
      Connection_handler_manager::dec_connection_count();
    }

    thd->conn_attr.set(*sa_iter);
    *adjusted = true;
    return false;
  } else {
    connections[*sa_iter]--;
    if (connections[*sa_iter] == 0) {
      mysql_cond_signal(&m_cond[*sa_iter]);
    }
    thd->conn_attr.set(IA_type::UNKNOWN_USER);
    return true;
  }
}
/**
  Change the connection count when change user
*/
bool Internal_account_ctx::change_connection(THD *thd,
                                             enum_server_command command) {
  bool adjusted;
  /**
     conn_type maybe not UNKNOWN_USER
     current sctx is the new priv user context
  */
  Security_context *sctx = thd->security_context();

  IA_iterator new_sa_iter(sctx->account_attr.get_type());
  IA_iterator bak_conn_iter(thd->conn_attr.get());
  assert(command == COM_CHANGE_USER);

  /* Do nothing if the new authed account type equal the origin connect type */
  if (*new_sa_iter == *bak_conn_iter) return false;

  thd->conn_attr.reset();
  bool error = adjust_connection(thd, command, &adjusted);
  if (error) {
    thd->conn_attr.set(*bak_conn_iter);
    return true;
  } else {
    /** If not adjusted within adjust_connection(),
        It means that the count is calculated through connection_count,
        so we should incr the global connection count.
    */
    if (!adjusted &&
        !Connection_handler_manager::get_instance()->check_and_incr_conn_count(
            false)) {
      thd->conn_attr.set(*bak_conn_iter);
      return true;
    }
    IA_iterator new_conn_iter(thd->conn_attr.get());
    thd->conn_attr.set(*bak_conn_iter);
    dec_connection_count(thd);
    thd->conn_attr.set(*new_conn_iter);
    return false;
  }
}

/**
  Decrease the connection count
*/
bool Internal_account_ctx::dec_connection_count(THD *thd) {
  if (thd == nullptr) {
    Connection_handler_manager::dec_connection_count();
    return false;
  }
  /** Attention:
      We should check connection type other than security account type.
  */
  IA_iterator iter(thd->conn_attr.get());
  if (*iter == IA_type::UNKNOWN_USER) {
    Connection_handler_manager::dec_connection_count();
  } else {
    Auto_conn_lock lock(this, *iter);
    assert(lock.effect());
    assert(connections[*iter] > 0);
    connections[*iter]--;
    if (connections[*iter] == 0) mysql_cond_signal(&m_cond[*iter]);
  }
  return false;
}
/**
   Check whether the normal user connection have already exceeded
   max available connections.
*/
bool Internal_account_ctx::exceed_max_connection(THD *thd) {
  uint extra_connection = 0;
  uint available_connection = 0;

  available_connection = ia_config.available_connections();

  /* Allowed to have an extra connection for super account */
  if (thd->m_main_security_ctx.check_access(SUPER_ACL) ||
      thd->m_main_security_ctx
          .has_global_grant(STRING_WITH_LEN("CONNECTION_ADMIN"))
          .first)
    extra_connection = 1;

  /* If user has kill acl, then can have extra connections. */
  if (thd->m_main_security_ctx.account_attr.get_type() == IA_type::KILL_USER)
    available_connection += ia_config.connections[IA_type::KILL_USER];

  if ((thd->conn_attr.get() == IA_type::UNKNOWN_USER) &&
      (Connection_handler_manager::get_connection_count() >
       (available_connection + extra_connection)))
    return true;

  return false;
}

ulonglong Internal_account_ctx::get_connection_count() {
  ulonglong sum_count = 0;
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    Auto_conn_lock lock(this, *iter);
    assert(lock.effect());
    sum_count += connections[*iter];
  }

  sum_count += Connection_handler_manager::get_connection_count();

  return sum_count;
}

/**
  Wait all the internal account connection exit
*/
void Internal_account_ctx::wait_till_no_connection() {
  for (IA_iterator iter = ia_config.begin(); iter != ia_config.end(); iter++) {
    Auto_conn_lock lock(this, *iter);
    assert(lock.effect());
    while (connections[*iter] > 0)
      mysql_cond_wait(&m_cond[*iter], &m_mutex[*iter]);
  }
}
/* End: Class Internal_account_ctx member function definition */

// Global interface
void internal_account_ctx_init() { Internal_account_ctx::instance()->init(); }
void internal_account_ctx_destroy() {
  // Nothing to do
  // The singleton instance will be freed when mysqld exit;
}

/* Judge whether the account needed to be protected according to account name */
bool internal_account_need_protected(const char *account) {
  return Internal_account_ctx::instance()->protected_account(account);
}
void internal_account_init_acl(Security_context *sctx, const char *account) {
  Internal_account_ctx::instance()->init_account_acl(sctx, account);
}
/* It's used when COM_CONNECT */
bool internal_account_adjust_connection(THD *thd, enum_server_command command) {
  bool adjusted = false;
  return Internal_account_ctx::instance()->adjust_connection(thd, command,
                                                             &adjusted);
}

ulonglong internal_account_active_connection_count(IA_type type) {
  return Internal_account_ctx::instance()->active_connection_count(type);
}
/* It's used when COM_CHANGE_USER */
bool internal_account_change_connection(THD *thd, enum_server_command command) {
  return Internal_account_ctx::instance()->change_connection(thd, command);
}

bool internal_account_exceed_max_connection(THD *thd) {
  return Internal_account_ctx::instance()->exceed_max_connection(thd);
}

bool global_manager_dec_connection(THD *thd) {
  return Internal_account_ctx::instance()->dec_connection_count(thd);
}

void global_manager_wait_no_connection() {
  Internal_account_ctx::instance()->wait_till_no_connection();
  Connection_handler_manager::wait_till_no_connection();
}

/**
  Display the maintain user active connection count.
*/
int show_maintain_connection_count(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long long *value = reinterpret_cast<long long *>(buff);
  *value = static_cast<long long>(
      internal_account_active_connection_count(IA_type::MAINTENACE_USER));
  return 0;
}

ulonglong get_total_connection_count() {
  return Internal_account_ctx::instance()->get_connection_count();
}

} /* namespace im */

/// @} (end of group Internal_Account_Management)
