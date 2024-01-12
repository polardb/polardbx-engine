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

#ifndef SQL_INTERNAL_ACCOUNT_INCLUDED
#define SQL_INTERNAL_ACCOUNT_INCLUDED

#include <stddef.h>
#include <vector>
#include "my_command.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_rwlock.h"  //mysql_rwlock_init
#include "mysql/status_var.h"
#include "prealloced_array.h"  //Prealloced_array
#include "sql/common/component.h"

class Security_context;

/**
  Internal Management strategy

  @Note Designed to deal with:
    1) Predefined internal account for management usage
    2) Refactor the privileges
    3) Group connection count according to account type
    4) Grant 'super kill' priv for KILL_USER specially

*/
namespace im {

/**
  All the internal account management configuration

*/
class Internal_account_config {
 public:
  /* Predefined Internal ACLs */
  static constexpr const ulonglong NO_ACL = 1L << 3;

  static constexpr const ulonglong MAINTENACE_ACL = 1L << 0;
  static constexpr const ulonglong KILL_ACL = 1L << 1;
  /* Allow to operate inner db if granted corresponding privileges */
  static constexpr const ulonglong INNER_DB_ACL = 1L << 2;

  /* Predefined Internal Account Type */
  enum User_type {
    UNKNOWN_USER = -1,    // For normal user
    MAINTENACE_USER = 0,  // Maintenance user that have all privileges
    KILL_USER,            // Special user who can kill all non-super connections
    INNER_USER,           // User specific allowed Inner DB operation
    LAST_USER             // Last user for ending enum
  };

  /* Account string type  */
  typedef char *User_value_type;

  /* Connection count type */
  typedef ulonglong Conn_value_type;

  /**
    Internal user_type enum iteration
  */
  class Iterator {
   public:
    /* Iterator operator* value */
    typedef User_type value_type;

    /**
      Iterator Construtor
    */
    Iterator(User_type type) { m_type = type; }
    /**
      Overload operator *
    */
    value_type operator*() { return m_type; }
    /**
      Overload operator ++
    */
    Iterator &operator++(int) {
      if (m_type != LAST_USER) m_type = static_cast<User_type>(m_type + 1);
      return *this;
    }
    /**
      Overload operator ==
      As the member function
    */
    bool operator==(const Iterator &iter) const {
      if (this->m_type == iter.m_type) return true;
      return false;
    }
    /**
      Overload operator !=
    */
    bool operator!=(const Iterator &iter) const { return !operator==(iter); }

   private:
    User_type m_type;
  }; /* Class Iterator */

 public:
  Internal_account_config();

  /**
    The enumeration begin function
  */
  Iterator begin() {
    Iterator iter(MAINTENACE_USER);
    return iter;
  }
  /**
    The enumeration end function
  */
  Iterator end() {
    Iterator iter(LAST_USER);
    return iter;
  }
  /**
    Position the user string
  */
  User_value_type user_at(Iterator &iter) { return user_str[*iter]; }
  /**
    Position the connection count value
  */
  Conn_value_type conn_at(Iterator &iter) { return connections[*iter]; }
  /**
    Set user string value
  */
  void user_set(Iterator &iter, User_value_type value) {
    user_str[*iter] = value;
  }
  /**
    Set connection count value
  */
  void conn_set(Iterator &iter, Conn_value_type value) {
    connections[*iter] = value;
  }
  /**
    How many connection count are left for the normal user

    @returns available connection count
  */
  ulonglong available_connections();
  /**
    Get predefined description for the user type

    @param[in] type   internal account type

    @returns prefined account desc
  */
  const static char *str(User_type type);

 public:
  /* Global variables which are referenced within sql/sys_var_ext.cc */
  char *user_str[LAST_USER];
  ulonglong connections[LAST_USER];

}; /* Class Internal_account_config */

/* Global type declaration */
typedef Internal_account_config::User_type IA_type;

typedef Internal_account_config::Iterator IA_iterator;

/* Internal management for account */
class Internal_account_ctx {
 public:
  static constexpr const size_t IA_PREALLOC_SIZE = 10;
  static constexpr const auto SEPARATOR = ",";

  typedef Prealloced_array<std::string, IA_PREALLOC_SIZE> Account_array;

  Internal_account_ctx();
  virtual ~Internal_account_ctx();

  /* Disable the copy and assign function */
  Internal_account_ctx(const Internal_account_ctx &) = delete;
  Internal_account_ctx(const Internal_account_ctx &&) = delete;
  Internal_account_ctx &operator=(const Internal_account_ctx &) = delete;

  /* Helper for locking/releasing lock */
  class Auto_lock : public Disable_unnamed_object {
   public:
    explicit Auto_lock(Internal_account_ctx *ia_ctx, IA_type type)
        : Disable_unnamed_object() {
      m_ctx = ia_ctx;
      m_type = type;
    }

    virtual ~Auto_lock() {}

   protected:
    Internal_account_ctx *m_ctx;
    IA_type m_type;
  };

  /* RWlock that protect account array */
  class Auto_user_lock : public Auto_lock {
   public:
    explicit Auto_user_lock(Internal_account_ctx *ia_ctx, IA_type type,
                            bool exclusive)
        : Auto_lock(ia_ctx, type) {
      if (exclusive) {
        mysql_rwlock_wrlock(&(m_ctx->m_rwlock[m_type]));
      } else {
        mysql_rwlock_rdlock(&(m_ctx->m_rwlock[m_type]));
      }
    }
    virtual ~Auto_user_lock() {
      mysql_rwlock_unlock(&(m_ctx->m_rwlock[m_type]));
    }
  };
  /* Mutex that protect account connection count */
  class Auto_conn_lock : public Auto_lock {
   public:
    explicit Auto_conn_lock(Internal_account_ctx *ia_ctx, IA_type type)
        : Auto_lock(ia_ctx, type) {
      mysql_mutex_lock(&(m_ctx->m_mutex[m_type]));
    }
    virtual ~Auto_conn_lock() { mysql_mutex_unlock(&(m_ctx->m_mutex[m_type])); }
  };

  /* Singleton internal account management object */
  static Internal_account_ctx *instance();

  /**
    Init the global account management instance.
  */
  void init();
  /**
    Destroy the global static account management instance when shutdown.
  */
  void destroy();
  /**
    Build the user account array by original string.

    @param[in]  type    user account type
  */
  void build_array(IA_type type);
  /**
    Init the internal account acl after authentication

    @param[in]  sctx      THD security context
    @param[in]  account   user account string
  */
  void init_account_acl(Security_context *sctx, const char *account);
  /**
    Judge whether the account exist in array

    @param[in]  type      user account type
    @param[in]  account   account string

    @retval     true      exist
    @retval     false     not exist
  */
  bool exist_account(IA_type type, const char *account);

  /**
    Judge whether the account need to be protected

    @param[in]  account   account string

    @retval     true      necessary
    @retval     false     unnecessary
  */
  bool protected_account(const char *account);

  /**
    For the THD, there are two IA_type properties:
      1) Connection type
          It's used to inc/dec the connection count.
      2) Security account type
          It's used to authenticate user type.
  */

  /**
    Get active connection count

    @param[in]  type    user account type

    @retval     connection count
  */
  ulonglong active_connection_count(IA_type type);

  /**
    Adjust the active connection count according to
    connection group setting after authentication.

    @param[in]  thd       User connection context
    @param[in]  command   server command
    @param[out] adjusted  whether do the adjustion

    @retval     true      failure
    @retval     false     success
  */
  bool adjust_connection(THD *thd, enum_server_command command, bool *adjusted);

  /**
    Decrease the connectin count

    @param[in]  thd       user connection context

    @retval     true      failure
    @retval     false     success
  */
  bool dec_connection_count(THD *thd);

  /**
    Wait all the internal account connection exit
  */
  void wait_till_no_connection();

  /**
    Change the connection count when change user

    @param[in]  thd       user connection context
    @param[in]  command   server command

    @retval     true      failure
    @retval     false     success
  */
  bool change_connection(THD *thd, enum_server_command command);

  /**
    Check whether the normal user connection have already exceeded
    max available connections.

    @param[in]  thd     user connection context

    @retval     true    exceed the max available connections
    @retval     false   still available
  */
  bool exceed_max_connection(THD *thd);

  ulonglong get_connection_count();

 private:
  std::vector<Account_array *> m_accounts;
  /* Protect account list */
  mysql_rwlock_t m_rwlock[IA_type::LAST_USER];

  ulonglong connections[IA_type::LAST_USER];
  /* Protect connection count */
  mysql_mutex_t m_mutex[IA_type::LAST_USER];

  mysql_cond_t m_cond[IA_type::LAST_USER];

}; /* Class Internal_account_ctx */

/* The struct of predefined account properties */
struct ST_ACCOUNT_INFO {
  const char *name;
  IA_type type;
  ulonglong acl;
  bool need_protected;
};

/* The account attributes of security_context */
struct ST_ACCOUNT_ATTR {
  IA_type m_account_type;
  ulonglong m_account_access;
  bool m_account_inited;

  /**
    Setter method for member m_account_type and m_account_access.
    Only allowed to have one account_type, but can have more than
    one account acl;

    @param[in]    type         account type.
    @param[in]    acl          account acl.
  */
  void set_acl(IA_type type, ulonglong acl) {
    /* Only allowed to have one account type according to IA_type order. */
    if (m_account_type == IA_type::UNKNOWN_USER) m_account_type = type;

    m_account_access |= acl;
  }

  void reset() {
    m_account_type = IA_type::UNKNOWN_USER;
    m_account_access = Internal_account_config::NO_ACL;
  }

  ulonglong get_acl() { return m_account_access; }
  IA_type get_type() { return m_account_type; }
};

/* The connection attributes of THD */
struct ST_CONN_ATTR {
  IA_type m_connect_type;

  void reset() { m_connect_type = IA_type::UNKNOWN_USER; }
  void set(IA_type type) { m_connect_type = type; }
  IA_type get() { return m_connect_type; }
};

/* Global variables */
extern Internal_account_config ia_config;
extern PSI_mutex_key key_LOCK_internal_account_string;
extern mysql_mutex_t LOCK_internal_account_string;

// Global interface
extern void internal_account_ctx_init();
extern void internal_account_ctx_destroy();
extern bool internal_account_need_protected(const char *account);
extern void internal_account_init_acl(Security_context *sctx,
                                      const char *account);

extern ulonglong internal_account_active_connection_count(IA_type type);
/* For COM_CONNECT */
extern bool internal_account_adjust_connection(THD *thd,
                                               enum_server_command command);
/* For COM_CHANGE_USER */
extern bool internal_account_change_connection(THD *thd,
                                               enum_server_command command);

/* We should promise the connections for internal account */
extern bool internal_account_exceed_max_connection(THD *thd);

/* Replace Connection_handler_manager::dec_connection_count() */
extern bool global_manager_dec_connection(THD *thd);

/* Replace Connection_handler_manager::wait_till_no_connection() */
extern void global_manager_wait_no_connection();

extern int show_maintain_connection_count(THD *, SHOW_VAR *var, char *buff);

} /* namespace im */

#endif /* SQL_INTERNAL_ACCOUNT_INCLUDED */
