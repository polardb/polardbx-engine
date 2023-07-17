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

  Implementation of internal account protection

  1. Protect internal account in table user/role/global_grant.
  2. Protect all the dynamic_privilege that cann't grant to normal user
  3. Protect SUPER/TABLESPACE/FILE/SHUTDOWN ACLS from normal user.
  4. Protect internal account role that cann't modify or grant to normal user
*/
#include "sql/auth/sql_guard.h"
#include "sql/auth/auth_acls.h"
#include "sql/field.h"

/**
  @addtogroup Internal Account Management

  The internal account protection implementation

  @{
*/
namespace im {

/** Whether apply the rds inner account and privileges stragety */
bool opt_enable_rds_priv_strategy = false;

/** Those privileges are not opened to normal user */
static int guard_privs[4] = {
    MYSQL_USER_FIELD_SHUTDOWN_PRIV, MYSQL_USER_FIELD_FILE_PRIV,
    MYSQL_USER_FIELD_SUPER_PRIV, MYSQL_USER_FIELD_CREATE_TABLESPACE_PRIV,
};

/**
  The constructor
*/
Object_guard::Object_guard(const String_type &key) : object_key(key) {}
/**
  The copy constructor
*/
Object_guard::Object_guard(const Object_guard &src)
    : Guard(src), object_key(src.object_key) {}
/**
  Protect the record from deleting within
  table[user/global_grants/role_edges]
*/
bool Entity_guard::protect_record_from_delete(THD *thd, TABLE *table,
                                              uchar *record, uint field_pos) {
  char *user_name;
  ptrdiff_t ptrdiff;
  Field *field;
  DBUG_ENTER("Entity_guard::protect_record_from_delete");

  Auto_bitmap helper(table);
  field = table->field[field_pos];

  if (record == table->record[0]) {
    user_name = get_field(thd->mem_root, field);
  } else {
    ptrdiff = table->record[1] - table->record[0];
    field->move_field_offset(ptrdiff);
    user_name = get_field(thd->mem_root, field);
    field->move_field_offset(-ptrdiff);
  }

  if (internal_account_need_protected(user_name)) {
    my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
    DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}
/**
   Copy constructor
*/
User_entity_guard::User_entity_guard(const User_entity_guard &src)
    : Entity_guard(src) {}
/**
  The singleton instance for user entity guard
*/
User_entity_guard *User_entity_guard::instance() {
  static User_entity_guard guard;
  return &guard;
}
/**
  The user table key
*/
const String_type User_entity_guard::object_name() {
  return String_type(user_key, key_len);
}

#ifndef DBUG_OFF
void dbug_user_field_rw_mode(TABLE *table) {
  for (uint i = MYSQL_USER_FIELD_HOST; i < MYSQL_USER_FIELD_COUNT; i++) {
    fprintf(stderr, "User table Fields [%d] read: %d, write: %d\n", i,
            bitmap_is_set(table->read_set, i),
            bitmap_is_set(table->write_set, i));
  }
}
#endif

/**
  Forbid normal user to insert user record
  which have the same name with reserved account or reserved privs

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool User_entity_guard::guard_insert(THD *thd, TABLE *table) {
  char *user_name;
  char *priv;
  DBUG_ENTER("User_entity_guard::guard_insert");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  Auto_bitmap_helper helper(table);

  /* Protect reserved username */
  user_name = get_field(thd->mem_root, table->field[MYSQL_USER_FIELD_USER]);
  if (internal_account_need_protected(user_name)) {
    my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
    DBUG_RETURN(true);
  }

  /* Protect reserved privileges */
  for (uint i = 0; i < sizeof(guard_privs) / sizeof(guard_privs[0]); i++) {
    priv = get_field(thd->mem_root, table->field[guard_privs[i]]);
    if (priv && (*priv == 'Y' || *priv == 'y')) {
      my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
      DBUG_RETURN(true);
    }
  }
  DBUG_RETURN(false);
}

/**
  Guard the inner accounts and privileges from modify.

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool User_entity_guard::guard_update(THD *thd, TABLE *table) {
  ptrdiff_t ptrdiff;
  const uchar *old_data;
  const uchar *new_data;
  char *new_name;
  char *old_name;
  Field *field;
  char *priv;
  DBUG_ENTER("User_entity_guard::guard_update");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  Auto_bitmap helper(table);

  old_data = table->record[1];
  new_data = table->record[0];
  ptrdiff = old_data - new_data;

  /* Step 1: confirm the reserved user */
  field = table->field[MYSQL_USER_FIELD_USER];

  new_name = get_field(thd->mem_root, field);
  field->move_field_offset(ptrdiff);
  old_name = get_field(thd->mem_root, field);
  field->move_field_offset(-ptrdiff);

  if (internal_account_need_protected(old_name) ||
      internal_account_need_protected(new_name)) {
    my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
    DBUG_RETURN(true);
  }

  /* Step 2: confirm the reserved priv */
  for (uint i = 0; i < sizeof(guard_privs) / sizeof(guard_privs[0]); i++) {
    priv = get_field(thd->mem_root, table->field[guard_privs[i]]);
    if (priv && (*priv == 'Y' || *priv == 'y')) {
      my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
      DBUG_RETURN(true);
    }
  }
  DBUG_RETURN(false);
}

/**
  Guard the user table record which is reserved account

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool User_entity_guard::guard_delete(THD *thd, TABLE *table, uint record_pos) {
  bool err;
  DBUG_ENTER("User_entity_guard::guard_delete");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  err = protect_record_from_delete(thd, table, table->record[record_pos],
                                   MYSQL_USER_FIELD_USER);
  DBUG_RETURN(err);
}

/* Clone the guard instance dynamically */
User_entity_guard *User_entity_guard::clone() const {
  return new User_entity_guard(*this);
}

User_entity_guard *User_entity_guard::clone(MEM_ROOT *mem_root) const {
  return new (mem_root) User_entity_guard(*this);
}
/**
  Destrutor if placement new object
*/
void User_entity_guard::destroy() const { this->~User_entity_guard(); }
/**
  Copy constructor
*/
Dynamic_privilege_entity_guard::Dynamic_privilege_entity_guard(
    const Dynamic_privilege_entity_guard &src)
    : Entity_guard(src) {}
/**
  The singleton instance for Dynamic_privilege_entity_guard entity guard
*/
Dynamic_privilege_entity_guard *Dynamic_privilege_entity_guard::instance() {
  static Dynamic_privilege_entity_guard guard;
  return &guard;
}
/**
  The dynamic privilege table key for global_grants
*/
const String_type Dynamic_privilege_entity_guard::object_name() {
  return String_type(dynamic_privilege_key, key_len);
}

/**
  Forbid normal user to insert any record of global_grants

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool Dynamic_privilege_entity_guard::guard_insert(THD *thd, TABLE *) {
  DBUG_ENTER("Dynamic_privilege_entity_guard::guard_insert");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
  DBUG_RETURN(true);
}

/**
  Guard the inner accounts and privileges from modify.

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool Dynamic_privilege_entity_guard::guard_update(THD *thd, TABLE *table) {
  ptrdiff_t ptrdiff;
  const uchar *old_data;
  const uchar *new_data;
  char *new_name;
  char *old_name;
  Field *field;
  DBUG_ENTER("Dynamic_privilege_entity_guard::guard_update");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  Auto_bitmap helper(table);

  old_data = table->record[1];
  new_data = table->record[0];
  ptrdiff = old_data - new_data;

  /* Step 1: confirm the reserved user */
  field = table->field[MYSQL_DYNAMIC_PRIV_FIELD_USER];

  new_name = get_field(thd->mem_root, field);
  field->move_field_offset(ptrdiff);
  old_name = get_field(thd->mem_root, field);
  field->move_field_offset(-ptrdiff);

  if (internal_account_need_protected(old_name) ||
      internal_account_need_protected(new_name)) {
    my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

/**
  Guard the global_grants table record

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool Dynamic_privilege_entity_guard::guard_delete(THD *thd, TABLE *table,
                                                  uint record_pos) {
  bool err;
  DBUG_ENTER("Dynamic_privilege_entity_guard::guard_delete");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  err = protect_record_from_delete(thd, table, table->record[record_pos],
                                   MYSQL_DYNAMIC_PRIV_FIELD_USER);
  DBUG_RETURN(err);
}

/* Clone the guard instance dynamically */
Dynamic_privilege_entity_guard *Dynamic_privilege_entity_guard::clone() const {
  return new Dynamic_privilege_entity_guard(*this);
}

Dynamic_privilege_entity_guard *Dynamic_privilege_entity_guard::clone(
    MEM_ROOT *mem_root) const {
  return new (mem_root) Dynamic_privilege_entity_guard(*this);
}
/**
  Destrutor if placement new object
*/
void Dynamic_privilege_entity_guard::destroy() const {
  this->~Dynamic_privilege_entity_guard();
}

/**
  Copy constructor
*/
Role_entity_guard::Role_entity_guard(const Role_entity_guard &src)
    : Entity_guard(src) {}
/**
  The singleton instance for Role_entity_guard entity guard
*/
Role_entity_guard *Role_entity_guard::instance() {
  static Role_entity_guard guard;
  return &guard;
}
/**
  The dynamic privilege table key for global_grants
*/
const String_type Role_entity_guard::object_name() {
  return String_type(role_key, key_len);
}

/* Clone the guard instance dynamically */
Role_entity_guard *Role_entity_guard::clone() const {
  return new Role_entity_guard(*this);
}

Role_entity_guard *Role_entity_guard::clone(MEM_ROOT *mem_root) const {
  return new (mem_root) Role_entity_guard(*this);
}
/**
  Destrutor if placement new object
*/
void Role_entity_guard::destroy() const { this->~Role_entity_guard(); }

/**
  Forbid normal user to inherit maintain user role

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool Role_entity_guard::guard_insert(THD *thd, TABLE *table) {
  char *user_name;
  DBUG_ENTER("Role_entity_guard::guard_insert");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  Auto_bitmap_helper helper(table);

  user_name =
      get_field(thd->mem_root, table->field[MYSQL_ROLE_EDGES_FIELD_FROM_USER]);
  if (internal_account_need_protected(user_name)) {
    my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
    DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}

/**
  Guard the role record in role_edges
  Not allowed to modify role whose from_user is protected user

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool Role_entity_guard::guard_delete(THD *thd, TABLE *table, uint record_pos) {
  bool err;
  DBUG_ENTER("User_entity_guard::guard_delete");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  err = protect_record_from_delete(thd, table, table->record[record_pos],
                                   MYSQL_ROLE_EDGES_FIELD_FROM_USER);
  DBUG_RETURN(err);
}

/**
  Not allowed to modify role whose from_user or to_user is protected user

  @param[in]    thd     connection context
  @param[in]    table   target table object

  @retval       true    not allowed
  @retval       false   success
*/
bool Role_entity_guard::guard_update(THD *thd, TABLE *table) {
  ptrdiff_t ptrdiff;
  const uchar *old_data;
  const uchar *new_data;
  char *new_from_user;
  char *new_to_user;
  char *old_from_user;
  char *old_to_user;
  Field *from_field;
  Field *to_field;
  DBUG_ENTER("Role_entity_guard::guard_update");

  if (thd && thd->security_context()->check_access(SUPER_ACL))
    DBUG_RETURN(false);

  Auto_bitmap helper(table);

  old_data = table->record[1];
  new_data = table->record[0];
  ptrdiff = old_data - new_data;

  /* Step 1: confirm the reserved user */
  from_field = table->field[MYSQL_ROLE_EDGES_FIELD_FROM_USER];
  to_field = table->field[MYSQL_ROLE_EDGES_FIELD_TO_USER];

  new_from_user = get_field(thd->mem_root, from_field);
  new_to_user = get_field(thd->mem_root, to_field);

  from_field->move_field_offset(ptrdiff);
  to_field->move_field_offset(ptrdiff);

  old_from_user = get_field(thd->mem_root, from_field);
  old_to_user = get_field(thd->mem_root, to_field);

  from_field->move_field_offset(-ptrdiff);
  to_field->move_field_offset(-ptrdiff);

  if (internal_account_need_protected(old_from_user) ||
      internal_account_need_protected(old_to_user) ||
      internal_account_need_protected(new_from_user) ||
      internal_account_need_protected(new_to_user)) {
    my_error(ER_RESERVED_USER_OR_PRIVS, MYF(0));
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}
/* Global guard singleton instance */
Internal_guard_strategy *Internal_guard_strategy::instance() {
  static Internal_guard_strategy guard;
  return &guard;
}
/* Constructor */
Internal_guard_strategy::Internal_guard_strategy()
    : m_object_map(), m_entity_map() {}
/**
  All the registered object are singleton instance,
  so just clear them.
*/
Internal_guard_strategy::~Internal_guard_strategy() {
  m_object_map.clear();
  m_entity_map.clear();
}

void internal_guard_strategy_init(bool bootstrap) {
  if (!bootstrap) {
    /* 1. User table record guard */
    Internal_guard_strategy::instance()->register_guard<Entity_guard>(
        User_entity_guard::instance()->object_name(),
        User_entity_guard::instance());

    /* 2. Global_grant table record guard */
    Internal_guard_strategy::instance()->register_guard<Entity_guard>(
        Dynamic_privilege_entity_guard::instance()->object_name(),
        Dynamic_privilege_entity_guard::instance());

    /* 3. Role_edges table record guard */
    Internal_guard_strategy::instance()->register_guard<Entity_guard>(
        Role_entity_guard::instance()->object_name(),
        Role_entity_guard::instance());
  }
}

void internal_guard_strategy_shutdown() {
  // nothing to do
}

/* Lookup the entity_guard through object_name */
Entity_guard *internal_entity_guard_lookup(const char *key, std::size_t len) {
  String_type str(key, len);
  return Internal_guard_strategy::instance()->lookup_guard<Entity_guard>(str);
}

/**
  Apply the guard strategy when DML

  @param[in]    thd         connection context
  @param[in]    table       target table object
  @param[in]    type        the guard type[DML]
  @param[in]    record_pos  which record will be used
                            table->record[0] or table->record[1]
*/
bool guard_record(THD *thd, TABLE *table, Guard_type type, uint record_pos) {
  Entity_guard *guard;

  if (!opt_enable_rds_priv_strategy) return false;

  if ((guard = table->entity_guard)) {
    switch (type) {
      case Guard_type::GUARD_INSERT:
        return guard->guard_insert(thd, table);
      case Guard_type::GUARD_UPDATE:
        return guard->guard_update(thd, table);
      case Guard_type::GUARD_DELETE:
        return guard->guard_delete(thd, table, record_pos);
    }
  }
  return false;
}

} /* namespace im */

/// @} (end of group Internal Account Management)
