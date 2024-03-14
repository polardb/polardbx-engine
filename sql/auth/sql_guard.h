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

#ifndef SQL_GUARD_INCLUDED
#define SQL_GUARD_INCLUDED

#include "sql/dd/string_type.h"
#include "sql/sql_class.h"
#include "sql/table.h"  //read_set, write_set

class THD;

namespace im {

/**
  Internal Guard Strategy

  Mainly for:
    1) Structure  : mysql schema & system table.
    2) Record     : internal account users/role, reserved privileges.
*/
using namespace dd;

/**
  The internal guard interface
*/
class Guard {
 public:
  Guard() {}
  Guard(const Guard &) {}

  virtual ~Guard() {}
  /**
    Allocate a new object and invoke copy constuctor

    @retval     Guard
  */
  virtual Guard *clone() const = 0;
  /**
    Placement new object;
    Should call destroy() function when free;

    @param[in]  mem_root  memory allocator

    @retval     Guard
  */
  virtual Guard *clone(MEM_ROOT *mem_root) const = 0;
  /**
    Explicitly call destructor
  */
  virtual void destroy() const = 0;

}; /* Class   Guard */

/**
  The SCHEMA/TABLE structure guard strategy
*/
class Object_guard : public Guard {
 public:
  Object_guard() {}
  ~Object_guard() override {}

  Object_guard(const String_type &key);

  Object_guard(const Object_guard &src);

  virtual Object_guard *clone() const override {
    return new Object_guard(*this);
  }

  virtual Object_guard *clone(MEM_ROOT *mem_root) const override {
    return new (mem_root) Object_guard(*this);
  }

  virtual void destroy() const override { this->~Object_guard(); }

 private:
  String_type object_key;
};

/**
  The table record guard strategy
*/
class Entity_guard : public Guard {
 public:
  Entity_guard() {}

  Entity_guard(const Entity_guard &src) : Guard(src) {}
  /**
    Guard the DML operation
  */
  enum class Type { GUARD_INSERT, GUARD_UPDATE, GUARD_DELETE };

  /* Use all columns temporarily, restore it automatically */
  class Auto_bitmap : public Disable_unnamed_object {
   public:
    explicit Auto_bitmap(TABLE *ptr) {
      m_table = ptr;
      save_bitmap();
    }
    /**
      Save the read/write set temporarily
    */
    void save_bitmap() {
      m_read_set = m_table->read_set;
      m_write_set = m_table->write_set;
      m_table->read_set = &m_table->s->all_set;
      m_table->write_set = &m_table->s->all_set;
    }
    /**
      Destrutor function and restore the read/write set
    */
    virtual ~Auto_bitmap() {
      m_table->read_set = m_read_set;
      m_table->write_set = m_write_set;
    }

   private:
    TABLE *m_table;
    MY_BITMAP *m_read_set;
    MY_BITMAP *m_write_set;
  };

  ~Entity_guard() override {}

  /*  Entity record guard should implement those three strategies */
  /**
    Guard the table insert operation

    @param[in]    thd     connection context
    @param[in]    table   target table object

    @retval       true    not allowed
    @retval       false   success
  **/
  virtual bool guard_insert(THD *thd, TABLE *table) = 0;
  /**
    Guard the table update operation

    @param[in]    thd     connection context
    @param[in]    table   target table object

    @retval       true    not allowed
    @retval       false   success
  **/
  virtual bool guard_update(THD *thd, TABLE *table) = 0;

  /* Maybe table->record[0] or table->record[1] will be used when delete */

  /**
    Guard the table update operation

    @param[in]    thd     connection context
    @param[in]    table   target table object

    @retval       true    not allowed
    @retval       false   success
  **/
  virtual bool guard_delete(THD *thd, TABLE *table, uint record_pos) = 0;

  /**
    Protect the record from deleting within in
    table[user/global_grants/role_edges]

    @param[in]    thd       connection context
    @param[in]    table     target table
    @param[in]    record    table->record[0] or table->record[1]
    @param[in]    field_pos user field pos

    @retval       true      not allowed
    @retval       false     success
  */
  bool protect_record_from_delete(THD *thd, TABLE *table, uchar *record,
                                  uint field_pos);
  /* The [schema + table] name */
  virtual const String_type object_name() = 0;

  virtual Entity_guard *clone() const override = 0;

  virtual Entity_guard *clone(MEM_ROOT *mem_root) const override = 0;

  virtual void destroy() const override = 0;
};

typedef Entity_guard::Type Guard_type;
typedef Entity_guard::Auto_bitmap Auto_bitmap_helper;

/**
  User table record guard strategy which is to protect internal account
  and reserved privs.
*/
class User_entity_guard : public Entity_guard {
 public:
  static constexpr const char *user_key = "mysql\0user\0";
  static constexpr const std::size_t key_len = 11;

  User_entity_guard() {}
  User_entity_guard(const User_entity_guard &src);

  ~User_entity_guard() override {}

  /* Entity record guard should implement those three strategies */
  /**
    Guard the table insert operation
  */
  virtual bool guard_insert(THD *thd, TABLE *table) override;
  /**
    Guard the table update operation
  */
  virtual bool guard_update(THD *thd, TABLE *table) override;
  /**
    Guard the table delete operation
  */
  virtual bool guard_delete(THD *thd, TABLE *table, uint record_pos) override;

  /**
    String format: [schema + "\0" + table]
    Here it is the user_key
  */
  virtual const String_type object_name() override;

  virtual User_entity_guard *clone() const override;
  virtual User_entity_guard *clone(MEM_ROOT *mem_root) const override;
  /**
    Explicit call destroy if placement new object
  */
  virtual void destroy() const override;

  /**
    Singleton instance will be registered when booting.
  */
  static User_entity_guard *instance();
};
/**
  Dynamic privileges guard of table global_grants.
*/
class Dynamic_privilege_entity_guard : public Entity_guard {
 public:
  static constexpr const char *dynamic_privilege_key = "mysql\0global_grants\0";
  static constexpr const std::size_t key_len = 20;

  Dynamic_privilege_entity_guard() {}

  Dynamic_privilege_entity_guard(const Dynamic_privilege_entity_guard &src);

  ~Dynamic_privilege_entity_guard() override {}

  /* Entity record guard should implement those three strategies */
  /**
    Guard the table insert operation
  */
  virtual bool guard_insert(THD *thd, TABLE *table) override;
  /**
    Guard the table update operation
  */
  virtual bool guard_update(THD *thd, TABLE *table) override;
  /**
    Guard the table delete operation
  */
  virtual bool guard_delete(THD *thd, TABLE *table, uint record_pos) override;

  /**
    String format: [schema + "\0" + table]
    Here it is the dynamic_privilege_key
  */
  virtual const String_type object_name() override;

  virtual Dynamic_privilege_entity_guard *clone() const override;
  virtual Dynamic_privilege_entity_guard *clone(
      MEM_ROOT *mem_root) const override;
  /**
    Explicit call destroy if placement new object
  */
  virtual void destroy() const override;
  /**
    Singleton instance will be registered when booting.
  */
  static Dynamic_privilege_entity_guard *instance();
};

/**
  Role guard of table role_edges
*/
class Role_entity_guard : public Entity_guard {
 public:
  static constexpr const char *role_key = "mysql\0role_edges\0";
  static constexpr const std::size_t key_len = 17;

  Role_entity_guard() {}

  Role_entity_guard(const Role_entity_guard &src);

  ~Role_entity_guard() override {}

  /* Entity record guard should implement those three strategies */
  /**
    Guard the table insert operation
  */
  virtual bool guard_insert(THD *thd, TABLE *table) override;
  /**
    Guard the table update operation
  */
  virtual bool guard_update(THD *thd, TABLE *table) override;
  /**
    Guard the table delete operation
  */
  virtual bool guard_delete(THD *thd, TABLE *table, uint record_pos) override;

  /**
    String format: [schema + "\0" + table]
    Here it is the role_key
  */
  virtual const String_type object_name() override;

  virtual Role_entity_guard *clone() const override;
  virtual Role_entity_guard *clone(MEM_ROOT *mem_root) const override;
  /**
    Explicit call destroy if placement new object
  */
  virtual void destroy() const override;
  /**
    Singleton instance will be registered when booting.
  */
  static Role_entity_guard *instance();
};
/**
  Global internal guard strategy:
   1) protect system table
   2) protect internal account record in
       mysql.[user/role_edges/global_grants]
*/
class Internal_guard_strategy {
 public:
  typedef std::map<String_type, Object_guard *> Object_map;
  typedef std::map<String_type, Entity_guard *> Entity_map;

  /* Global singleton instance */
  static Internal_guard_strategy *instance();

  Internal_guard_strategy();
  virtual ~Internal_guard_strategy();

  /* The map selector */
  template <typename T>
  struct Type_selector {};

  /* Uniform function to get guard map */
  template <typename T>
  std::map<String_type, T *> *m_map() {
    return m_map(Type_selector<T>());
  }
  /**
    Register the predefined guard instance

    @param[in]    object_name   the object key
    @param[in]    object        object pointer
  */
  template <typename T>
  void register_guard(const String_type &object_name, T *object) {
    m_map<T>()->insert(std::pair<String_type, T *>(object_name, object));
  }
  /**
    Lookup the guard instance through object_name

    @param[in]    object_name   the object key

    @retval       T*            registered guard
  */
  template <typename T>
  T *lookup_guard(String_type &object_name) {
    typename std::map<String_type, T *>::const_iterator it;

    it = m_map<T>()->find(object_name);
    if (it == m_map<T>()->end())
      return nullptr;
    else
      return it->second;
  }

 private:
  /**
    Private getter of object_map
  */
  Object_map *m_map(Type_selector<Object_guard>) { return &m_object_map; }
  /**
    Private getter of entity_map
  */
  Entity_map *m_map(Type_selector<Entity_guard>) { return &m_entity_map; }

  Object_map m_object_map;
  Entity_map m_entity_map;
};

/* The global variable/interface for the guard strategy */

extern bool opt_enable_rds_priv_strategy;

extern void internal_guard_strategy_init(bool bootstrap);

extern void internal_guard_strategy_shutdown();

extern Entity_guard *internal_entity_guard_lookup(const char *key,
                                                  std::size_t len);
extern bool guard_record(THD *thd, TABLE *table, Guard_type type,
                         uint record_pos = 0);

/**
  Clone the guard object
*/
template <typename T>
inline T *guard_clone(T *ptr, MEM_ROOT *mem_root) {
  if (ptr == nullptr) return nullptr;

  return ptr->clone(mem_root);
}
/**
  Destroy the guard object
*/
template <typename T>
inline void guard_destroy(T *ptr) {
  if (ptr != nullptr) ptr->destroy();
}

/**
  The mysql database access control:
  We didn't allowed normal user to modify the mysql database structure.
*/
class Mysql_internal_schema_access : public ACL_internal_schema_access {
 public:
  Mysql_internal_schema_access() {}

  ~Mysql_internal_schema_access() override {}

  ACL_internal_access_result check(ulong want_access, ulong *,
                                   bool) const override;

  const ACL_internal_table_access *lookup(const char *name) const override;

  static Mysql_internal_schema_access *instance();
};

/**
  The inner database access control:

   - Inner databases can be set by read only variable "INNER_SCHEMA_LIST"
   - Only inner or SUPER_ACL user can operate these DB if granted privs,
     other account can't operate these DB even though granted except
     SELECT/INDEX/GRANT ACLs.
*/
class Inner_schema_access : public ACL_internal_schema_access {
 public:
  Inner_schema_access() {}

  ~Inner_schema_access() override {}

  ACL_internal_access_result check(ulong want_access, ulong *save_priv,
                                   bool any_combination_will_do) const override;

  const ACL_internal_table_access *lookup(const char *name) const override;

  static Inner_schema_access *instance();
};

extern char *inner_schema_list_str;

/**
  Register mysql and inner database according to global variable
  INNER_SCHEMA_LIST.

  @param[in]  bootstrap   Initialize or start MYSQLD

  @retval     false       Success
  @retval     true        Failure
*/
extern bool ACL_inner_schema_register(bool bootstrap);
/**
  Register schema access control.

  @param[in]  schema      Database name
  @param[in]  access      ACL strategy
*/
extern void register_schema_access(const std::string &schema,
                                   const ACL_internal_schema_access *access);

} /* namespace im */
#endif
