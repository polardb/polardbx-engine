/* Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.

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

#ifndef SQL_CMD_CREATE_TABLE_INCLUDED
#define SQL_CMD_CREATE_TABLE_INCLUDED

#include "lex_string.h"
#include "my_sqlcommand.h"
#include "sql/sql_cmd_ddl.h"

class Alter_info;
class THD;
struct TABLE_LIST;


/**
  A base class for CREATE/ALTER TABLE commands and friends.

  Child classes deal with SQL statements:
  * ALTER TABLE
  * ANALYZE TABLE
  * CACHE INDEX
  * CHECK TABLE
  * CREATE INDEX
  * CREATE TABLE
  * DROP INDEX
  * LOAD INDEX
  * OPTIMIZE TABLE
  * REPAIR TABLE
*/
class Sql_cmd_ddl_table : public Sql_cmd
{
public:
  explicit Sql_cmd_ddl_table(Alter_info *alter_info);

  virtual ~Sql_cmd_ddl_table()= 0; // force abstract class

protected:
  Alter_info * const m_alter_info;
};

inline Sql_cmd_ddl_table::~Sql_cmd_ddl_table() {}


class Sql_cmd_create_table final : public Sql_cmd_ddl_table
{
public:
  Sql_cmd_create_table(Alter_info *alter_info,
                       TABLE_LIST *query_expression_tables)
    : Sql_cmd_ddl_table(alter_info),
      query_expression_tables(query_expression_tables)
  {}

  enum_sql_command sql_command_code() const override
  {
    return SQLCOM_CREATE_TABLE;
  }

  bool execute(THD *thd) override;
  bool prepare(THD *thd) override;

private:
  TABLE_LIST *query_expression_tables;
};


class Sql_cmd_create_or_drop_index_base : public Sql_cmd_ddl_table
{
public:
  using Sql_cmd_ddl_table::Sql_cmd_ddl_table;

  virtual ~Sql_cmd_create_or_drop_index_base()= 0; // force abstract class

  bool execute(THD *thd) override;
};

inline Sql_cmd_create_or_drop_index_base::~Sql_cmd_create_or_drop_index_base() {}


class Sql_cmd_create_index final : public Sql_cmd_create_or_drop_index_base
{
public:
  using Sql_cmd_create_or_drop_index_base::Sql_cmd_create_or_drop_index_base;

  enum_sql_command sql_command_code() const override
  {
    return SQLCOM_CREATE_INDEX;
  }
};


class Sql_cmd_drop_index final : public Sql_cmd_create_or_drop_index_base
{
public:
  using Sql_cmd_create_or_drop_index_base::Sql_cmd_create_or_drop_index_base;

  enum_sql_command sql_command_code() const override
  {
    return SQLCOM_DROP_INDEX;
  }
};


class Sql_cmd_cache_index final : public Sql_cmd_ddl_table
{
public:
  Sql_cmd_cache_index(Alter_info *alter_info, const LEX_STRING &key_cache_name)
    : Sql_cmd_ddl_table(alter_info),
      m_key_cache_name(key_cache_name)
  {}

  enum_sql_command sql_command_code() const override
  {
    return SQLCOM_ASSIGN_TO_KEYCACHE;
  }

  bool execute(THD *thd) override;

private:
  bool assign_to_keycache(THD* thd, TABLE_LIST* tables);

private:
  const LEX_STRING m_key_cache_name;
};


class Sql_cmd_load_index final : public Sql_cmd_ddl_table
{
public:
  using Sql_cmd_ddl_table::Sql_cmd_ddl_table;

  enum_sql_command sql_command_code() const override
  {
    return SQLCOM_PRELOAD_KEYS ;
  }

  bool execute(THD *thd) override;

private:
  bool preload_keys(THD* thd, TABLE_LIST* tables);
};

#endif /* SQL_CMD_CREATE_TABLE_INCLUDED */
