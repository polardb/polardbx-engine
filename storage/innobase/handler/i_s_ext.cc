/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

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

/** @file handler/i_s_ext.cc
 Information of data file operation.

 Created 5/14/2019 Galaxy SQL
 *******************************************************/

#include <field.h>
#include <sql_show.h>

#include "dd/types/partition.h"
#include "dd/types/partition_index.h"
#include "fil0key.h"
#include "fil0purge.h"
#include "i_s.h"
#include "lizard0dict.h"
#include "mysql/plugin.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dictionary.h"
#include "sql/dd/types/table.h"
#include "sql/table.h"
#include "storage/innobase/handler/i_s_ext.h"
#include "dict0dd.h"

static const char plugin_author[] = "Alibaba Corporation";

#define OK(expr)     \
  if ((expr) != 0) { \
    DBUG_RETURN(1);  \
  }

#if !defined __STRICT_ANSI__ && defined __GNUC__ && !defined __clang__
#define STRUCT_FLD(name, value) \
  name:                         \
  value
#else
#define STRUCT_FLD(name, value) value
#endif

/* Don't use a static const variable here, as some C++ compilers (notably
HPUX aCC: HP ANSI C++ B3910B A.03.65) can't handle it. */
#define END_OF_ST_FIELD_INFO                                           \
  {                                                                    \
    STRUCT_FLD(field_name, NULL), STRUCT_FLD(field_length, 0),         \
        STRUCT_FLD(field_type, MYSQL_TYPE_NULL), STRUCT_FLD(value, 0), \
        STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),          \
        STRUCT_FLD(open_method, 0)                                     \
  }

ST_FIELD_INFO innodb_purge_file_fields_info[] = {
#define IDX_LOG_ID 0
    {STRUCT_FLD(field_name, "log_id"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_PURGE_START 1
    {STRUCT_FLD(field_name, "start_time"), STRUCT_FLD(field_length, 0),
     STRUCT_FLD(field_type, MYSQL_TYPE_DATETIME), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_ORIGINAL_PATH 2
    {STRUCT_FLD(field_name, "original_path"),
     STRUCT_FLD(field_length, PURGE_FILE_NAME_MAX_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_ORIGNINAL_SIZE 3
    {STRUCT_FLD(field_name, "original_size"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_TEMPORARY_PATH 4
    {STRUCT_FLD(field_name, "temporary_path"),
     STRUCT_FLD(field_length, PURGE_FILE_NAME_MAX_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_CURRENT_SIZE 5
    {STRUCT_FLD(field_name, "current_size"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

    END_OF_ST_FIELD_INFO};

/** Fill the purging file
 INFORMATION_SCHEMA.INNODB_PURGE_FILES
 @return 0 on SUCCESS */

static int innodb_purge_file_fill_table(
    THD *thd,          /*!< in: thread */
    Table_ref *tables, /*!< in/out: tables to fill */
    Item *)            /*!< in: condition (not used) */
{
  Field **fields;
  TABLE *table;
  file_purge_node_t *node = nullptr;
  bool first = true;
  DBUG_ENTER("innodb_purge_file_fill_table");

  table = tables->table;
  fields = table->field;

  if (file_purge_sys) {
    file_purge_sys->lock();

    while ((node = file_purge_sys->iterate_node(first, node))) {
      first = false;

      /* log_id */
      OK(fields[IDX_LOG_ID]->store(node->m_log_ddl_id, true));

      /* start_time */
      OK(field_store_time_t(fields[IDX_PURGE_START], node->m_start_time));

      /* original_path */
      if (node->m_original_path) {
        OK(field_store_string(fields[IDX_ORIGINAL_PATH],
                              node->m_original_path));
        fields[IDX_ORIGINAL_PATH]->set_notnull();
      } else {
        fields[IDX_ORIGINAL_PATH]->set_null();
      }

      /* original_size */
      OK(fields[IDX_ORIGNINAL_SIZE]->store(node->m_original_size, true));

      /* temporary_path */
      OK(field_store_string(fields[IDX_TEMPORARY_PATH], node->m_file_path));

      /* current_size */
      OK(fields[IDX_CURRENT_SIZE]->store(node->m_current_size, true));

      OK(schema_table_store_record(thd, table));
    }

    file_purge_sys->unlock();
  }

  DBUG_RETURN(0);
}

/** Bind the dynamic table INFORMATION_SCHEMA.innodb_purge_files
 @return 0 on success */
static int innodb_purge_file_init(void *p) {
  ST_SCHEMA_TABLE *schema;
  DBUG_ENTER("innodb_purge_file_init");

  schema = (ST_SCHEMA_TABLE *)p;

  schema->fields_info = innodb_purge_file_fields_info;
  schema->fill_table = innodb_purge_file_fill_table;

  DBUG_RETURN(0);
}

/** Unbind a dynamic INFORMATION_SCHEMA table.
 @return 0 on success */
static int i_s_common_deinit(void *p) /*!< in/out: table schema object */
{
  DBUG_ENTER("i_s_common_deinit");

  /* Do nothing */

  DBUG_RETURN(0);
}

static struct st_mysql_information_schema i_s_info = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

/** I_S.innodb_purge_files view version. It would be X.Y and X should be the
    server major , Y is the extra_is_plugin_version. */
static const uint EXTRA_IS_PLUGIN_VERSION = 1;
constexpr uint64_t i_s_innodb_plugin_version_for_purge_file =
    (INNODB_VERSION_MAJOR << 8 | (EXTRA_IS_PLUGIN_VERSION));

struct st_mysql_plugin i_s_innodb_data_file_purge = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

    /* pointer to type-specific plugin descriptor */
    /* void* */
    STRUCT_FLD(info, &i_s_info),

    /* plugin name */
    STRUCT_FLD(name, "INNODB_PURGE_FILES"),

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(author, plugin_author),

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(descr, "InnoDB purging data file list"),

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    STRUCT_FLD(init, innodb_purge_file_init),

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    NULL,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    STRUCT_FLD(deinit, i_s_common_deinit),

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    STRUCT_FLD(version, i_s_innodb_plugin_version_for_purge_file),

    /* SHOW_VAR* */
    STRUCT_FLD(status_vars, NULL),

    /* SYS_VAR** */
    STRUCT_FLD(system_vars, NULL),

    /* reserved for dependency checking */
    /* void* */
    STRUCT_FLD(__reserved1, NULL),

    /* Plugin flags */
    /* unsigned long */
    STRUCT_FLD(flags, 0UL),
};

ST_FIELD_INFO innodb_tablespace_master_key_fields_info[] = {
#define IDX_TABLE_SPACE 0
    {STRUCT_FLD(field_name, "table_space"),
     STRUCT_FLD(field_length, MAX_FULL_NAME_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_MASTER_KEY_ID 1
    {STRUCT_FLD(field_name, "master_key_id"),
     STRUCT_FLD(field_length, Encryption::MASTER_KEY_NAME_MAX_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

    END_OF_ST_FIELD_INFO};

/** Fill the tablespace master key id
INFORMATION_SCHEMA.INNODB_TABLESPACE_MASTER_KEY
@return 0 on SUCCESS */
static int innodb_tablespace_master_key_fill_table(
    THD *thd,          /*!< in: thread */
    Table_ref *tables, /*!< in/out: tables to fill */
    Item *)            /*!< in: condition (not used) */
{
  Field **fields;
  TABLE *table;
  Tbl_keys tbl_keys;
  DBUG_ENTER("innodb_tablespace_master_key_fill_table");

  table = tables->table;
  fields = table->field;

  fil_encryption_all_key(tbl_keys);

  for (auto &elem : tbl_keys) {
    OK(field_store_string(fields[IDX_TABLE_SPACE], elem.first.c_str()));
    OK(field_store_string(fields[IDX_MASTER_KEY_ID], elem.second.c_str()));

    OK(schema_table_store_record(thd, table));
  }

  DBUG_RETURN(0);
}

/** Bind the dynamic table INFORMATION_SCHEMA.INNODB_TABLESPACE_MASTER_KEY
@return 0 on success */
static int innodb_tablespace_master_key_init(void *p) {
  ST_SCHEMA_TABLE *schema;
  DBUG_ENTER("innodb_tablespace_master_key_init");
  schema = (ST_SCHEMA_TABLE *)p;

  schema->fields_info = innodb_tablespace_master_key_fields_info;
  schema->fill_table = innodb_tablespace_master_key_fill_table;

  DBUG_RETURN(0);
}

/** I_S.innodb_* views version postfix. Everytime the define of any InnoDB I_S
 * table is changed, this value has to be increased accordingly */
constexpr uint8_t i_s_innodb_plugin_version_postfix_rds = 1;

/** I_S.innodb_* views version. It would be X.Y and X should be the server major
 *  * version while Y is the InnoDB I_S views version, starting from 1 */
constexpr uint64_t i_s_innodb_plugin_version_rds =
    (INNODB_VERSION_MAJOR << 8 |
     (i_s_innodb_plugin_version_postfix_rds + EXTRA_IS_PLUGIN_VERSION));

struct st_mysql_plugin i_s_innodb_tablespace_master_key = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

    /* pointer to type-specific plugin descriptor */
    /* void* */
    STRUCT_FLD(info, &i_s_info),

    /* plugin name */
    STRUCT_FLD(name, "INNODB_TABLESPACE_MASTER_KEY"),

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(author, plugin_author),

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(descr, "InnoDB encrypted tablespace list"),

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    STRUCT_FLD(init, innodb_tablespace_master_key_init),

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    NULL,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    STRUCT_FLD(deinit, i_s_common_deinit),

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    STRUCT_FLD(version, i_s_innodb_plugin_version_rds),

    /* SHOW_VAR* */
    STRUCT_FLD(status_vars, NULL),

    /* SYS_VAR** */
    STRUCT_FLD(system_vars, NULL),

    /* reserved for dependency checking */
    /* void* */
    STRUCT_FLD(__reserved1, NULL),

    /* Plugin flags */
    /* unsigned long */
    STRUCT_FLD(flags, 0UL),
};

ST_FIELD_INFO innodb_table_status_fields_info[] = {
#define IDX_TS_SCHEMA_NAME 0
    {STRUCT_FLD(field_name, "SCHEMA_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_TS_TABLE_NAME 1
    {STRUCT_FLD(field_name, "TABLE_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_TS_PARTITION_NAME 2
    {STRUCT_FLD(field_name, "PARTITION_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_TS_OPTIONS 3
    {STRUCT_FLD(field_name, "options"), STRUCT_FLD(field_length, 8192),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

    END_OF_ST_FIELD_INFO};

static int innodb_table_status_fill_table(THD *thd,
		Table_ref *tables, Item *) {
  Field **fields;
  TABLE *table;
  std::vector<const dd::Schema *> dd_schemas;
  std::vector<const dd::Table *> dd_tables;
  std::unordered_map<dd::Object_id, dd::String_type> schema_map;
  DBUG_ENTER("innodb_table_status_fill_table");

  table = tables->table;
  fields = table->field;

  auto dc = dd::get_dd_client(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(dc);

  if (dc->fetch_global_components(&dd_schemas)) {
    DBUG_RETURN(1);
  }
  for (auto schema : dd_schemas) {
    schema_map[schema->id()] = schema->name();
  }

  if (dc->fetch_global_components(&dd_tables)) {
    DBUG_RETURN(1);
  }

  for (auto dd_table : dd_tables) {
    auto it = schema_map.find(dd_table->schema_id());
    if (it == schema_map.end()) {
      continue;
    } else {
      OK(field_store_string(fields[IDX_TS_SCHEMA_NAME], it->second.c_str()));
      fields[IDX_TS_SCHEMA_NAME]->set_notnull();
    }

    OK(field_store_string(fields[IDX_TS_TABLE_NAME], dd_table->name().c_str()));
    fields[IDX_TS_TABLE_NAME]->set_notnull();

    fields[IDX_TS_PARTITION_NAME]->set_null();

    dd::String_type options = dd_table->options().raw_string();
    if (options.empty()) {
      fields[IDX_TS_OPTIONS]->set_null();
    } else {
      OK(field_store_string(fields[IDX_TS_OPTIONS], options.c_str()));
      fields[IDX_TS_OPTIONS]->set_notnull();
    }

    OK(schema_table_store_record(thd, table));

    for (auto dd_par : dd_table->partitions()) {
      OK(field_store_string(fields[IDX_TS_SCHEMA_NAME], it->second.c_str()));
      fields[IDX_TS_SCHEMA_NAME]->set_notnull();

      OK(field_store_string(fields[IDX_TS_TABLE_NAME],
                            dd_table->name().c_str()));
      fields[IDX_TS_TABLE_NAME]->set_notnull();

      OK(field_store_string(fields[IDX_TS_PARTITION_NAME],
                            dd_par->name().c_str()));
      fields[IDX_TS_PARTITION_NAME]->set_notnull();

      dd::String_type options = dd_par->options().raw_string();
      if (options.empty()) {
        fields[IDX_TS_OPTIONS]->set_null();
      } else {
        OK(field_store_string(fields[IDX_TS_OPTIONS], options.c_str()));
        fields[IDX_TS_OPTIONS]->set_notnull();
      }

      OK(schema_table_store_record(thd, table));
    }
  }

  DBUG_RETURN(0);
}

static int innodb_table_status_init(void *p) {
  ST_SCHEMA_TABLE *schema;
  DBUG_ENTER("innodb_table_status_init");
  schema = (ST_SCHEMA_TABLE *)p;

  schema->fields_info = innodb_table_status_fields_info;
  schema->fill_table = innodb_table_status_fill_table;

  DBUG_RETURN(0);
}

/** I_S.innodb_* views version postfix. Everytime the define of any InnoDB I_S
 * table is changed, this value has to be increased accordingly */
static constexpr uint8_t EXTRA_IS_PLUGIN_TS_VERSION = 1;

/** I_S.innodb_* views version. It would be X.Y and X should be the server major
 *  * version while Y is the InnoDB I_S views version, starting from 1 */
constexpr uint64_t i_s_innodb_plugin_version_table_status =
    (INNODB_VERSION_MAJOR << 8 | (EXTRA_IS_PLUGIN_TS_VERSION));

struct st_mysql_plugin i_s_innodb_table_status = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

    /* pointer to type-specific plugin descriptor */
    /* void* */
    STRUCT_FLD(info, &i_s_info),

    /* plugin name */
    STRUCT_FLD(name, "INNODB_TABLE_STATUS"),

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(author, plugin_author),

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(descr, "InnoDB all table status"),

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    STRUCT_FLD(init, innodb_table_status_init),

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    NULL,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    STRUCT_FLD(deinit, i_s_common_deinit),

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    STRUCT_FLD(version, i_s_innodb_plugin_version_table_status),

    /* SHOW_VAR* */
    STRUCT_FLD(status_vars, NULL),

    /* SYS_VAR** */
    STRUCT_FLD(system_vars, NULL),

    /* reserved for dependency checking */
    /* void* */
    STRUCT_FLD(__reserved1, NULL),

    /* Plugin flags */
    /* unsigned long */
    STRUCT_FLD(flags, 0UL),
};

static ST_FIELD_INFO innodb_gpp_stats_fields_info[] = {
#define IDX_GPP_TABLE_NAME 0
    {STRUCT_FLD(field_name, "TABLE_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_GPP_INDEX_NAME 1
    {STRUCT_FLD(field_name, "INDEX_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_GPP_HIT 2
    {STRUCT_FLD(field_name, "GPP_HIT"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_GPP_MISS 3
    {STRUCT_FLD(field_name, "GPP_MISS"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

    END_OF_ST_FIELD_INFO};

static int innodb_gpp_stats_fill_table(THD *thd, Table_ref *tables, Item *) {
  Field **fields;
  TABLE *table;
  std::vector<im::Index_gpp_stat> result;
  DBUG_ENTER("innodb_gpp_stats_fill_table");

  table = tables->table;
  fields = table->field;

  lizard::Collector collector = lizard::Collector(result);
  dict_sys->for_each_table(collector);

  for (auto &stat : result) {
    OK(field_store_string(fields[IDX_GPP_TABLE_NAME], stat.table_name.c_str()));
    fields[IDX_GPP_TABLE_NAME]->set_notnull();

    OK(field_store_string(fields[IDX_GPP_INDEX_NAME], stat.index_name.c_str()));
    fields[IDX_GPP_INDEX_NAME]->set_notnull();

    OK(fields[IDX_GPP_HIT]->store(stat.gpp_hit, true));

    OK(fields[IDX_GPP_MISS]->store(stat.gpp_miss, true));

    OK(schema_table_store_record(thd, table));
  }

  DBUG_RETURN(0);
}

static int innodb_gpp_stats_init(void *p) {
  ST_SCHEMA_TABLE *schema;
  DBUG_ENTER("innodb_gpp_stats_init");
  schema = (ST_SCHEMA_TABLE *)p;

  schema->fields_info = innodb_gpp_stats_fields_info;
  schema->fill_table = innodb_gpp_stats_fill_table;

  DBUG_RETURN(0);
}

static constexpr uint8_t EXTRA_IS_PLUGIN_GPP_VERSION = 1;

/** I_S.innodb_* views version. It would be X.Y and X should be the server major
 *  * version while Y is the InnoDB I_S views version, starting from 1 */
constexpr uint64_t i_s_innodb_plugin_version_innodb_gpp_stats =
    (INNODB_VERSION_MAJOR << 8 | (EXTRA_IS_PLUGIN_GPP_VERSION));

struct st_mysql_plugin i_s_innodb_gpp_stats = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

    /* pointer to type-specific plugin descriptor */
    /* void* */
    STRUCT_FLD(info, &i_s_info),

    /* plugin name */
    STRUCT_FLD(name, "INNODB_GPP_STATS"),

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(author, plugin_author),

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(descr, "Guess Primary-key Page-no(GPP) status for each index. "),

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    STRUCT_FLD(init, innodb_gpp_stats_init),

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    NULL,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    STRUCT_FLD(deinit, i_s_common_deinit),

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    STRUCT_FLD(version, i_s_innodb_plugin_version_innodb_gpp_stats),

    /* SHOW_VAR* */
    STRUCT_FLD(status_vars, NULL),

    /* SYS_VAR** */
    STRUCT_FLD(system_vars, NULL),

    /* reserved for dependency checking */
    /* void* */
    STRUCT_FLD(__reserved1, NULL),

    /* Plugin flags */
    /* unsigned long */
    STRUCT_FLD(flags, 0UL),
};

ST_FIELD_INFO innodb_index_status_fields_info[] = {
#define IDX_IS_SCHEMA_NAME 0
    {STRUCT_FLD(field_name, "SCHEMA_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_IS_TABLE_NAME 1
    {STRUCT_FLD(field_name, "TABLE_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_IS_INDEX_NAME 2
    {STRUCT_FLD(field_name, "INDEX_NAME"), STRUCT_FLD(field_length, 1024),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_IS_GPP_ENABLED 3
    {STRUCT_FLD(field_name, "GPP_ENABLED"), STRUCT_FLD(field_length, 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_IS_INDEX_PAGE_TYPE 4
    {STRUCT_FLD(field_name, "INDEX_PAGE_TYPE"), STRUCT_FLD(field_length, 64),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

    END_OF_ST_FIELD_INFO};

static int innodb_index_status_fill_one(THD *thd, const dict_index_t *index,
                                        TABLE *table) {
  DBUG_ENTER("innodb_index_status_fill_one");

  Field **fields = table->field;
  std::string schema_name, table_name, partition_name;
  index->table->get_table_name(schema_name, table_name, partition_name);
  if (!partition_name.empty()) {
    table_name.append(partition_name);
  }

  OK(field_store_string(fields[IDX_IS_SCHEMA_NAME], schema_name.c_str()));
  OK(field_store_string(fields[IDX_IS_TABLE_NAME], table_name.c_str()));
  OK(field_store_string(fields[IDX_IS_INDEX_NAME], index->name));
  OK(fields[IDX_IS_GPP_ENABLED]->store(index->gpp_stored));
  ut_ad(fil_page_type_is_index(index->page_type()) || index->type & DICT_FTS);
  OK(field_store_string(fields[IDX_IS_INDEX_PAGE_TYPE],
                        i_s_index_page_type_to_str(index->page_type())));

  OK(schema_table_store_record(thd, table));
  DBUG_RETURN(0);
}

static int innodb_index_status_fill_table(THD *thd, Table_ref *tables, Item *) {
  return fill_i_s_innodb_indexes_low(
      thd, tables, nullptr, innodb_index_status_fill_one, false, false);
}

static int innodb_index_status_init(void *p) {
  ST_SCHEMA_TABLE *schema;
  DBUG_ENTER("innodb_index_status_init");
  schema = (ST_SCHEMA_TABLE *)p;

  schema->fields_info = innodb_index_status_fields_info;
  schema->fill_table = innodb_index_status_fill_table;

  DBUG_RETURN(0);
}

/** I_S.innodb_index_status version. */
static constexpr uint8_t EXTRA_IS_PLUGIN_IS_VERSION = 1;

/** I_S.innodb_* views version. It would be X.Y and X should be the server major
 *  * version while Y is the InnoDB I_S views version, starting from 1 */
constexpr uint64_t i_s_innodb_plugin_version_index_status =
    (INNODB_VERSION_MAJOR << 8 | (EXTRA_IS_PLUGIN_IS_VERSION));

struct st_mysql_plugin i_s_innodb_index_status = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

    /* pointer to type-specific plugin descriptor */
    /* void* */
    STRUCT_FLD(info, &i_s_info),

    /* plugin name */
    STRUCT_FLD(name, "INNODB_INDEX_STATUS"),

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(author, plugin_author),

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(descr, "InnoDB all index status"),

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    STRUCT_FLD(init, innodb_index_status_init),

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    NULL,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    STRUCT_FLD(deinit, i_s_common_deinit),

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    STRUCT_FLD(version, i_s_innodb_plugin_version_index_status),

    /* SHOW_VAR* */
    STRUCT_FLD(status_vars, NULL),

    /* SYS_VAR** */
    STRUCT_FLD(system_vars, NULL),

    /* reserved for dependency checking */
    /* void* */
    STRUCT_FLD(__reserved1, NULL),

    /* Plugin flags */
    /* unsigned long */
    STRUCT_FLD(flags, 0UL),
};
