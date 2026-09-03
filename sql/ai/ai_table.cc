/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

/**
  @file sql/ai/ai_table.cc
  Implementation of mysql.ai_model_config system table operations.
*/

#include "sql/ai/ai_table.h"

#include "lex_string.h"
#include "m_string.h"
#include "my_dbug.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/log_shared.h"
#include "mysqld_error.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/log.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/transaction.h"

#include "sql/common/table.h"
#include "sql/common/table_common.h"

#include <string>

namespace im {

/* ---- table / schema identifiers ---- */
static LEX_CSTRING AI_SCHEMA_NAME = {C_STRING_WITH_LEN("mysql")};
static LEX_CSTRING AI_TABLE_NAME = {C_STRING_WITH_LEN("ai_model_config")};
static const char *AI_TABLE_ALIAS = "ai_model_config";
static LEX_CSTRING AI_FUNCTION_DEFAULT_TABLE_NAME = {
    C_STRING_WITH_LEN("ai_function_config")};
static const char *AI_FUNCTION_DEFAULT_TABLE_ALIAS =
    "ai_function_config";

static constexpr const char *AI_API_KEY_OBFUSCATION_PREFIX = "$AI_OBF$1:";
static constexpr const char *AI_API_KEY_OBFUSCATION_SALT =
    "PolarDBXAIKey";

static bool is_obfuscated_api_key(const char *api_key) {
  return api_key && strncmp(api_key, AI_API_KEY_OBFUSCATION_PREFIX,
                            strlen(AI_API_KEY_OBFUSCATION_PREFIX)) == 0;
}

static std::string transform_api_key(const char *api_key, bool decode) {
  std::string result;
  if (!api_key) return result;

  const size_t salt_len = strlen(AI_API_KEY_OBFUSCATION_SALT);
  result.reserve(strlen(api_key));
  for (size_t i = 0; api_key[i] != '\0'; i++) {
    const unsigned char c = static_cast<unsigned char>(api_key[i]);
    if (c < 33 || c > 126) {
      result.push_back(static_cast<char>(c));
      continue;
    }

    const unsigned int shift =
        (static_cast<unsigned char>(AI_API_KEY_OBFUSCATION_SALT[i % salt_len]) +
         static_cast<unsigned int>(i * 13)) % 94;
    const unsigned int pos = c - 33;
    const unsigned int transformed =
        decode ? (pos + 94 - shift) % 94 : (pos + shift) % 94;
    result.push_back(static_cast<char>(transformed + 33));
  }
  return result;
}

static std::string obfuscate_api_key(const char *api_key) {
  if (!api_key || api_key[0] == '\0') return "";
  return std::string(AI_API_KEY_OBFUSCATION_PREFIX) +
         transform_api_key(api_key, false);
}

static std::string deobfuscate_api_key(const char *api_key) {
  if (!api_key) return "";
  if (!is_obfuscated_api_key(api_key)) return api_key;
  return transform_api_key(api_key + strlen(AI_API_KEY_OBFUSCATION_PREFIX),
                           true);
}

/* ---- TABLE_FIELD_TYPE definition (must match CREATE TABLE DDL) ---- */
static const TABLE_FIELD_TYPE
    mysql_ai_table_fields[MYSQL_AI_FIELD_COUNT] = {
        /* 0  id BIGINT NOT NULL AUTO_INCREMENT */
        {{C_STRING_WITH_LEN("id")},
         {C_STRING_WITH_LEN("bigint")},
         {NULL, 0}},
        /* 1  model_name VARCHAR(128) NOT NULL */
        {{C_STRING_WITH_LEN("model_name")},
         {C_STRING_WITH_LEN("varchar(128)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 2  provider VARCHAR(64) NOT NULL */
        {{C_STRING_WITH_LEN("provider")},
         {C_STRING_WITH_LEN("varchar(64)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 3  endpoint VARCHAR(512) NOT NULL */
        {{C_STRING_WITH_LEN("endpoint")},
         {C_STRING_WITH_LEN("varchar(512)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 4  model VARCHAR(128) */
        {{C_STRING_WITH_LEN("model")},
         {C_STRING_WITH_LEN("varchar(128)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 5  api_key VARCHAR(512) */
        {{C_STRING_WITH_LEN("api_key")},
         {C_STRING_WITH_LEN("varchar(512)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 6  model_params TEXT */
        {{C_STRING_WITH_LEN("model_params")},
         {C_STRING_WITH_LEN("text")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 7  status VARCHAR(16) DEFAULT 'ACTIVE' */
        {{C_STRING_WITH_LEN("status")},
         {C_STRING_WITH_LEN("varchar(16)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 8  description VARCHAR(1024) */
        {{C_STRING_WITH_LEN("description")},
         {C_STRING_WITH_LEN("varchar(1024)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 9  gmt_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP */
        {{C_STRING_WITH_LEN("gmt_created")},
         {C_STRING_WITH_LEN("timestamp")},
         {NULL, 0}},
        /* 10 gmt_modified TIMESTAMP DEFAULT ... ON UPDATE ... */
        {{C_STRING_WITH_LEN("gmt_modified")},
         {C_STRING_WITH_LEN("timestamp")},
         {NULL, 0}},
};

static const TABLE_FIELD_DEF ai_table_def = {MYSQL_AI_FIELD_COUNT,
                                              mysql_ai_table_fields};

static const TABLE_FIELD_TYPE mysql_ai_function_default_table_fields
    [MYSQL_AI_FUNCTION_DEFAULT_FIELD_COUNT] = {
        /* 0 function_name VARCHAR(64) NOT NULL */
        {{C_STRING_WITH_LEN("function_name")},
         {C_STRING_WITH_LEN("varchar(64)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 1 model_name VARCHAR(128) NULL */
        {{C_STRING_WITH_LEN("model_name")},
         {C_STRING_WITH_LEN("varchar(128)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
        /* 2 description VARCHAR(1024) NOT NULL */
        {{C_STRING_WITH_LEN("description")},
         {C_STRING_WITH_LEN("varchar(1024)")},
         {C_STRING_WITH_LEN("utf8mb4")}},
};

static const TABLE_FIELD_DEF ai_function_default_table_def = {
    MYSQL_AI_FUNCTION_DEFAULT_FIELD_COUNT,
    mysql_ai_function_default_table_fields};

/* ================================================================
   Ai_reader implementation
   ================================================================ */

void Ai_reader::row_warning(Conf_record *record, const char *when,
                            const char *msg) {
  push_warning_printf(m_thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                      "AI model config record %llu: %s (%s)",
                      record->get_id(), when, msg ? msg : "");
}

void Ai_reader::print_ha_error(int errcode) {
  ha_error(ER_WRONG_ARGUMENTS, errcode);
}

void Ai_reader::log_error(Conf_error err) {
  if (err != Conf_error::CONF_OK) {
    LogErr(ERROR_LEVEL, ER_WRONG_ARGUMENTS,
           "ai_model_config table operation failed");
  }
}

Conf_record *Ai_reader::new_record() {
  return new (m_mem_root) Ai_record();
}

void Ai_reader::read_attributes(Conf_record *r) {
  DBUG_ENTER("Ai_reader::read_attributes");
  Ai_record *record = dynamic_cast<Ai_record *>(r);
  assert(record);
  record->reset();

  record->id = m_table->field[MYSQL_AI_FIELD_ID]->val_int();
  record->model_name =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_MODEL_NAME]);
  record->provider =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_PROVIDER]);
  record->endpoint =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_ENDPOINT]);
  record->model =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_MODEL]);
  const char *stored_api_key =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_API_KEY]);
  if (stored_api_key) {
    std::string api_key = deobfuscate_api_key(stored_api_key);
    record->api_key =
        strmake_root(m_mem_root, api_key.c_str(), api_key.length());
  }
  record->model_params =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_MODEL_PARAMS]);
  record->status =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_STATUS]);
  record->description =
      get_field(m_mem_root, m_table->field[MYSQL_AI_FIELD_DESCRIPTION]);

  DBUG_VOID_RETURN;
}

/* ================================================================
   Ai_writer implementation
   ================================================================ */

void Ai_writer::print_ha_error(int errcode) {
  ha_error(ER_WRONG_ARGUMENTS, errcode);
}

void Ai_writer::store_attributes(const Conf_record *r) {
  const Ai_record *record = dynamic_cast<const Ai_record *>(r);
  assert(record);

  if (m_op_type == Conf_table_op::OP_INSERT) {
    restore_record(m_table, s->default_values);

    /* model_name */
    if (record->model_name) {
      m_table->field[MYSQL_AI_FIELD_MODEL_NAME]->store(
          record->model_name, strlen(record->model_name),
          system_charset_info);
      m_table->field[MYSQL_AI_FIELD_MODEL_NAME]->set_notnull();
    }
    /* provider */
    if (record->provider) {
      m_table->field[MYSQL_AI_FIELD_PROVIDER]->store(
          record->provider, strlen(record->provider), system_charset_info);
      m_table->field[MYSQL_AI_FIELD_PROVIDER]->set_notnull();
    }
    /* endpoint */
    if (record->endpoint) {
      m_table->field[MYSQL_AI_FIELD_ENDPOINT]->store(
          record->endpoint, strlen(record->endpoint), system_charset_info);
      m_table->field[MYSQL_AI_FIELD_ENDPOINT]->set_notnull();
    }
    /* model (underlying model name) */
    if (record->model) {
      m_table->field[MYSQL_AI_FIELD_MODEL]->store(
          record->model, strlen(record->model), system_charset_info);
      m_table->field[MYSQL_AI_FIELD_MODEL]->set_notnull();
    } else {
      m_table->field[MYSQL_AI_FIELD_MODEL]->set_null();
    }
    /* api_key */
    if (record->api_key) {
      std::string api_key = obfuscate_api_key(record->api_key);
      m_table->field[MYSQL_AI_FIELD_API_KEY]->store(
          api_key.c_str(), api_key.length(), system_charset_info);
      m_table->field[MYSQL_AI_FIELD_API_KEY]->set_notnull();
    } else {
      m_table->field[MYSQL_AI_FIELD_API_KEY]->set_null();
    }
    /* model_params */
    if (record->model_params) {
      m_table->field[MYSQL_AI_FIELD_MODEL_PARAMS]->store(
          record->model_params, strlen(record->model_params),
          system_charset_info);
      m_table->field[MYSQL_AI_FIELD_MODEL_PARAMS]->set_notnull();
    } else {
      m_table->field[MYSQL_AI_FIELD_MODEL_PARAMS]->set_null();
    }
    /* status */
    if (record->status) {
      m_table->field[MYSQL_AI_FIELD_STATUS]->store(
          record->status, strlen(record->status), system_charset_info);
      m_table->field[MYSQL_AI_FIELD_STATUS]->set_notnull();
    }
    /* description */
    if (record->description) {
      m_table->field[MYSQL_AI_FIELD_DESCRIPTION]->store(
          record->description, strlen(record->description),
          system_charset_info);
      m_table->field[MYSQL_AI_FIELD_DESCRIPTION]->set_notnull();
    } else {
      m_table->field[MYSQL_AI_FIELD_DESCRIPTION]->set_null();
    }
  }
}

void Ai_writer::store_id(const Conf_record *r) {
  const Ai_record *record = dynamic_cast<const Ai_record *>(r);
  m_table->field[MYSQL_AI_FIELD_ID]->store(record->id, true);
}

void Ai_writer::retrieve_attr(Conf_record *r) {
  Ai_record *record = dynamic_cast<Ai_record *>(r);
  /* Retrieve model_name after index read */
  record->model_name = get_field(
      m_mem_root, m_table->field[MYSQL_AI_FIELD_MODEL_NAME]);
}

void Ai_writer::row_not_found_warning(Conf_record *r) {
  Ai_record *record = dynamic_cast<Ai_record *>(r);
  push_warning_printf(
      m_thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
      "AI model id %llu not found in table", record->id);
}

/* ================================================================
   Ai_function_default_reader implementation
   ================================================================ */

void Ai_function_default_reader::row_warning(Conf_record *record,
                                             const char *when,
                                             const char *msg) {
  Ai_function_default_record *r =
      dynamic_cast<Ai_function_default_record *>(record);
  push_warning_printf(
      m_thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
      "AI function default record '%s': %s (%s)",
      (r && r->function_name) ? r->function_name : "", when,
      msg ? msg : "");
}

void Ai_function_default_reader::print_ha_error(int errcode) {
  ha_error(ER_WRONG_ARGUMENTS, errcode);
}

void Ai_function_default_reader::log_error(Conf_error err) {
  if (err != Conf_error::CONF_OK) {
    LogErr(ERROR_LEVEL, ER_WRONG_ARGUMENTS,
           "ai_function_config table operation failed");
  }
}

Conf_record *Ai_function_default_reader::new_record() {
  return new (m_mem_root) Ai_function_default_record();
}

void Ai_function_default_reader::read_attributes(Conf_record *r) {
  DBUG_ENTER("Ai_function_default_reader::read_attributes");
  Ai_function_default_record *record =
      dynamic_cast<Ai_function_default_record *>(r);
  assert(record);
  record->reset();

  record->function_name = get_field(
      m_mem_root,
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME]);
  record->model_name = get_field(
      m_mem_root, m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]);
  record->description = get_field(
      m_mem_root,
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]);

  DBUG_VOID_RETURN;
}

/* ================================================================
   Ai_function_default_writer implementation
   ================================================================ */

void Ai_function_default_writer::print_ha_error(int errcode) {
  ha_error(ER_WRONG_ARGUMENTS, errcode);
}

void Ai_function_default_writer::store_attributes(const Conf_record *r) {
  const Ai_function_default_record *record =
      dynamic_cast<const Ai_function_default_record *>(r);
  assert(record);

  if (m_op_type == Conf_table_op::OP_INSERT) {
    restore_record(m_table, s->default_values);

    if (record->function_name) {
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME]->store(
          record->function_name, strlen(record->function_name),
          system_charset_info);
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME]
          ->set_notnull();
    }
    if (record->model_name) {
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]->store(
          record->model_name, strlen(record->model_name), system_charset_info);
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]
          ->set_notnull();
    } else {
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]->set_null();
    }
    if (record->description) {
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]->store(
          record->description, strlen(record->description),
          system_charset_info);
      m_table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]
          ->set_notnull();
    }
  }
}

void Ai_function_default_writer::row_not_found_warning(Conf_record *r) {
  Ai_function_default_record *record =
      dynamic_cast<Ai_function_default_record *>(r);
  push_warning_printf(
      m_thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
      "AI function default '%s' not found in table",
      (record && record->function_name) ? record->function_name : "");
}

/* ================================================================
   Table open / close helpers
   ================================================================ */

Conf_error open_ai_table(THD *thd, TABLE_LIST_PTR &table_list, bool write) {
  DBUG_ENTER("open_ai_table");
  if (open_conf_table(thd, table_list, AI_SCHEMA_NAME, AI_TABLE_NAME,
                      AI_TABLE_ALIAS, &ai_table_def, write))
    DBUG_RETURN(Conf_error::CONF_ER_TABLE_OP_ERROR);

  DBUG_RETURN(Conf_error::CONF_OK);
}

Conf_error open_ai_function_default_table(THD *thd,
                                          TABLE_LIST_PTR &table_list,
                                          bool write) {
  DBUG_ENTER("open_ai_function_default_table");
  if (open_conf_table(thd, table_list, AI_SCHEMA_NAME,
                      AI_FUNCTION_DEFAULT_TABLE_NAME,
                      AI_FUNCTION_DEFAULT_TABLE_ALIAS,
                      &ai_function_default_table_def, write))
    DBUG_RETURN(Conf_error::CONF_ER_TABLE_OP_ERROR);

  DBUG_RETURN(Conf_error::CONF_OK);
}

bool ai_end_trans(THD *thd, bool rollback) {
  return conf_end_trans(thd, rollback);
}

/* ================================================================
   High-level CRUD operations
   ================================================================ */

/**
  Insert a new model into mysql.ai_model_config.
*/
bool add_ai_model(THD *thd, Ai_record *record) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  DBUG_ENTER("add_ai_model");

  if ((error = open_ai_table(thd, table_list, true)) !=
      Conf_error::CONF_OK) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai: failed to open ai_model_config table",
                    MYF(0));
    DBUG_RETURN(true);
  }

  {
    Ai_writer writer(thd, table_list->table, thd->mem_root,
                     Conf_table_op::OP_INSERT);

    int write_rc = writer.write_row(record);
    if (write_rc) goto err;
  }

  if (ai_end_trans(thd, false)) DBUG_RETURN(true);
  DBUG_RETURN(false);

err:
  ai_end_trans(thd, true);
  DBUG_RETURN(true);
}

/**
  Delete a model by model_name. Scans all rows looking for a name match.
*/
bool del_ai_model_by_name(THD *thd, const char *model_name) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  bool found = false;
  DBUG_ENTER("del_ai_model_by_name");

  if ((error = open_ai_table(thd, table_list, true)) !=
      Conf_error::CONF_OK) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai: failed to open ai_model_config table",
                    MYF(0));
    DBUG_RETURN(true);
  }

  TABLE *table = table_list->table;
  table->use_all_columns();

  /* Full table scan to find the row by model_name */
  unique_ptr_destroy_only<RowIterator> iterator =
      init_table_iterator(thd, table, false, false);
  if (iterator == nullptr) goto err;

  {
    int read_err;
    while (!(read_err = iterator->Read())) {
      char *name = get_field(thd->mem_root,
                             table->field[MYSQL_AI_FIELD_MODEL_NAME]);
      if (name && strcasecmp(name, model_name) == 0) {
        found = true;
        if (table->file->ha_delete_row(table->record[0])) {
          my_printf_error(ER_WRONG_ARGUMENTS,
                          "dbms_ai: failed to delete model '%s'",
                          MYF(0), model_name);
          goto err;
        }
        break;
      }
    }
  }

  iterator.reset(nullptr);

  if (!found) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai: model '%s' not found", MYF(0), model_name);
    goto err;
  }

  ai_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  ai_end_trans(thd, true);
  DBUG_RETURN(true);
}

/**
  Update a model's endpoint/api_key/model_params by model_name.
  Only non-NULL fields in the record are updated.
*/
bool update_ai_model(THD *thd, Ai_record *record) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  bool found = false;
  DBUG_ENTER("update_ai_model");

  if ((error = open_ai_table(thd, table_list, true)) !=
      Conf_error::CONF_OK) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai: failed to open ai_model_config table",
                    MYF(0));
    DBUG_RETURN(true);
  }

  TABLE *table = table_list->table;
  table->use_all_columns();

  {
    unique_ptr_destroy_only<RowIterator> iterator =
        init_table_iterator(thd, table, false, false);
    if (iterator == nullptr) goto err;

    int read_err;
    while (!(read_err = iterator->Read())) {
      char *name = get_field(thd->mem_root,
                             table->field[MYSQL_AI_FIELD_MODEL_NAME]);
      if (name && strcasecmp(name, record->model_name) == 0) {
        found = true;
        /* Save current row for update */
        store_record(table, record[1]);

        /* Update only non-NULL fields */
        if (record->provider && record->provider[0] != '\0') {
          table->field[MYSQL_AI_FIELD_PROVIDER]->store(
              record->provider, strlen(record->provider),
              system_charset_info);
          table->field[MYSQL_AI_FIELD_PROVIDER]->set_notnull();
        }
        if (record->endpoint && record->endpoint[0] != '\0') {
          table->field[MYSQL_AI_FIELD_ENDPOINT]->store(
              record->endpoint, strlen(record->endpoint),
              system_charset_info);
          table->field[MYSQL_AI_FIELD_ENDPOINT]->set_notnull();
        }
        if (record->model && record->model[0] != '\0') {
          table->field[MYSQL_AI_FIELD_MODEL]->store(
              record->model, strlen(record->model),
              system_charset_info);
          table->field[MYSQL_AI_FIELD_MODEL]->set_notnull();
        }
        if (record->api_key && record->api_key[0] != '\0') {
          std::string api_key = obfuscate_api_key(record->api_key);
          table->field[MYSQL_AI_FIELD_API_KEY]->store(
              api_key.c_str(), api_key.length(), system_charset_info);
          table->field[MYSQL_AI_FIELD_API_KEY]->set_notnull();
        }
        if (record->model_params && record->model_params[0] != '\0') {
          table->field[MYSQL_AI_FIELD_MODEL_PARAMS]->store(
              record->model_params, strlen(record->model_params),
              system_charset_info);
          table->field[MYSQL_AI_FIELD_MODEL_PARAMS]->set_notnull();
        }
        if (record->status && record->status[0] != '\0') {
          table->field[MYSQL_AI_FIELD_STATUS]->store(
              record->status, strlen(record->status),
              system_charset_info);
          table->field[MYSQL_AI_FIELD_STATUS]->set_notnull();
        }
        if (record->description && record->description[0] != '\0') {
          table->field[MYSQL_AI_FIELD_DESCRIPTION]->store(
              record->description, strlen(record->description),
              system_charset_info);
          table->field[MYSQL_AI_FIELD_DESCRIPTION]->set_notnull();
        }

        int upd_err = table->file->ha_update_row(table->record[1],
                                                   table->record[0]);
        if (upd_err && upd_err != HA_ERR_RECORD_IS_THE_SAME) {
          my_printf_error(ER_WRONG_ARGUMENTS,
                          "dbms_ai: failed to update model '%s' (error %d)",
                          MYF(0), record->model_name, upd_err);
          goto err;
        }
        break;
      }
    }
    iterator.reset(nullptr);
  }

  if (!found) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai: model '%s' not found", MYF(0),
                    record->model_name);
    goto err;
  }

  ai_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  ai_end_trans(thd, true);
  DBUG_RETURN(true);
}

/**
  Insert or update one function default model mapping.
*/
bool upsert_ai_function_default_model(THD *thd, const char *function_name,
                                      const char *model_name,
                                      const char *description) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  bool found = false;
  DBUG_ENTER("upsert_ai_function_default_model");

  if ((error = open_ai_function_default_table(thd, table_list, true)) !=
      Conf_error::CONF_OK) {
    my_printf_error(
        ER_WRONG_ARGUMENTS,
        "dbms_ai: failed to open ai_function_config table",
        MYF(0));
    DBUG_RETURN(true);
  }

  TABLE *table = table_list->table;
  table->use_all_columns();

  {
    unique_ptr_destroy_only<RowIterator> iterator =
        init_table_iterator(thd, table, false, false);
    if (iterator == nullptr) goto err;

    int read_err;
    while (!(read_err = iterator->Read())) {
      char *name = get_field(
          thd->mem_root,
          table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME]);
      if (name && strcasecmp(name, function_name) == 0) {
        found = true;
        store_record(table, record[1]);
        table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]->store(
            model_name, strlen(model_name), system_charset_info);
        table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]->set_notnull();
        if (description) {
          table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]->store(
              description, strlen(description), system_charset_info);
          table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]
              ->set_notnull();
        }

        int upd_err = table->file->ha_update_row(table->record[1],
                                                 table->record[0]);
        if (upd_err && upd_err != HA_ERR_RECORD_IS_THE_SAME) {
          my_printf_error(
              ER_WRONG_ARGUMENTS,
              "dbms_ai: failed to update default model for '%s' (error %d)",
              MYF(0), function_name, upd_err);
          goto err;
        }
        break;
      }
    }
    iterator.reset(nullptr);
  }

  if (!found) {
    restore_record(table, s->default_values);
    table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME]->store(
        function_name, strlen(function_name), system_charset_info);
    table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME]->set_notnull();
    table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]->store(
        model_name, strlen(model_name), system_charset_info);
    table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME]->set_notnull();
    if (description) {
      table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]->store(
          description, strlen(description), system_charset_info);
      table->field[MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION]->set_notnull();
    }

    int write_err = table->file->ha_write_row(table->record[0]);
    if (write_err && !table->file->is_ignorable_error(write_err)) {
      my_printf_error(
          ER_WRONG_ARGUMENTS,
          "dbms_ai: failed to insert default model for '%s' (error %d)",
          MYF(0), function_name, write_err);
      goto err;
    }
  }

  ai_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  ai_end_trans(thd, true);
  DBUG_RETURN(true);
}

/**
  Read all rows from mysql.ai_model_config.
*/
bool read_all_ai_models(THD *thd, std::vector<Ai_record *> *result) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  DBUG_ENTER("read_all_ai_models");
  result->clear();

  if ((error = open_ai_table(thd, table_list, false)) !=
      Conf_error::CONF_OK) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai: failed to open ai_model_config table",
                    MYF(0));
    DBUG_RETURN(true);
  }

  {
    TABLE *table = table_list->table;
    Ai_reader reader(thd, table, thd->mem_root);
    Conf_error read_error;
    Conf_records *records = reader.read_all_rows(&read_error);

    if (read_error != Conf_error::CONF_OK || !records) {
      commit_and_close_conf_table(thd);
      DBUG_RETURN(true);
    }

    for (auto *r : *records) {
      result->push_back(dynamic_cast<Ai_record *>(r));
    }
    destroy(records);
  }

  commit_and_close_conf_table(thd);
  DBUG_RETURN(false);
}

/**
  Read all rows from mysql.ai_function_config.
*/
bool read_all_ai_function_default_models(
    THD *thd, std::vector<Ai_function_default_record *> *result) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  DBUG_ENTER("read_all_ai_function_default_models");
  result->clear();

  if ((error = open_ai_function_default_table(thd, table_list, false)) !=
      Conf_error::CONF_OK) {
    my_printf_error(
        ER_WRONG_ARGUMENTS,
        "dbms_ai: failed to open ai_function_config table",
        MYF(0));
    DBUG_RETURN(true);
  }

  {
    TABLE *table = table_list->table;
    Ai_function_default_reader reader(thd, table, thd->mem_root);
    Conf_error read_error;
    Conf_records *records = reader.read_all_rows(&read_error);

    if (read_error != Conf_error::CONF_OK || !records) {
      commit_and_close_conf_table(thd);
      DBUG_RETURN(true);
    }

    for (auto *r : *records) {
      result->push_back(dynamic_cast<Ai_function_default_record *>(r));
    }
    destroy(records);
  }

  commit_and_close_conf_table(thd);
  DBUG_RETURN(false);
}

/**
  Read a single model by name. Full-table scan with name comparison.
*/
Ai_record *read_ai_model_by_name(THD *thd, const char *model_name,
                                 bool *found) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  Ai_record *result = nullptr;
  DBUG_ENTER("read_ai_model_by_name");
  *found = false;

  if ((error = open_ai_table(thd, table_list, false)) !=
      Conf_error::CONF_OK) {
    DBUG_RETURN(nullptr);
  }

  {
    TABLE *table = table_list->table;
    Ai_reader reader(thd, table, thd->mem_root);
    Conf_error read_error;
    Conf_records *records = reader.read_all_rows(&read_error);

    if (read_error != Conf_error::CONF_OK || !records) {
      commit_and_close_conf_table(thd);
      DBUG_RETURN(nullptr);
    }

    for (auto *r : *records) {
      Ai_record *rec = dynamic_cast<Ai_record *>(r);
      if (rec && rec->model_name &&
          strcasecmp(rec->model_name, model_name) == 0) {
        result = rec;
        *found = true;
        break;
      }
    }
    destroy(records);
  }

  commit_and_close_conf_table(thd);
  DBUG_RETURN(result);
}

} /* namespace im */
