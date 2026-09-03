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

#ifndef SQL_AI_AI_TABLE_INCLUDED
#define SQL_AI_AI_TABLE_INCLUDED

/**
  @file sql/ai/ai_table.h
  AI model configuration system table (mysql.ai_model_config) access layer.
  Follows the Conf_reader / Conf_writer pattern used by CCL, Outline, etc.
*/

#include "sql/common/table.h"
#include "sql/common/table_common.h"
#include "sql/sql_class.h"
#include "sql/table.h"

class THD;

namespace im {

/* ---------- field ordinals for mysql.ai_model_config ---------- */
enum mysql_ai_table_field {
  MYSQL_AI_FIELD_ID = 0,
  MYSQL_AI_FIELD_MODEL_NAME,
  MYSQL_AI_FIELD_PROVIDER,
  MYSQL_AI_FIELD_ENDPOINT,
  MYSQL_AI_FIELD_MODEL,
  MYSQL_AI_FIELD_API_KEY,
  MYSQL_AI_FIELD_MODEL_PARAMS,
  MYSQL_AI_FIELD_STATUS,
  MYSQL_AI_FIELD_DESCRIPTION,
  MYSQL_AI_FIELD_GMT_CREATED,
  MYSQL_AI_FIELD_GMT_MODIFIED,
  MYSQL_AI_FIELD_COUNT
};

/* ---------- field ordinals for mysql.ai_function_config ---------- */
enum mysql_ai_function_default_table_field {
  MYSQL_AI_FUNCTION_DEFAULT_FIELD_FUNCTION_NAME = 0,
  MYSQL_AI_FUNCTION_DEFAULT_FIELD_MODEL_NAME,
  MYSQL_AI_FUNCTION_DEFAULT_FIELD_DESCRIPTION,
  MYSQL_AI_FUNCTION_DEFAULT_FIELD_COUNT
};

/* ---------- Ai_record : one row in mysql.ai_model_config ---------- */
struct Ai_record : public Conf_record {
 public:
  ulonglong id;
  const char *model_name;
  const char *provider;
  const char *endpoint;
  const char *model;
  const char *api_key;
  const char *model_params;
  const char *status;
  const char *description;

 public:
  Ai_record() { reset(); }

  void reset() {
    id = 0;
    model_name = nullptr;
    provider = nullptr;
    endpoint = nullptr;
    model = nullptr;
    api_key = nullptr;
    model_params = nullptr;
    status = nullptr;
    description = nullptr;
  }

  virtual ulonglong get_id() const override { return id; }
  virtual void set_id(ulonglong value) override { id = value; }

  virtual bool check_valid(const char **msg) const override {
    if (!model_name || model_name[0] == '\0') {
      *msg = "model_name is empty";
      return false;
    }
    return true;
  }

  virtual bool check_active() const override {
    if (!status) return true;
    return (strcasecmp(status, "ACTIVE") == 0);
  }
};

/* ---------- Ai_function_default_record : one row in default mapping table ---------- */
struct Ai_function_default_record : public Conf_record {
 public:
  const char *function_name;
  const char *model_name;
  const char *description;

 public:
  Ai_function_default_record() { reset(); }

  void reset() {
    function_name = nullptr;
    model_name = nullptr;
    description = nullptr;
  }

  virtual ulonglong get_id() const override { return 0; }
  virtual void set_id(ulonglong value [[maybe_unused]]) override {}

  virtual bool check_valid(const char **msg) const override {
    if (!function_name || function_name[0] == '\0') {
      *msg = "function_name is empty";
      return false;
    }
    return true;
  }

  virtual bool check_active() const override { return true; }
};

/* ---------- Ai_reader ---------- */
class Ai_reader : public Conf_reader {
 public:
  explicit Ai_reader(THD *thd, TABLE *table, MEM_ROOT *mem_root)
      : Conf_reader(thd, table, mem_root) {}

  ~Ai_reader() override {}

  virtual void row_warning(Conf_record *record, const char *when,
                           const char *msg) override;
  virtual void print_ha_error(int errcode) override;
  virtual void log_error(Conf_error err) override;
  virtual void read_attributes(Conf_record *record) override;
  virtual Conf_record *new_record() override;
};

/* ---------- Ai_writer ---------- */
class Ai_writer : public Conf_writer {
 public:
  explicit Ai_writer(THD *thd, TABLE *table, MEM_ROOT *mem_root,
                     Conf_table_op op_type)
      : Conf_writer(thd, table, mem_root, op_type) {}

  virtual void print_ha_error(int errcode) override;
  virtual void store_attributes(const Conf_record *r) override;
  virtual bool has_autoinc() override { return true; }
  virtual void store_id(const Conf_record *r) override;
  virtual void retrieve_attr(Conf_record *r) override;
  virtual void row_not_found_warning(Conf_record *r) override;
};

/* ---------- Ai_function_default_reader ---------- */
class Ai_function_default_reader : public Conf_reader {
 public:
  explicit Ai_function_default_reader(THD *thd, TABLE *table,
                                      MEM_ROOT *mem_root)
      : Conf_reader(thd, table, mem_root) {}

  ~Ai_function_default_reader() override {}

  virtual void row_warning(Conf_record *record, const char *when,
                           const char *msg) override;
  virtual void print_ha_error(int errcode) override;
  virtual void log_error(Conf_error err) override;
  virtual void read_attributes(Conf_record *record) override;
  virtual Conf_record *new_record() override;
};

/* ---------- Ai_function_default_writer ---------- */
class Ai_function_default_writer : public Conf_writer {
 public:
  explicit Ai_function_default_writer(THD *thd, TABLE *table,
                                      MEM_ROOT *mem_root,
                                      Conf_table_op op_type)
      : Conf_writer(thd, table, mem_root, op_type) {}

  virtual void print_ha_error(int errcode) override;
  virtual void store_attributes(const Conf_record *r) override;
  virtual bool has_autoinc() override { return false; }
  virtual void store_id(const Conf_record *r [[maybe_unused]]) override {}
  virtual void retrieve_attr(Conf_record *r [[maybe_unused]]) override {}
  virtual void row_not_found_warning(Conf_record *r) override;
};

/* ---------- Table open / close helpers ---------- */
Conf_error open_ai_table(THD *thd, TABLE_LIST_PTR &table_list, bool write);
Conf_error open_ai_function_default_table(THD *thd,
                                          TABLE_LIST_PTR &table_list,
                                          bool write);
bool ai_end_trans(THD *thd, bool rollback);

/* ---------- High-level CRUD ---------- */
bool add_ai_model(THD *thd, Ai_record *record);
bool del_ai_model_by_name(THD *thd, const char *model_name);
bool update_ai_model(THD *thd, Ai_record *record);
bool upsert_ai_function_default_model(THD *thd, const char *function_name,
                                      const char *model_name,
                                      const char *description);

/**
  Read all AI model configs from table.
  @param[in]   thd    Thread context
  @param[out]  result Output vector (caller provides stack-allocated vector)
  @retval false  Success
  @retval true   Error
*/
bool read_all_ai_models(THD *thd, std::vector<Ai_record *> *result);
bool read_all_ai_function_default_models(
    THD *thd, std::vector<Ai_function_default_record *> *result);

/**
  Read a single model by name.
  Returns nullptr if not found (sets *found = false).
*/
Ai_record *read_ai_model_by_name(THD *thd, const char *model_name,
                                 bool *found);

} /* namespace im */

#endif /* SQL_AI_AI_TABLE_INCLUDED */
