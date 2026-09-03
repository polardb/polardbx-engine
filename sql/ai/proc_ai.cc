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
  @file sql/ai/proc_ai.cc
  Implementation of dbms_ai package procedures for AI model management.
*/

#include "sql/ai/proc_ai.h"
#include "sql/ai/ai_cache.h"
#include "sql/ai/ai_function_defs.h"
#include "sql/ai/ai_table.h"

#include "my_dbug.h"
#include "my_rapidjson_size_t.h"
#include "mysqld_error.h"
#include "sql/log.h"
#include "sql/package/package_common.h"

#include <rapidjson/document.h>

#include <algorithm>

namespace im {

/* The uniform schema name for AI procedures */
const LEX_CSTRING AI_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_ai")};

/* ================================================================
   Helper: extract a VARCHAR arg from the parameter list
   ================================================================ */
static const char *get_str_arg(THD *thd, mem_root_deque<Item *> *list,
                               size_t idx) {
  char buff[2048];
  String str(buff, sizeof(buff), system_charset_info);
  String *res = (*list)[idx]->val_str(&str);
  if (!res || res->length() == 0) return nullptr;
  return strmake_root(thd->mem_root, res->ptr(), res->length());
}

/**
  Helper: get a string member from a rapidjson object (case-insensitive key).
  Returns nullptr if not present or not a string.
*/
static const char *json_get_string(THD *thd,
                                   const rapidjson::Document &doc,
                                   const char *key) {
  /* Case-insensitive key lookup */
  for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    if (strcasecmp(it->name.GetString(), key) == 0) {
      if (!it->value.IsString()) return nullptr;
      size_t len = it->value.GetStringLength();
      if (len == 0) return nullptr;
      return strmake_root(thd->mem_root, it->value.GetString(), len);
    }
  }
  return nullptr;
}

static const Ai_function_def *find_ai_function(const char *function_name) {
  for (size_t i = 0; i < ai_function_defs_count; i++) {
    const Ai_function_def &m = ai_function_defs[i];
    if (function_name && strcasecmp(function_name, m.function_name) == 0)
      return &m;
  }
  return nullptr;
}

static size_t ai_function_display_order(const char *function_name) {
  for (size_t i = 0; i < ai_function_defs_count; i++) {
    const Ai_function_def &m = ai_function_defs[i];
    if (function_name && strcasecmp(function_name, m.function_name) == 0)
      return i;
  }
  return ai_function_defs_count;
}

/* ================================================================
   1) dbms_ai.register_model
   ================================================================ */

Proc *Ai_proc_register_model::instance() {
  static Proc *proc = new Ai_proc_register_model(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_register_model::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_register_model::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_register_model::pc_execute");

  Ai_record *record = new (thd->mem_root) Ai_record();
  record->model_name = get_str_arg(thd, m_list, 0);
  record->provider = get_str_arg(thd, m_list, 1);
  record->endpoint = get_str_arg(thd, m_list, 2);
  record->model = get_str_arg(thd, m_list, 3);
  record->status = "ACTIVE";

  if (!record->model_name || !record->provider || !record->endpoint) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.register_model: model_name, "
                    "provider, and endpoint are required",
                    MYF(0));
    DBUG_RETURN(true);
  }

  /* Parse optional options_json (5th parameter) */
  const char *options_str = get_str_arg(thd, m_list, 4);
  if (options_str && options_str[0] != '\0') {
    rapidjson::Document doc;
    doc.Parse(options_str);
    if (!doc.HasParseError() && doc.IsObject()) {
      const char *v;
      if ((v = json_get_string(thd, doc, "api_key")))
        record->api_key = v;
      if ((v = json_get_string(thd, doc, "description")))
        record->description = v;
    }
  }

  /* Check for duplicate model_name before insert */
  {
    bool found = false;
    read_ai_model_by_name(thd, record->model_name, &found);
    if (found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "dbms_ai: model '%s' already exists",
                      MYF(0), record->model_name);
      DBUG_RETURN(true);
    }
  }

  bool error = add_ai_model(thd, record);
  if (!error) reload_ai_model_cache(thd);
  DBUG_RETURN(error);
}

/* ================================================================
   2) dbms_ai.list_models
   ================================================================ */

Proc *Ai_proc_list_models::instance() {
  static Proc *proc = new Ai_proc_list_models(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_list_models::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_list_models::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_list_models::pc_execute");

  std::vector<Ai_record *> models;
  if (read_all_ai_models(thd, &models)) {
    DBUG_RETURN(true);
  }

  m_records.clear();
  if (m_records.reserve(models.size())) DBUG_RETURN(true);
  for (auto *rec : models) {
    if (m_records.push_back(rec)) DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

void Sql_cmd_ai_list_models::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("Sql_cmd_ai_list_models::send_result");

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto *record : m_records) {
    protocol->start_row();
    protocol->store_string(record->model_name ? record->model_name : "",
                           record->model_name ? strlen(record->model_name) : 0,
                           system_charset_info);
    protocol->store_string(record->provider ? record->provider : "",
                           record->provider ? strlen(record->provider) : 0,
                           system_charset_info);
    std::string ep = record->endpoint ? record->endpoint : "";
    protocol->store_string(ep.c_str(), ep.length(), system_charset_info);
    protocol->store_string(record->model ? record->model : "",
                           record->model ? strlen(record->model) : 0,
                           system_charset_info);
    protocol->store_string(record->status ? record->status : "",
                           record->status ? strlen(record->status) : 0,
                           system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/* ================================================================
   3) dbms_ai.describe_model
   ================================================================ */

Proc *Ai_proc_describe_model::instance() {
  static Proc *proc = new Ai_proc_describe_model(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_describe_model::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_describe_model::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_describe_model::pc_execute");

  const char *model_name = get_str_arg(thd, m_list, 0);
  if (!model_name || model_name[0] == '\0') {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.describe_model: model_name is required",
                    MYF(0));
    DBUG_RETURN(true);
  }

  bool found = false;
  m_record = read_ai_model_by_name(thd, model_name, &found);
  if (!found || !m_record) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.describe_model: model '%s' not found",
                    MYF(0), model_name);
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

/**
  Mask the api_key for display: show first 4 and last 4 chars.
*/
static std::string mask_api_key(const char *key) {
  if (!key || strlen(key) == 0) return "";
  size_t len = strlen(key);
  if (len <= 8) return "****";
  return std::string(key, 4) + "****" + std::string(key + len - 4, 4);
}

void Sql_cmd_ai_describe_model::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("Sql_cmd_ai_describe_model::send_result");

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  if (m_record) {
    std::string masked_key = mask_api_key(m_record->api_key);
    std::string ep = m_record->endpoint ? m_record->endpoint : "";

    protocol->start_row();
    protocol->store_string(
        m_record->model_name ? m_record->model_name : "",
        m_record->model_name ? strlen(m_record->model_name) : 0,
        system_charset_info);
    protocol->store_string(m_record->provider ? m_record->provider : "",
                           m_record->provider ? strlen(m_record->provider) : 0,
                           system_charset_info);
    protocol->store_string(ep.c_str(), ep.length(), system_charset_info);
    protocol->store_string(
        m_record->model ? m_record->model : "",
        m_record->model ? strlen(m_record->model) : 0,
        system_charset_info);
    protocol->store_string(masked_key.c_str(), masked_key.length(),
                           system_charset_info);
    protocol->store_string(
        m_record->model_params ? m_record->model_params : "",
        m_record->model_params ? strlen(m_record->model_params) : 0,
        system_charset_info);
    protocol->store_string(m_record->status ? m_record->status : "",
                           m_record->status ? strlen(m_record->status) : 0,
                           system_charset_info);
    protocol->store_string(
        m_record->description ? m_record->description : "",
        m_record->description ? strlen(m_record->description) : 0,
        system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/* ================================================================
   4) dbms_ai.update_model
   ================================================================ */

Proc *Ai_proc_update_model::instance() {
  static Proc *proc = new Ai_proc_update_model(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_update_model::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_update_model::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_update_model::pc_execute");

  const char *model_name = get_str_arg(thd, m_list, 0);
  const char *options_str = get_str_arg(thd, m_list, 1);

  if (!model_name || model_name[0] == '\0') {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.update_model: model_name is required",
                    MYF(0));
    DBUG_RETURN(true);
  }

  if (!options_str || options_str[0] == '\0') {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.update_model: options_json is required",
                    MYF(0));
    DBUG_RETURN(true);
  }

  Ai_record *record = new (thd->mem_root) Ai_record();
  record->model_name = model_name;

  rapidjson::Document doc;
  doc.Parse(options_str);
  if (doc.HasParseError() || !doc.IsObject()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.update_model: invalid JSON in options",
                    MYF(0));
    DBUG_RETURN(true);
  }

  const char *v;
  if ((v = json_get_string(thd, doc, "endpoint"))) {
    record->endpoint = v;
  }
  if ((v = json_get_string(thd, doc, "provider"))) {
    if (strcasecmp(v, "dashscope") != 0 && strcasecmp(v, "openai") != 0) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "dbms_ai.update_model: provider must be 'dashscope' "
                      "or 'openai', got '%s'",
                      MYF(0), v);
      DBUG_RETURN(true);
    }
    record->provider = v;
  }
  if ((v = json_get_string(thd, doc, "model")))
    record->model = v;
  if ((v = json_get_string(thd, doc, "api_key")))
    record->api_key = v;
  if ((v = json_get_string(thd, doc, "model_params")))
    record->model_params = v;
  if ((v = json_get_string(thd, doc, "status")))
    record->status = v;
  if ((v = json_get_string(thd, doc, "description"))) record->description = v;

  /* Check that at least one updatable field was provided */
  if (!record->endpoint && !record->provider && !record->model &&
      !record->api_key && !record->model_params && !record->status &&
      !record->description) {
    my_printf_error(
        ER_WRONG_ARGUMENTS,
        "dbms_ai.update_model: no updatable field found in options. "
        "Supported fields: endpoint, provider, model, api_key, "
        "model_params, status, description",
        MYF(0));
    DBUG_RETURN(true);
  }

  bool error = update_ai_model(thd, record);
  if (!error) reload_ai_model_cache(thd);
  DBUG_RETURN(error);
}

/* ================================================================
   5) dbms_ai.update_function
   ================================================================ */

Proc *Ai_proc_update_function::instance() {
  static Proc *proc = new Ai_proc_update_function(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_update_function::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_update_function::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_update_function::pc_execute");

  const char *function_name = get_str_arg(thd, m_list, 0);
  const char *model_name = get_str_arg(thd, m_list, 1);

  const Ai_function_def *function_def = find_ai_function(function_name);
  if (!function_def) {
    my_printf_error(ER_WRONG_ARGUMENTS, "Unknown AI function.", MYF(0));
    DBUG_RETURN(true);
  }

  if (!model_name || model_name[0] == '\0') {
    my_printf_error(ER_WRONG_ARGUMENTS, "Model not found.", MYF(0));
    DBUG_RETURN(true);
  }
  if (strlen(model_name) > 128) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.update_function: model_name is too long",
                    MYF(0));
    DBUG_RETURN(true);
  }

  bool found = false;
  read_ai_model_by_name(thd, model_name, &found);
  if (!found) {
    my_printf_error(ER_WRONG_ARGUMENTS, "Model not found.", MYF(0));
    DBUG_RETURN(true);
  }

  bool error = upsert_ai_function_default_model(
      thd, function_def->function_name, model_name, function_def->description);
  if (!error) reload_ai_model_cache(thd);
  DBUG_RETURN(error);
}

/* ================================================================
   6) dbms_ai.show_ai_function
   ================================================================ */

Proc *Ai_proc_show_ai_function::instance() {
  static Proc *proc = new Ai_proc_show_ai_function(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_show_ai_function::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_show_ai_function::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_show_ai_function::pc_execute");

  std::vector<Ai_function_default_record *> functions;
  if (read_all_ai_function_default_models(thd, &functions)) {
    DBUG_RETURN(true);
  }
  std::vector<Ai_record *> models;
  if (read_all_ai_models(thd, &models)) {
    DBUG_RETURN(true);
  }

  std::vector<Ai_function_display_record *> records;
  for (auto *rec : functions) {
    Ai_function_display_record *display =
        new (thd->mem_root) Ai_function_display_record();
    display->function_name = rec->function_name;
    display->default_model = rec->model_name;
    display->actual_model = "";
    display->description = rec->description ? rec->description : "";
    display->display_order = ai_function_display_order(rec->function_name);

    for (auto *model : models) {
      if (rec->model_name && model->model_name &&
          strcasecmp(rec->model_name, model->model_name) == 0) {
        display->actual_model = model->model ? model->model : "";
        break;
      }
    }
    records.push_back(display);
  }

  std::sort(records.begin(), records.end(),
            [](const Ai_function_display_record *lhs,
               const Ai_function_display_record *rhs) {
              if (lhs->display_order != rhs->display_order)
                return lhs->display_order < rhs->display_order;
              const char *l = lhs->function_name ? lhs->function_name : "";
              const char *r = rhs->function_name ? rhs->function_name : "";
              return strcmp(l, r) < 0;
            });

  m_records.clear();
  if (m_records.reserve(records.size())) DBUG_RETURN(true);
  for (auto *rec : records) {
    if (m_records.push_back(rec)) DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

void Sql_cmd_ai_show_ai_function::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("Sql_cmd_ai_show_ai_function::send_result");

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto *record : m_records) {
    protocol->start_row();
    protocol->store_string(
        record->function_name ? record->function_name : "",
        record->function_name ? strlen(record->function_name) : 0,
        system_charset_info);
    protocol->store_string(record->default_model ? record->default_model : "",
                           record->default_model
                               ? strlen(record->default_model)
                               : 0,
                           system_charset_info);
    protocol->store_string(record->actual_model ? record->actual_model : "",
                           record->actual_model
                               ? strlen(record->actual_model)
                               : 0,
                           system_charset_info);
    protocol->store_string(record->description ? record->description : "",
                           record->description ? strlen(record->description)
                                               : 0,
                           system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/* ================================================================
   7) dbms_ai.drop_model
   ================================================================ */

Proc *Ai_proc_drop_model::instance() {
  static Proc *proc = new Ai_proc_drop_model(key_memory_package);
  return proc;
}

Sql_cmd *Ai_proc_drop_model::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_ai_drop_model::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ai_drop_model::pc_execute");

  const char *model_name = get_str_arg(thd, m_list, 0);
  if (!model_name || model_name[0] == '\0') {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "dbms_ai.drop_model: model_name is required",
                    MYF(0));
    DBUG_RETURN(true);
  }

  bool error = del_ai_model_by_name(thd, model_name);
  if (!error) reload_ai_model_cache(thd);
  DBUG_RETURN(error);
}

} /* namespace im */
