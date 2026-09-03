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
  @file sql/ai/ai_cache.cc
  In-memory cache for AI model configs, modeled after CCL / Outline caches.
*/

#include "sql/ai/ai_cache.h"
#include "sql/ai/ai_reload.h"
#include "sql/ai/ai_table.h"

#include <cstdlib>
#include <string>

#include "my_dbug.h"
#include "mysql/psi/mysql_rwlock.h"
#include "sql/current_thd.h"
#include "sql/field.h"
#include "sql/log.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"

namespace im {

/* PSI key for the AI cache rwlock */
static PSI_rwlock_key key_rwlock_ai_cache;

#ifdef HAVE_PSI_INTERFACE
static PSI_rwlock_info ai_cache_rwlocks[] = {
    {&key_rwlock_ai_cache, "RWLOCK_ai_cache", 0, 0, PSI_DOCUMENT_ME}};

static void init_ai_cache_psi_keys() {
  mysql_rwlock_register("sql", ai_cache_rwlocks,
                        static_cast<int>(array_elements(ai_cache_rwlocks)));
}
#endif

/* Singleton pointer */
Ai_model_cache *Ai_model_cache::m_instance = nullptr;

/**
  Global DASHSCOPE_API_KEY read from the environment variable at startup.
  Used as a runtime fallback when a model's api_key column is empty.
  Never persisted to the system table, never exposed to users.
*/
static std::string g_dashscope_api_key;

const std::string &get_global_api_key() { return g_dashscope_api_key; }

std::string resolve_global_api_key() {
  return g_dashscope_api_key;
}

std::string get_no_api_key_hint() {
  return "Set DASHSCOPE_API_KEY environment variable "
         "or specify api_key when registering the model.";
}

/* ---- Ai_model_cache implementation ---- */

Ai_model_cache::Ai_model_cache() {
  mysql_rwlock_init(key_rwlock_ai_cache, &m_rwlock);
}

Ai_model_cache::~Ai_model_cache() {
  mysql_rwlock_destroy(&m_rwlock);
}

void Ai_model_cache::refresh(
    const std::vector<Ai_cached_model> &models,
    const std::vector<Ai_cached_function_default_model> &function_defaults) {
  mysql_rwlock_wrlock(&m_rwlock);
  m_models = models;
  m_function_defaults = function_defaults;
  mysql_rwlock_unlock(&m_rwlock);
}

bool Ai_model_cache::find_by_name(const char *name,
                                   Ai_cached_model *out) const {
  mysql_rwlock_rdlock(&m_rwlock);
  for (const auto &m : m_models) {
    if (strcasecmp(m.model_name.c_str(), name) == 0) {
      *out = m;
      mysql_rwlock_unlock(&m_rwlock);
      return true;
    }
  }
  mysql_rwlock_unlock(&m_rwlock);
  return false;
}

Ai_default_model_lookup Ai_model_cache::find_default_model(
    const char *function_name, Ai_cached_model *out,
    std::string *model_name) const {
  mysql_rwlock_rdlock(&m_rwlock);
  for (const auto &d : m_function_defaults) {
    if (strcasecmp(d.function_name.c_str(), function_name) == 0) {
      if (model_name) *model_name = d.model_name;
      if (d.model_name.empty()) {
        mysql_rwlock_unlock(&m_rwlock);
        return AI_DEFAULT_MODEL_NOT_CONFIGURED;
      }
      for (const auto &m : m_models) {
        if (strcasecmp(m.model_name.c_str(), d.model_name.c_str()) == 0) {
          *out = m;
          mysql_rwlock_unlock(&m_rwlock);
          return AI_DEFAULT_MODEL_FOUND;
        }
      }
      mysql_rwlock_unlock(&m_rwlock);
      return AI_DEFAULT_MODEL_MISSING;
    }
  }
  mysql_rwlock_unlock(&m_rwlock);
  return AI_DEFAULT_MODEL_NOT_CONFIGURED;
}

/* ---- lifecycle ---- */

void ai_cache_init() {
  DBUG_ENTER("ai_cache_init");
#ifdef HAVE_PSI_INTERFACE
  init_ai_cache_psi_keys();
#endif
  assert(Ai_model_cache::m_instance == nullptr);
  Ai_model_cache::m_instance = new Ai_model_cache();
  register_ai_model_reload_entry();
  DBUG_VOID_RETURN;
}

void ai_cache_destroy() {
  DBUG_ENTER("ai_cache_destroy");
  remove_ai_model_reload_entry();
  if (Ai_model_cache::m_instance) {
    delete Ai_model_cache::m_instance;
    Ai_model_cache::m_instance = nullptr;
  }
  DBUG_VOID_RETURN;
}

/**
  Helper: read all rows from ai_model_config table via the existing
  read_all_ai_models() function, convert Ai_record* -> Ai_cached_model,
  and refresh the singleton cache.
*/
void reload_ai_model_cache(THD *thd) {
  DBUG_ENTER("reload_ai_model_cache");

  if (!Ai_model_cache::instance()) DBUG_VOID_RETURN;

  bool error = false;
  std::vector<Ai_record *> records;
  error = read_all_ai_models(thd, &records);
  std::vector<Ai_cached_model> cache_entries;
  std::vector<Ai_function_default_record *> default_records;
  bool default_error = read_all_ai_function_default_models(thd, &default_records);
  std::vector<Ai_cached_function_default_model> default_entries;

  if (!error) {
    for (auto *rec : records) {
      Ai_cached_model cm;
      if (rec->model_name) cm.model_name = rec->model_name;
      if (rec->provider) cm.provider = rec->provider;
      if (rec->endpoint) cm.endpoint = rec->endpoint;
      if (rec->model) cm.model = rec->model;
      if (rec->api_key) cm.api_key = rec->api_key;
      if (rec->model_params) cm.model_params = rec->model_params;
      if (rec->status) cm.status = rec->status;
      if (rec->description) cm.description = rec->description;
      cache_entries.push_back(std::move(cm));
    }
  }

  if (!default_error) {
    for (auto *rec : default_records) {
      Ai_cached_function_default_model cd;
      if (rec->function_name) cd.function_name = rec->function_name;
      if (rec->model_name) cd.model_name = rec->model_name;
      default_entries.push_back(std::move(cd));
    }
  }

  Ai_model_cache::instance()->refresh(cache_entries, default_entries);
  DBUG_VOID_RETURN;
}

/**
  Called once during mysqld boot (after DD / system tables are available).
  Creates a temporary THD to:
    1. Read the DASHSCOPE_API_KEY environment variable into the global
       fallback variable (every startup).
    2. Read existing model/function config rows into the in-memory cache.
*/
void ai_models_init(bool bootstrap) {
  DBUG_ENTER("ai_models_init");

  /* Always capture the env-var API key into the global variable. */
  const char *env_key = getenv("DASHSCOPE_API_KEY");
  if (env_key && env_key[0] != '\0') {
    g_dashscope_api_key = env_key;
  }

  if (bootstrap) DBUG_VOID_RETURN;

  THD *orig_thd = current_thd;

  THD *thd = new THD;
  thd->thread_stack = pointer_cast<char *>(&thd);
  thd->store_globals();
  lex_start(thd);

  reload_ai_model_cache(thd);

  lex_end(thd->lex);
  delete thd;
  if (orig_thd) orig_thd->store_globals();
  DBUG_VOID_RETURN;
}

} /* namespace im */
