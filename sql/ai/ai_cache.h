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

#ifndef SQL_AI_AI_CACHE_INCLUDED
#define SQL_AI_AI_CACHE_INCLUDED

/**
  @file sql/ai/ai_cache.h
  In-memory cache for mysql.ai_model_config and
  mysql.ai_function_config.
  Follows the same pattern as CCL / Outline caches:
    startup  -> read table -> fill cache
    CALL     -> modify table -> reload cache
    SELECT   -> read from cache only (no table I/O)
*/

#include <string>
#include <strings.h>
#include <vector>

#include "mysql/psi/mysql_rwlock.h"

class THD;

namespace im {

/**
  A self-contained copy of one ai_model_config row.
  Owns all its string data so it is independent of any THD mem_root.
*/
struct Ai_cached_model {
  std::string model_name;
  std::string provider;
  std::string endpoint;
  std::string model;
  std::string api_key;
  std::string model_params;
  std::string status;
  std::string description;

  Ai_cached_model() {}

  bool check_active() const {
    return status.empty() || strcasecmp(status.c_str(), "ACTIVE") == 0;
  }
};

struct Ai_cached_function_default_model {
  std::string function_name;
  std::string model_name;
};

enum Ai_default_model_lookup {
  AI_DEFAULT_MODEL_FOUND = 0,
  AI_DEFAULT_MODEL_NOT_CONFIGURED,
  AI_DEFAULT_MODEL_MISSING
};

/**
  Singleton cache protected by a read-write lock.
*/
class Ai_model_cache {
 public:
  static Ai_model_cache *instance() { return m_instance; }

  /** Refresh the cache: clear + fill from the supplied lists. */
  void refresh(
      const std::vector<Ai_cached_model> &models,
      const std::vector<Ai_cached_function_default_model> &function_defaults);

  /** Lookup by config name.  Returns true if found. */
  bool find_by_name(const char *name, Ai_cached_model *out) const;

  /** Lookup the default model configured for one AI function. */
  Ai_default_model_lookup find_default_model(const char *function_name,
                                             Ai_cached_model *out,
                                             std::string *model_name) const;

 private:
  Ai_model_cache();
  ~Ai_model_cache();

  mutable mysql_rwlock_t m_rwlock;
  std::vector<Ai_cached_model> m_models;
  std::vector<Ai_cached_function_default_model> m_function_defaults;

  static Ai_model_cache *m_instance;

  friend void ai_cache_init();
  friend void ai_cache_destroy();
};

/* ---- lifecycle (called from mysqld.cc) ---- */
void ai_cache_init();
void ai_cache_destroy();

/** Read table -> refresh cache.  Needs a valid THD (may create its own). */
void ai_models_init(bool bootstrap);

/** Reload cache from table.  Called after CALL dbms_ai.register/update/delete. */
void reload_ai_model_cache(THD *thd);

/**
  Get the global DASHSCOPE_API_KEY read from the environment variable at
  startup.  This key is used as a fallback when a model's api_key field
  in the system table is empty.  It is NEVER exposed to users (not shown
  in describe_model, list_models, or any result set).
*/
const std::string &get_global_api_key();

/**
  Resolve the effective global API key fallback.
  Returns the DASHSCOPE_API_KEY captured at startup.
  Returned by value so the result is a stable snapshot.
*/
std::string resolve_global_api_key();

/**
  Build a user-facing hint string describing how to configure a missing
  AI api_key.  Used as a suffix for "no api_key configured" error messages.
*/
std::string get_no_api_key_hint();

} /* namespace im */

#endif /* SQL_AI_AI_CACHE_INCLUDED */
