/* Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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
  @file sql/ai/ai_reload.cc
  Slave-side reload handler for mysql.ai_model_config and
  mysql.ai_function_config.

  When a follower node applies binlog events that modify the
  AI model config tables, this module refreshes the in-memory
  Ai_model_cache so that AI functions on followers see up-to-date
  model configurations without requiring a restart.
*/

#include "sql/ai/ai_reload.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/ai/ai_cache.h"
#include "sql/common/reload.h"
#include "sql/common/table_common.h"
#include "sql/sql_class.h"

namespace im {

/* AI model config table key: "mysql\0ai_model_config\0" */
static LEX_CSTRING AI_MODEL_TABLE_KEY = {
    C_STRING_WITH_LEN("mysql\0ai_model_config\0")};
static LEX_CSTRING AI_FUNCTION_DEFAULT_TABLE_KEY = {
    C_STRING_WITH_LEN("mysql\0ai_function_config\0")};

/* Reload cache from table on slave commit */
void Ai_model_reload::execute(THD *thd) {
  reload_ai_model_cache(thd);
}

/* Return reload type identifier */
Reload_type Ai_model_reload::type() const { return RELOAD_AI_MODEL; }

/**
  Register the AI model reload entry at startup.
*/
void register_ai_model_reload_entry() {
  Ai_model_reload *entry = new Ai_model_reload();
  if (entry)
    register_reload_entry(AI_MODEL_TABLE_KEY.str, AI_MODEL_TABLE_KEY.length,
                          entry);
  Ai_model_reload *default_entry = new Ai_model_reload();
  if (default_entry)
    register_reload_entry(AI_FUNCTION_DEFAULT_TABLE_KEY.str,
                          AI_FUNCTION_DEFAULT_TABLE_KEY.length,
                          default_entry);
}

/**
  Remove AI model reload entry at shutdown.
*/
void remove_ai_model_reload_entry() {
  remove_reload_entry(AI_MODEL_TABLE_KEY.str, AI_MODEL_TABLE_KEY.length);
  remove_reload_entry(AI_FUNCTION_DEFAULT_TABLE_KEY.str,
                      AI_FUNCTION_DEFAULT_TABLE_KEY.length);
}

} /* namespace im */
