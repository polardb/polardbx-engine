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

#ifndef SQL_AI_AI_RELOAD_INCLUDED
#define SQL_AI_AI_RELOAD_INCLUDED

#include "sql/common/reload.h"

class THD;

namespace im {

/**
  Reload handler for AI model system tables.
  When a slave/follower applies DML on ai_model_config or
  ai_function_config, this handler is invoked at transaction
  commit to refresh the in-memory AI model cache.
*/
class Ai_model_reload : public Reload {
 public:
  Ai_model_reload() {}
  ~Ai_model_reload() override {}

  void execute(THD *thd) override;
  Reload_type type() const override;
};

extern void register_ai_model_reload_entry();
extern void remove_ai_model_reload_entry();

} /* namespace im */

#endif /* SQL_AI_AI_RELOAD_INCLUDED */
