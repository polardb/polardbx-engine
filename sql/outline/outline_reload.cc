/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/outline/outline_reload.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/common/reload.h"
#include "sql/common/table_common.h"
#include "sql/outline/outline_table.h"
#include "sql/sql_class.h"

namespace im {

/* Outline table key def */
LEX_CSTRING OUTLINE_TABLE_KEY = {C_STRING_WITH_LEN("mysql\0outline\0")};

/* Outline cache reload operation */
void Outline_reload::execute(THD *thd) {
  Conf_error error;
  if ((error = reload_outlines(thd)) != Conf_error::CONF_OK)
    LogErr(ERROR_LEVEL, ER_OUTLINE, "reload outline failed");
}

/* Outline type */
Reload_type Outline_reload::type() const { return RELOAD_OUTLINE; }

/**
  Register the outline reload entry when reboot outline module.
*/
void register_outline_reload_entry() {
  Outline_reload *entry = new Outline_reload();
  if (entry)
    register_reload_entry(OUTLINE_TABLE_KEY.str, OUTLINE_TABLE_KEY.length,
                          entry);
}

/**
  Remove outline reload entry when shutdown outline module.
*/
void remove_outline_reload_entry() {
  remove_reload_entry(OUTLINE_TABLE_KEY.str, OUTLINE_TABLE_KEY.length);
}

} /* namespace im */
