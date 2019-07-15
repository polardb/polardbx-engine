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

#include "sql/ccl/ccl_table_common.h"
#include "errmsg.h"
#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "mysys_err.h"

namespace im {

/**
  Classify the records into three kinds of rules.

  @param[in]    type      Rule type
  @param[in]    records   record set

  @retval       affected rows
*/
size_t Ccl_record_group::classify_rule(Ccl_rule_type type,
                                       Conf_records *records) {
  size_t num = 0;
  DBUG_ENTER("Ccl_record_group::classify_rule");
  Ccl_record *t_record;
  for (Conf_records::const_iterator it = records->cbegin();
       it != records->cend(); it++) {
    t_record = dynamic_cast<Ccl_record *>(*it);
    if (t_record->type == type) {
      if (!(t_record->blank_schema()) && !(t_record->blank_table())) {
        num++;
        intact_rules.push_back(t_record);
      } else if (t_record->blank_schema() && t_record->blank_table() &&
                 !t_record->blank_keywords()) {
        num++;
        keyword_rules.push_back(t_record);
      } else if (t_record->blank_schema() && t_record->blank_table() &&
                 t_record->blank_keywords()) {
        num++;
        command_rules.push_back(t_record);
      }
    }
  }
  m_type = type;
  DBUG_RETURN(num);
}

/**
  Log the error message.
*/
void ccl_log_error(Conf_error err) { log_conf_error(ER_CCL, err); }

Ccl_rule_type to_ccl_type(const char *str) {
  for (size_t i = 0; i < static_cast<size_t>(Ccl_rule_type::RULE_UNKNOWN);
       i++) {
    if (my_strcasecmp(system_charset_info, ccl_rule_type_str[i], str) == 0) {
      return static_cast<Ccl_rule_type>(i);
    }
  }
  return Ccl_rule_type::RULE_UNKNOWN;
}
} /* namespace im */
