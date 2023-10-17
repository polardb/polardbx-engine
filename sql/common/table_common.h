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

#ifndef SQL_COMMON_TABLE_COMMON_INCLUDED
#define SQL_COMMON_TABLE_COMMON_INCLUDED

#include <vector>
#include "m_ctype.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sqlcommand.h"
#include "prealloced_array.h"

namespace im {

/* Conf table writer operation type */
enum class Conf_table_op { OP_INSERT, OP_DELETE, OP_UPDATE };
/**
  Conf table operation error type;
*/
enum class Conf_error { CONF_OK, CONF_ER_TABLE_OP_ERROR, CONF_ER_RECORD };

enum class Conf_error_level { CONF_WARNING, CONF_CRITICAL };

/**
  Reconstruct error by handler error.

  @param[in]    nr          Redefine error code
  @param[in]    ha_error    error number from SE.
*/
extern void ha_error(int nr, int ha_error);

/**
  Log the error string by conf error type.
*/
extern void log_conf_error(int nr, Conf_error error);

inline bool blank(const char *str) {
  if (str == NULL || str[0] == '\0') return true;
  return false;
}
inline bool blank(const LEX_CSTRING &s) {
  if (s.length > 0) return false;
  return true;
}

/**
  Conf table record object

  Should implement check_active and check_valid function.
*/
class Conf_record {
 public:
  Conf_record() {}
  virtual ~Conf_record() {}

  virtual ulonglong get_id() const = 0;

  virtual void set_id(ulonglong value) = 0;

  virtual bool check_valid(const char **msg) const = 0;

  virtual bool check_active() const = 0;
};

#define PREALLOC_RECORD_COUNT 20

using Conf_records = Prealloced_array<Conf_record *, PREALLOC_RECORD_COUNT>;

} /* namespace im */

#endif
