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

#ifndef SQL_RECYCLE_BIN_RECYCLE_AUTH_INCLUDED
#define SQL_RECYCLE_BIN_RECYCLE_AUTH_INCLUDED

#include "sql/auth/auth_common.h"

namespace im {
namespace recycle_bin {

/**
  Recycle schema access control:

  Only allow read, grant privileges for normal user.
*/
class Recycle_internal_schema_access : public ACL_internal_schema_access {
 public:
  Recycle_internal_schema_access() {}

  ~Recycle_internal_schema_access() override {}

  virtual ACL_internal_access_result check(ulong want_access, ulong *save_priv,
                                           bool) const override;

  const ACL_internal_table_access *lookup(const char *name) const override;

  static Recycle_internal_schema_access *instance();
};

} /* namespace recycle_bin */
} /* namespace im */

#endif
