/* Copyright (c) 2016, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef PLUGIN_KEYRING_RDS_KEYRING_RDS_COMMON_H_INCLUDED
#define PLUGIN_KEYRING_RDS_KEYRING_RDS_COMMON_H_INCLUDED

#include "mysql/psi/mysql_rwlock.h"

namespace keyring_rds {

class Lock_helper {
 public:
  explicit Lock_helper(mysql_rwlock_t *lock, bool exclusive) : m_lock(lock) {
    if (exclusive)
      mysql_rwlock_wrlock(m_lock);
    else
      mysql_rwlock_rdlock(m_lock);
  }

  ~Lock_helper() { mysql_rwlock_unlock(m_lock); }

 private:
  mysql_rwlock_t *m_lock;
};

}  // namespace keyring_rds

#endif
