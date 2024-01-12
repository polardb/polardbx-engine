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

#ifndef SQL_CCL_CCL_COMMON_INCLUDED
#define SQL_CCL_CCL_COMMON_INCLUDED

#include "mysql/components/services/bits/mysql_cond_bits.h"
#include "mysql/components/services/bits/mysql_mutex_bits.h"
#include "mysql/components/services/bits/mysql_rwlock_bits.h"
#include "mysql/components/services/bits/psi_cond_bits.h"
#include "mysql/components/services/bits/psi_memory_bits.h"
#include "mysql/components/services/bits/psi_mutex_bits.h"
#include "mysql/components/services/bits/psi_rwlock_bits.h"

#include "prealloced_array.h"
#include "sql/common/component.h"
#include "sql/protocol_classic.h"

namespace im {

/**
  Common definition of conncurency control system.
*/
extern PSI_memory_key key_memory_ccl;

extern PSI_mutex_key key_LOCK_ccl_slot;

extern PSI_cond_key key_COND_ccl_slot;

extern PSI_rwlock_key key_rwlock_rule_container;

/* Ccl object allocator */
template <typename T, typename... Args>
T *allocate_ccl_object(Args &&...args) {
  return allocate_object<T, Args...>(key_memory_ccl,
                                     std::forward<Args>(args)...);
}

/* CCL rule array type */
template <typename T, size_t Prealloc>
using Ccl_rule_list_type = Prealloced_array<T, Prealloc>;

/* CCL string type */
struct Ccl_string_alloc {
  void *operator()(size_t s) const;
};

using String_ccl = String_template<Ccl_string_alloc>;

/* CCL rule map type */
template <typename T>
using Ccl_rule_map_type =
    Pair_key_icase_unordered_map<String_ccl, String_ccl, T>;

/* Key words list */
template <size_t Prealloc>
using Keyword_array_type = Prealloced_array<String_ccl, Prealloc>;

} /* namespace im */

namespace std {

template <>
struct hash<im::String_ccl> {
  typedef im::String_ccl argument_type;
  typedef size_t result_type;

  size_t operator()(const im::String_ccl &s) const;
};

} /* namespace std */

#endif
