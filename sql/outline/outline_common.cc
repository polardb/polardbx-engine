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

#include "sql/outline/outline_common.h"
#include "my_inttypes.h"
#include "my_murmur3.h"                 // my_murmur3_32
#include "my_sys.h"                     // MY_WME, MY_FATALERROR
#include "mysql/service_mysql_alloc.h"  // my_malloc

namespace im {

void *Outline_alloc::operator()(size_t s) const {
  return my_malloc(key_memory_outline, s, MYF(MY_WME | ME_FATALERROR));
}

/* String compare ingoring case */
template <typename F, typename S>
bool Pair_key_comparator<F, S>::operator()(
    const Pair_key_type<F, S> &lhs, const Pair_key_type<F, S> &rhs) const {
  return (my_strcasecmp(system_charset_info, lhs.first.c_str(),
                        rhs.first.c_str()) == 0 &&
          my_strcasecmp(system_charset_info, lhs.second.c_str(),
                        rhs.second.c_str()) == 0);
}

template bool Pair_key_comparator<String_outline, String_outline>::operator()(
    const Pair_key_type<String_outline, String_outline> &lhs,
    const Pair_key_type<String_outline, String_outline> &rhs) const;

} /* namespace im */

namespace std {

size_t hash<im::String_outline>::operator()(const im::String_outline &s) const {
  return murmur3_32(reinterpret_cast<const uchar *>(s.c_str()), s.size(), 0);
}

template <typename F, typename S>
size_t hash<im::Pair_key_type<F, S>>::operator()(
    const im::Pair_key_type<F, S> &p) const {
  return hash<F>()(static_cast<const F>(p.first)) ^
         hash<S>()(static_cast<const S>(p.second));
}

template size_t
hash<im::Pair_key_type<im::String_outline, im::String_outline>>::operator()(
    const im::Pair_key_type<im::String_outline, im::String_outline> &p) const;
}  // namespace std
