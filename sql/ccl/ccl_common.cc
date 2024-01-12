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

#include "sql/ccl/ccl_common.h"
#include <boost/algorithm/string/case_conv.hpp>
#include "my_inttypes.h"
#include "my_murmur3.h"                 // my_murmur3_32
#include "my_sys.h"                     // MY_WME, MY_FATALERROR
#include "mysql/service_mysql_alloc.h"  // my_malloc

namespace im {

void *Ccl_string_alloc::operator()(size_t s) const {
  return my_malloc(key_memory_ccl, s, MYF(MY_WME | ME_FATALERROR));
}

/* String compare ingoring case */
template <typename F, typename S>
bool Pair_key_icase_comparator<F, S>::operator()(
    const Pair_key_type<F, S> &lhs, const Pair_key_type<F, S> &rhs) const {
  F l_f = lhs.first;
  S l_s = lhs.second;
  F r_f = rhs.first;
  S r_s = rhs.second;
  boost::algorithm::to_upper(l_f);
  boost::algorithm::to_upper(l_s);
  boost::algorithm::to_upper(r_f);
  boost::algorithm::to_upper(r_s);
  return (l_f.compare(r_f) == 0 && l_s.compare(r_s) == 0);
}

template bool Pair_key_icase_comparator<String_ccl, String_ccl>::operator()(
    const Pair_key_type<String_ccl, String_ccl> &lhs,
    const Pair_key_type<String_ccl, String_ccl> &rhs) const;

} /* namespace im */

namespace std {

size_t hash<im::String_ccl>::operator()(const im::String_ccl &s) const {
  return murmur3_32(reinterpret_cast<const uchar *>(s.c_str()), s.size(), 0);
}

} /* namespace std */

namespace im {

template <typename F, typename S>
size_t Pair_key_icase_hash<F, S>::operator()(
    const im::Pair_key_type<F, S> &p) const {
  F s1 = p.first;
  S s2 = p.second;
  boost::algorithm::to_upper(s1);
  boost::algorithm::to_upper(s2);
  return std::hash<F>()(static_cast<const F>(s1)) ^
         std::hash<S>()(static_cast<const S>(s2));
}

template size_t Pair_key_icase_hash<String_ccl, String_ccl>::operator()(
    const im::Pair_key_type<String_ccl, String_ccl> &p) const;

} /* namespace im */
