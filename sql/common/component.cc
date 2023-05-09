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

#include "sql/common/component.h"

namespace im {

/* String compare ingoring case */
template <typename F, typename S>
bool Pair_key_comparator<F, S>::operator()(
    const Pair_key_type<F, S> &lhs, const Pair_key_type<F, S> &rhs) const {
  return (my_strcasecmp(system_charset_info, lhs.first.c_str(),
                        rhs.first.c_str()) == 0 &&
          my_strcasecmp(system_charset_info, lhs.second.c_str(),
                        rhs.second.c_str()) == 0);
}

template bool Pair_key_comparator<std::string, std::string>::operator()(
    const Pair_key_type<std::string, std::string> &lhs,
    const Pair_key_type<std::string, std::string> &rhs) const;

} /* namespace im */


namespace std {
template<typename F, typename S>
size_t hash<im::Pair_key_type<F, S>>::operator()(
    const im::Pair_key_type<F, S> &p) const {
  return hash<F>()(static_cast<const F>(p.first)) ^
         hash<S>()(static_cast<const S>(p.second));
}

template size_t hash<im::Pair_key_type<std::string, std::string>>::operator()(
    const im::Pair_key_type<std::string, std::string> &p) const;

} /* namespace std */
