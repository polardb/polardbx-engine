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

#ifndef SQL_COMMON_COMPONENT_INCLUDED
#define SQL_COMMON_COMPONENT_INCLUDED

#include <string>

#include "map_helpers.h"
#include "mysql/components/services/bits/psi_memory_bits.h"
#include "mysql/psi/psi_memory.h"

namespace im  {

/**
  PSI memory detect interface;
*/
class PSI_memory_base {
 public:
  PSI_memory_base(PSI_memory_key key) : m_key(key) {}

  virtual ~PSI_memory_base() {}

  /* Getting */
  PSI_memory_key psi_key() { return m_key; }

  /* Setting */
  void set_psi_key(PSI_memory_key key) { m_key = key; }

 private:
  PSI_memory_key m_key;
};

/*
  Pair key map definition
*/
template <typename F, typename S>
using Pair_key_type = std::pair<F, S>;

template <typename F, typename S>
class Pair_key_comparator {
 public:
  bool operator()(const Pair_key_type<F, S> &lhs,
                  const Pair_key_type<F, S> &rhs) const;
};

template <typename F, typename S, typename T>
class Pair_key_unordered_map
    : public malloc_unordered_map<Pair_key_type<F, S>, const T *,
                                  std::hash<Pair_key_type<F, S>>,
                                  Pair_key_comparator<F, S>> {
 public:
  explicit Pair_key_unordered_map(PSI_memory_key key)
      : malloc_unordered_map<Pair_key_type<F, S>, const T *,
                             std::hash<Pair_key_type<F, S>>,
                             Pair_key_comparator<F, S>>(key) {}
};

} /* namespace im */

namespace std {
template <typename F, typename S>
struct hash<im::Pair_key_type<F, S>> {
 public:
  typedef typename im::Pair_key_type<F, S> argument_type;
  typedef size_t result_type;

  size_t operator()(const im::Pair_key_type<F, S> &p) const;
};

} /* namespace std */


#endif
