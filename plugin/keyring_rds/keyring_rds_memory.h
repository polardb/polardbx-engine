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

#ifndef PLUGIN_KEYRING_RDS_KEYRING_RDS_MEMORY_H_INCLUDED
#define PLUGIN_KEYRING_RDS_KEYRING_RDS_MEMORY_H_INCLUDED

#include <string>
#include <vector>

#include "my_murmur3.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"
#include "sql/malloc_allocator.h"
#include "sql/stateless_allocator.h"

#include "keyring_rds.h"

namespace keyring_rds {

template <class T>
T keyring_rds_malloc(size_t size, int flags = MYF(MY_WME | ME_FATALERROR)) {
  void *allocated_memory = my_malloc(key_memory_KEYRING_rds, size, flags);
  return allocated_memory ? reinterpret_cast<T>(allocated_memory) : NULL;
}

/**
  Functor class which allocates memory for Key_string,
  Implementation uses my_malloc with key_memory_KEYRING_rds.
*/
struct Keyring_str_alloc {
  void *operator()(size_t s) const { return keyring_rds_malloc<void *>(s); }
};

typedef Stateless_allocator<char, Keyring_str_alloc> Keyring_str_allocator;

/**
 Template alias for char-based std::basic_string.
*/
template <class A>
using Char_string_templ = std::basic_string<char, std::char_traits<char>, A>;

typedef Char_string_templ<Keyring_str_allocator> Key_string;

/**
  Base class for operator new/delete
*/
class Keyring_obj_alloc {
 public:
  void *operator new(size_t s) noexcept { return keyring_rds_malloc<void*>(s); }
  void operator delete(void *ptr, size_t) { my_free(ptr); }
};

/**
  std::vector, but with my_malloc
*/
template <typename T>
class Keyring_vector : public std::vector<T, Malloc_allocator<T>> {
 public:
  explicit Keyring_vector()
      : std::vector<T, Malloc_allocator<T>>(
            Malloc_allocator<>(key_memory_KEYRING_rds)) {}
};

}  // namespace keyring_rds

namespace std {

/**
 Specialize std::hash for dd::String_type so that it can be
 used with unordered_ containers. Uses our standard murmur3_32
 implementation, and the same suitability restrictions apply.
*/
template <>
struct hash<keyring_rds::Key_string> {
  typedef keyring_rds::Key_string argument_type;
  typedef size_t result_type;

  size_t operator()(const keyring_rds::Key_string &s) const {
    return murmur3_32(reinterpret_cast<const uchar *>(s.c_str()), s.size(), 0);
  }
};

}  // namespace std

template keyring_rds::Key_string::~basic_string();

#endif
