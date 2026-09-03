#pragma once
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

#include <map>
#include <string>
#include <vector>

#include "my_murmur3.h"
#include "my_sys.h"
#include "sql/malloc_allocator.h"
#include "sql/stateless_allocator.h"

namespace encdb {

extern PSI_memory_key key_memory_ENCDB_rds;

template <class T>
T encdb_rds_malloc(size_t size, int flags = MYF(MY_WME | ME_FATALERROR)) {
  void *allocated_memory = my_malloc(key_memory_ENCDB_rds, size, flags);
  return allocated_memory ? reinterpret_cast<T>(allocated_memory) : NULL;
}

template <class T>
void encdb_rds_free(T ptr) {
  my_free(ptr);
}

bool encdb_rds_memory_init(void);
void encdb_rds_memory_deinit();

}  // namespace encdb
