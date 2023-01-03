/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#include "my_sys.h"                     // MY_WME
#include "mysql/service_mysql_alloc.h"  // my_malloc

/**
  Memory detector for PPS by PFS
*/
extern PSI_memory_key key_memory_PPS;

/* Allocate the object */
template <typename T>
T *allocate_object() {
  void *ptr = nullptr;
  T *obj = nullptr;

  ptr = my_malloc(key_memory_PPS, sizeof(T), MYF(MY_WME | ME_FATALERROR));

  if (ptr) obj = new (ptr) T();
  return obj;
}

/* Dellocate the object */
template <typename T>
void destroy_object(T *obj) {
  if (obj) {
    obj->~T();
    my_free(obj);
  }
}

/* PPS all statistic object deleter */
template <typename T>
class PPS_element_deleter {
 public:
  void operator()(T *t) const { destroy_object<T>(t); }
};
