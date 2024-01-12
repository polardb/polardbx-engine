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

#include "service_udf.h"

namespace gs {
namespace srv {

bool Service_udf_registry::udf_register(const char *func_name,
                                        enum Item_result return_type,
                                        Udf_func_any func,
                                        Udf_func_init init_func,
                                        Udf_func_deinit deinit_func) {
  return m_udf_registration->udf_register(func_name, return_type, func,
                                          init_func, deinit_func);
}

bool Service_udf_registry::udf_unregister(const char *name, int *was_present) {
  return m_udf_registration->udf_unregister(name, was_present);
}

bool Service_udf_registry_aggregate::udf_register(
    const char *func_name, enum Item_result return_type, Udf_func_any func,
    Udf_func_init init_func, Udf_func_deinit deinit_func, Udf_func_add add_func,
    Udf_func_clear clear_func) {
  return m_udf_registration_aggregate->udf_register(
      func_name, return_type, func, init_func, deinit_func, add_func,
      clear_func);
}

bool Service_udf_registry_aggregate::udf_unregister(const char *name,
                                                    int *was_present) {
  return m_udf_registration_aggregate->udf_unregister(name, was_present);
}

}  // namespace srv
}  // namespace gs
