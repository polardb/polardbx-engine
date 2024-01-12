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

#ifndef PLUGIN_GALAXY_SERVICE_SERVICE_UDF_H
#define PLUGIN_GALAXY_SERVICE_SERVICE_UDF_H

#include "mysql/components/services/udf_registration.h"
#include "service_registry.h"

namespace gs {
namespace srv {

/**
  Registry service for udf function
*/
class Service_udf_registry {
 public:
  Service_udf_registry(Service_registry *registry) : m_registry(registry) {
    m_udf_registration =
        reinterpret_cast<SERVICE_TYPE_NO_CONST(udf_registration) *>(
            m_registry->acquire("udf_registration"));
  }

  ~Service_udf_registry() {
    m_registry->release(reinterpret_cast<my_h_service>(m_udf_registration));
  }

  bool udf_register(const char *func_name, enum Item_result return_type,
                    Udf_func_any func, Udf_func_init init_func,
                    Udf_func_deinit deinit_func);

  bool udf_unregister(const char *name, int *was_present);

 private:
  Service_registry *m_registry;

  SERVICE_TYPE_NO_CONST(udf_registration) * m_udf_registration;
};

/**
  Registry service for udf aggregate function
*/
class Service_udf_registry_aggregate {
 public:
  Service_udf_registry_aggregate(Service_registry *registry)
      : m_registry(registry) {
    m_udf_registration_aggregate =
        reinterpret_cast<SERVICE_TYPE_NO_CONST(udf_registration_aggregate) *>(
            m_registry->acquire("udf_registration_aggregate"));
  }

  ~Service_udf_registry_aggregate() {
    m_registry->release(
        reinterpret_cast<my_h_service>(m_udf_registration_aggregate));
  }

  bool udf_register(const char *func_name, enum Item_result return_type,
                    Udf_func_any func, Udf_func_init init_func,
                    Udf_func_deinit deinit_func, Udf_func_add add_func,
                    Udf_func_clear clear_func);

  bool udf_unregister(const char *name, int *was_present);

 private:
  Service_registry *m_registry;

  SERVICE_TYPE_NO_CONST(udf_registration_aggregate) *
      m_udf_registration_aggregate;
};

}  // namespace srv
}  // namespace gs

#endif
