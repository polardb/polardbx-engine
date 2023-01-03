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

#ifndef PLUGIN_GALAXY_SERVICE_SERVICE_REGISTRY_H
#define PLUGIN_GALAXY_SERVICE_SERVICE_REGISTRY_H

#include "mysql/service_plugin_registry.h"

namespace gs {
namespace srv {

class Service_registry {
 public:
  Service_registry() { m_registry = mysql_plugin_registry_acquire(); }

  virtual ~Service_registry() { mysql_plugin_registry_release(m_registry); }

  my_h_service acquire(const char *service_name) {
    my_h_service result;

    if (!m_registry->acquire(service_name, &result)) return result;

    return nullptr;
  }

  bool release(my_h_service service) { return m_registry->release(service); }

 private:
  SERVICE_TYPE(registry) * m_registry;
};

}  // namespace srv
}  // namespace gs

#endif
