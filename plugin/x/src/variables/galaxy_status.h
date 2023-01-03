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


#ifndef PLUGIN_X_SRC_VARIABLES_GALAXY_STATUS_H
#define PLUGIN_X_SRC_VARIABLES_GALAXY_STATUS_H

#include "plugin/x/src/server/galaxy_properties.h"

struct SHOW_VAR;

namespace gx {

class Galaxy_status_variables {
 public:
  static struct SHOW_VAR m_status_variables[];

  static Galaxy_properties m_properties;

  static std::string get_property(const Galaxy_property_type type);

  static bool set_property(Galaxy_property_type type, std::string value);
};

}  // namespace gx

#endif
