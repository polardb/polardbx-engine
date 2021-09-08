/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxySQL hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "plugin/x/src/variables/galaxy_variables.h"
#include "plugin/x/src/variables/galaxy_variables_defaults.h"

#include <cstdlib>
#include <limits>
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/plugin.h"

/** Galaxy X-protocol */
namespace gx {

unsigned int Galaxy_system_variables::m_port = gx::defaults::G_PORT;

static MYSQL_SYSVAR_UINT(
    port, Galaxy_system_variables::m_port,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "Port on which Galaxy X-protocol is going to accept incoming connections.",
    nullptr, nullptr, gx::defaults::G_PORT, 1,
    std::numeric_limits<uint16_t>::max(), 0);

struct SYS_VAR *Galaxy_system_variables::m_system_variables[] = {
    MYSQL_SYSVAR(port), nullptr};

}  // namespace gx

