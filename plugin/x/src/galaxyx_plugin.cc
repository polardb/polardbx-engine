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


#include "plugin/x/generated/galaxyx_version.h"
#include "plugin/x/src/variables/galaxy_status.h"
#include "plugin/x/src/variables/galaxy_variables.h"

#include "mysql/plugin.h"

namespace gx {
static struct st_mysql_daemon galaxyx_deamon_plugin_descriptor = {
    MYSQL_DAEMON_INTERFACE_VERSION};

struct st_mysql_plugin galaxyx_plugin = {
    MYSQL_DAEMON_PLUGIN,                             /* plugin type */
    &galaxyx_deamon_plugin_descriptor,               /* descriptor */
    GALAXYX_PLUGIN_NAME,                             /* plugin name */
    "Alibaba Corporation",                           /* author */
    "Galaxy X-protocol",                             /* description */
    PLUGIN_LICENSE_GPL,                              /* license */
    nullptr,                                         /* init */
    nullptr,                                         /* uninstall */
    nullptr,                                         /* deinit */
    0x0100,                                          /* version */
    gx::Galaxy_status_variables::m_status_variables, /* status variables */
    gx::Galaxy_system_variables::m_system_variables, /* system variables */
    nullptr,                                         /* reserved */
    0UL                                              /* flags */
};

}  // namespace gx
