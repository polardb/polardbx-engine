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

#include "plugin/x/src/variables/galaxy_variables.h"
#include "plugin/x/src/variables/galaxy_variables_defaults.h"

#include <cstdlib>
#include <limits>
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/plugin.h"

/** Galaxy X-protocol */
namespace gx {

bool Galaxy_system_variables::m_mtr = false;
int Galaxy_system_variables::m_port = gx::defaults::G_PORT;
unsigned int Galaxy_system_variables::m_max_queued_messages;
unsigned int Galaxy_system_variables::m_galaxy_worker_threads_per_tcp;
unsigned int Galaxy_system_variables::m_galaxy_worker_threads_shrink_time;
bool Galaxy_system_variables::m_enable_galaxy_session_pool_log;
bool Galaxy_system_variables::m_enable_galaxy_kill_log;
unsigned int Galaxy_system_variables::m_socket_recv_buffer;
unsigned int Galaxy_system_variables::m_socket_send_buffer;

static MYSQL_SYSVAR_BOOL(
    mtr, Galaxy_system_variables::m_mtr,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "Enable galaxy x protocol for mtr mode.",
    nullptr, &Galaxy_system_variables::update_func<bool>, false);

static MYSQL_SYSVAR_INT(
    port, Galaxy_system_variables::m_port,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "Port on which Galaxy X-protocol is going to accept incoming connections.",
    nullptr, nullptr, gx::defaults::G_PORT, -1,
    std::numeric_limits<uint16_t>::max(), 0);

static MYSQL_SYSVAR_UINT(
    max_queued_messages, Galaxy_system_variables::m_max_queued_messages,
    PLUGIN_VAR_OPCMDARG,
    "Number of max queued messages that session is going to handle.", nullptr,
    &Galaxy_system_variables::update_func<unsigned int>, 128, 8, 2048, 0);

static MYSQL_SYSVAR_UINT(
    galaxy_worker_threads_per_tcp,
    Galaxy_system_variables::m_galaxy_worker_threads_per_tcp,
    PLUGIN_VAR_OPCMDARG,
    "Number of worker threads for single galaxy parallel TCP connection.",
    nullptr, &Galaxy_system_variables::update_func<unsigned int>, 2, 1, 32, 0);

static MYSQL_SYSVAR_UINT(
    galaxy_worker_threads_shrink_time,
    Galaxy_system_variables::m_galaxy_worker_threads_shrink_time,
    PLUGIN_VAR_OPCMDARG,
    "Time(ms) of shrinking the thread pool when multiple worker threads(over "
    "2/3) are not in state of waiting.",
    nullptr, &Galaxy_system_variables::update_func<unsigned int>, 5000, 1000,
    600000, 0);

static MYSQL_SYSVAR_BOOL(
    enable_galaxy_session_pool_log,
    Galaxy_system_variables::m_enable_galaxy_session_pool_log,
    PLUGIN_VAR_OPCMDARG,
    "Enable galaxy session pool and thread pool log(default warning level).",
    nullptr, &Galaxy_system_variables::update_func<bool>, false);

static MYSQL_SYSVAR_BOOL(
    enable_galaxy_kill_log,
    Galaxy_system_variables::m_enable_galaxy_kill_log,
    PLUGIN_VAR_OPCMDARG,
    "Enable galaxy session kill/cancel log(default warning level).",
    nullptr, &Galaxy_system_variables::update_func<bool>, true);

static MYSQL_SYSVAR_UINT(
    socket_recv_buffer, Galaxy_system_variables::m_socket_recv_buffer,
    PLUGIN_VAR_OPCMDARG,
    "Receive buffer size for galaxy parallel TCP connection.", nullptr,
    &Galaxy_system_variables::update_func<unsigned int>, 16 * 1024, 0,
    1024 * 1024, 0);

static MYSQL_SYSVAR_UINT(socket_send_buffer,
                         Galaxy_system_variables::m_socket_send_buffer,
                         PLUGIN_VAR_OPCMDARG,
                         "Send buffer size for galaxy parallel TCP connection.",
                         nullptr,
                         &Galaxy_system_variables::update_func<unsigned int>,
                         256 * 1024, 0, 1024 * 1024, 0);

struct SYS_VAR *Galaxy_system_variables::m_system_variables[] = {
    MYSQL_SYSVAR(mtr),
    MYSQL_SYSVAR(port),
    MYSQL_SYSVAR(max_queued_messages),
    MYSQL_SYSVAR(galaxy_worker_threads_per_tcp),
    MYSQL_SYSVAR(galaxy_worker_threads_shrink_time),
    MYSQL_SYSVAR(enable_galaxy_session_pool_log),
    MYSQL_SYSVAR(enable_galaxy_kill_log),
    MYSQL_SYSVAR(socket_recv_buffer),
    MYSQL_SYSVAR(socket_send_buffer),
    nullptr};

}  // namespace gx
