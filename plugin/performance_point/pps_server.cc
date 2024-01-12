/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.
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

/**
  @file

  Performance point plugin init and destroy
*/

#include "plugin/performance_point/pps_server.h"
#include "plugin/performance_point/pps_table_iostat.h"

#include "mysql/psi/mysql_memory.h"
#include "sql/psi_memory_key.h"  // PSI_memory_key
#include "template_utils.h"
/**
  @addtogroup Performance Point plugin

  Implementation of Performance Point plugin.

  @{
*/

PSI_memory_key key_memory_PPS;

bool pps_server_inited = false;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info PPS_all_memory[] = {
    {&key_memory_PPS, "Performance_point_system", PSI_FLAG_THREAD, 0,
     PSI_DOCUMENT_ME}};

static void init_pps_ppi_key() {
  const char *category = "pps";
  int count = static_cast<int>(array_elements(PPS_all_memory));
  mysql_memory_register(category, PPS_all_memory, count);
}
#endif

void pre_init_performance_point() {
  pps_server_inited = false;

#ifdef HAVE_PSI_INTERFACE
  /* Register the PFS memory monitor */
  init_pps_ppi_key();
#endif
  register_table_iostat();

  pps_server_inited = true;
}

void destroy_performance_point() {
  if (pps_server_inited) {
    unregister_table_iostat();
  }
}

/// @} (end of group Performance Point plugin)
