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

#include "plugin/galaxy/plugin.h"
#include <mysql/components/services/log_builtins.h>
#include <mysqld_error.h>
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "mysql/status_var.h"

#include "mysql/galaxy/service_galaxy.h"
#include "plugin/galaxy/udf/registry.h"
#include "plugin/galaxy/udf/udf.h"

static SERVICE_TYPE(registry) *reg_srv = nullptr;
SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

namespace gs {

/** Global udf registry */
static udf::Registry *udf_registry = nullptr;

/** Plugin initialization */
static int galaxy_init(void *) {
  int udf_cnt;
  /** 1. Register all udf */
  DBUG_ASSERT(udf_registry == nullptr);

  if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs)) return 1;

  udf_registry = new udf::Registry();

  if ((udf_cnt = udf_registry->insert(
           {udf::UDF(bloomfilter_udf).def(), udf::UDF(hllndv_udf).def(),
            udf::UDF(hyperloglog_udf).def(), udf::UDF(hashcheck_udf).def()})) != 4) {
    /** Log error */
    LogErr(ERROR_LEVEL, ER_GALAXY_PLUGIN, "Register UDF error!");

    deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
    return 1;
  }

  LogErr(INFORMATION_LEVEL, ER_GALAXY_PLUGIN, "Register four UDFs.");
  return 0;
}

static int galaxy_deinit(void *) {
  if (udf_registry != nullptr) {
    udf_registry->drop();
    delete udf_registry;
  }
  deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
  return 0;
}

static char udf_list[PATH_MAX] = "hashcheck,bloomfilter,hyperloglog,hllndv";
static char *p_udf_list = udf_list;
static MYSQL_SYSVAR_STR(function_list, p_udf_list,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_NOCMDARG,
  "list of user defined functions for PolarX",
  nullptr, nullptr, nullptr);

#define MYSQL_PLUGIN_VAR_HEADER \
  int flags;                    \
  const char *name;             \
  const char *comment;          \
  mysql_var_check_func check;   \
  mysql_var_update_func update

static SYS_VAR *galaxy_system_vars[] = {
    MYSQL_SYSVAR(function_list),
    NULL,
};

/** All counter for show global status */
static SHOW_VAR galaxy_status_vars[] = {
    {"polarx_udf_number_bloomfilter_calls",
     (char *)&udf::udf_counter.bloomfilter_counter, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"polarx_udf_number_hyperloglog_calls",
     (char *)&udf::udf_counter.hyperloglog_counter, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"polarx_udf_number_hllndv_calls", (char *)&udf::udf_counter.hllndv_counter,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"polarx_udf_number_hashcheck_calls", (char*)&udf::udf_counter.hashcheck_counter, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {NULL, NULL, SHOW_LONG, SHOW_SCOPE_GLOBAL},
};

/**
  The global service supplied by galaxy plugin
*/
static struct mysql_galaxy_service_st galaxy_descriptor = {NULL};

}  // namespace gs

mysql_declare_plugin(galaxy){
    MYSQL_GALAXY_PLUGIN,    /*   type                            */
    &gs::galaxy_descriptor, /*   descriptor                      */
    GALAXY_PLUGIN_NAME,     /*   name                            */
    "Alibaba Cloud",        /*   author                          */
    "Galaxy Plugin",        /*   description                     */
    PLUGIN_LICENSE_GPL,
    gs::galaxy_init,        /*   init function (when loaded)     */
    NULL,                   /*   check uninstall function        */
    gs::galaxy_deinit,      /*   deinit function (when unloaded) */
    0x0101,                 /*   version                         */
    gs::galaxy_status_vars, /*   status variables                */
    gs::galaxy_system_vars, /*   system variables                */
    NULL,
    0,
} mysql_declare_plugin_end;
