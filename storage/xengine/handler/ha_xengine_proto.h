/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012,2013 Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#pragma once

/* C++ standard header files */
#include <string>
#include <vector>

/* MySQL header files */
#include "./sql_string.h"

/* XENGINE includes */
#include "xengine/table.h"
#include "xengine/utilities/transaction_db.h"

namespace myx {

enum XDB_IO_ERROR_TYPE {
  XDB_IO_ERROR_TX_COMMIT,
  XDB_IO_ERROR_DICT_COMMIT,
  XDB_IO_ERROR_BG_THREAD,
  XDB_IO_ERROR_GENERAL,
  XDB_IO_ERROR_LAST
};

const char *get_xdb_io_error_string(const XDB_IO_ERROR_TYPE err_type);

void xdb_handle_io_error(const xengine::common::Status status,
                         const XDB_IO_ERROR_TYPE err_type);

int xdb_normalize_tablename(const std::string &tablename, std::string *str)
    MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

int xdb_split_normalized_tablename(const std::string &fullname, std::string *db,
                                   std::string *table = nullptr,
                                   std::string *partition = nullptr)
    MY_ATTRIBUTE((__warn_unused_result__));

std::vector<std::string> xdb_get_open_table_names(void);

void xdb_queue_save_stats_request();

/*
  Access to singleton objects.
*/

bool xdb_is_initialized();

xengine::util::TransactionDB *xdb_get_xengine_db();

class Xdb_cf_manager;
Xdb_cf_manager &xdb_get_cf_manager();

xengine::table::BlockBasedTableOptions &xdb_get_table_options();

class Xdb_dict_manager;
Xdb_dict_manager *xdb_get_dict_manager(void)
    MY_ATTRIBUTE((__warn_unused_result__));

class Xdb_ddl_manager;
Xdb_ddl_manager *xdb_get_ddl_manager(void)
    MY_ATTRIBUTE((__warn_unused_result__));

class Xdb_binlog_manager;
Xdb_binlog_manager *xdb_get_binlog_manager(void)
    MY_ATTRIBUTE((__warn_unused_result__));

} // namespace myx
