/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by zzy on 2022/8/9.
//

#pragma once

#include "sql/log.h"

#define log_debug sql_print_information
#define log_exec_warning sql_print_warning
#define log_exec_error sql_print_error

#define POLARX_EXEC_DBG 1

#if POLARX_EXEC_DBG
#define EXEC_LOG_INFO(_x_) sql_print_information _x_
#define EXEC_LOG_WARN(_x_) sql_print_warning _x_
#define EXEC_LOG_ERR(_x_) sql_print_error _x_
#else
#define EXEC_LOG_INFO(_x_)
#endif
