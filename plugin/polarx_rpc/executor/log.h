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
