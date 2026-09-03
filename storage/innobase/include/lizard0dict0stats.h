/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file lizard0dict0stats.h
  SPC dict stats.

 Created 2026-03-06 by Jiyang.zhang
 *******************************************************/
#ifndef lizard0dict0stats_h
#define lizard0dict0stats_h

#include <string>
#include "db0err.h"

class dict_table_t;

namespace lizard {
/**
  Analyze SPC statistics for a table and return formatted string.
  @param table          Table to analyze
  @param str_out        Output string for formatted SPC statistics
  @retval DB_SUCCESS    Success
  @retval DB_CORRUPTION Table is corrupted
*/
extern dberr_t dict_stats_analyze_table_spc(
    dict_table_t *table,   /*!< in/out: table */
    std::string *str_out); /*!< out: formatted statistics */
}

#endif
