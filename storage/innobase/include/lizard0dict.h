/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0dict.h
 Special Zeus tablespace definition.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0dict_h
#define lizard0dict_h
#include "univ.i"

#include "api0api.h"
#include "dict0dict.h"
#include "mem0mem.h"
#include "sql/dd/object_id.h"

#include "lizard0txn0types.h"
#include "lizard0undo0types.h"

struct dict_table_t;
struct dict_index_t;

namespace dd {
class Table;
class Index;
}  // namespace dd

namespace lizard {

/** Here define the lizard related dictionary content */

struct dict_lizard {
  /** The lizard tablespace space id */
  static constexpr space_id_t s_lizard_space_id = 0xFFFFFFFA;

  /** The dd::Tablespace::id of innodb_lizard */
  static constexpr dd::Object_id s_dd_lizard_space_id = 3;

  /** Whether the last file of lizard space is auto extend */
  static constexpr const bool s_file_auto_extend = true;

  /** The init file size (bytes) */
  static constexpr const ulint s_file_init_size = 12 * 1024 * 1024;

  /** The last file max size (bytes) */
  static constexpr const ulint s_last_file_max_size = 0;

  /** Lizard tablespace file count */
  static constexpr const ulint s_n_files = 1;

  /** The space name of lizard */
  static const char *s_lizard_space_name;

  /** The file name of lizard space */
  static const char *s_lizard_space_file_name;

  /** The lizard transaction tablespace is from max undo tablespace */
  static constexpr space_id_t s_max_txn_space_id =
      dict_sys_t::s_max_undo_space_id;

  /** The lizard transaction tablespace minmum space id */
  static constexpr space_id_t s_min_txn_space_id =
      dict_sys_t::s_max_undo_space_id - FSP_IMPLICIT_TXN_TABLESPACES + 1;

  /** Lizard: First several undo tbs will be treated as txn tablespace */
  static const char *s_default_txn_space_names[FSP_IMPLICIT_TXN_TABLESPACES];
};

/** Judge the undo tablespace is txn tablespace by name */
extern bool is_txn_space_by_name(const char *name);
}  // namespace lizard

#endif  // lizard0dict_h
