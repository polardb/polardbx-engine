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

#include "lizard0dict.h"
#include "dict0dd.h"
#include "dict0mem.h"

namespace lizard {

/** The space name of lizard */
const char *dict_lizard::s_lizard_space_name = "innodb_lizard";

/** The file name of lizard space */
const char *dict_lizard::s_lizard_space_file_name = "lizard.ibd";

/** Lizard: First several undo tablespaces will be treated as txn tablespace */
const char *dict_lizard::s_default_txn_space_names[] = {
    "innodb_undo_001", "innodb_undo_002", "innodb_undo_003", "innodb_undo_004"};

/** Judge the undo tablespace is txn tablespace by name */
bool is_txn_space_by_name(const char *name) {
  if (name && (strcmp(name, dict_lizard::s_default_txn_space_names[0]) == 0 ||
               strcmp(name, dict_lizard::s_default_txn_space_names[1]) == 0 ||
               strcmp(name, dict_lizard::s_default_txn_space_names[2]) == 0 ||
               strcmp(name, dict_lizard::s_default_txn_space_names[3]) == 0))
    return true;

  return false;
}

}  // namespace lizard
