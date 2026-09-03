/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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
/** @file sql/column_encryption/column_encryption_ctx.cc

  Column Encryption Context Implementation
  Created 2025-05-19 by Zefeng.Liu
 *******************************************************/

#include "sql/column_encryption/column_encryption_ctx.h"
#include "sql/sql_class.h"

namespace komodo {
Column_encryption_ctx::Column_encryption_ctx()
    : m_max_error_count(0), m_is_backup(false) {}

/** Backup column encryption context.
 * @param[in]		which to backup.
 */
void Column_encryption_ctx::backup(const THD *thd) {
  if (!m_is_backup) {
    m_max_error_count = thd->variables.max_error_count;
    m_is_backup = true;
  }
}

/** Restore column encryption context.
 * @param[in/out]		which to restore.
 */
void Column_encryption_ctx::restore(THD *thd) {
  if (m_is_backup) {
    thd->variables.max_error_count = m_max_error_count;
    m_is_backup = false;
  }
}
}  // namespace komodo
