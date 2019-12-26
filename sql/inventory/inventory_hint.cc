/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/inventory/inventory_hint.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/transaction.h"

namespace im {

void Inventory_hint::print(const THD *, String *str) {
  if (m_flags & IC_HINT_COMMIT)
    str->append(STRING_WITH_LEN("COMMIT_ON_SUCCESS "));
  if (m_flags & IC_HINT_ROLLBACK)
    str->append(STRING_WITH_LEN("ROLLBACK_ON_FAIL "));
  if (m_flags & IC_HINT_TARGET) {
    std::stringstream ss;
    ss << "TARGET_AFFECT_ROW"
       << "(" << m_target_rows << ")";
    str->append(ss.str().c_str(), ss.str().length());
  }
}

/**
  Precheck the inventory hint whether it's reasonable.
*/
bool disable_inventory_hint(THD *thd, Inventory_hint *hint) {
  if (!hint)
    return false;

  bool has_transactional_hint = hint->is_specified(IC_HINT_COMMIT) ||
                                hint->is_specified(IC_HINT_ROLLBACK);

  /* Transactional hint didn't allowed in autocommit mode. */
  if (has_transactional_hint && !thd->in_multi_stmt_transaction_mode()) {
    my_error(ER_IC_NOT_ALLOWED_IN_AUTOCOMMIT, MYF(0));
    return true;
  }

  /* Transactional hint didn't allowed in sub statement. */
  if (has_transactional_hint && thd->in_sub_stmt) {
    my_error(ER_IC_NOT_ALLOWED_IN_SUB_STMT, MYF(0));
    return true;
  }

  return false;
}

/**
  Process the target affected rows hint.
*/
void process_inventory_target_hint(THD *thd, Inventory_hint *hint) {
  DBUG_ENTER("process_inventory_target");
  assert(!thd->in_sub_stmt);
  if (hint && hint->is_specified(IC_HINT_TARGET)) {
    if (thd->get_row_count_func() != (longlong)(hint->get_target_rows())) {
      thd->get_stmt_da()->set_overwrite_status(true);
      my_error(ER_IC_NOT_MATCH_TARGET, MYF(0));
      thd->get_stmt_da()->set_overwrite_status(false);
    }
  }
  DBUG_VOID_RETURN;
}

/**
  Process the transactional hint.
*/
void process_inventory_transactional_hint(THD *thd, Inventory_hint *hint) {
  if (hint && hint->is_specified(IC_HINT_COMMIT)) {
    if (!thd->is_error()) {
      thd->get_stmt_da()->set_overwrite_status(true);
      /* Commit the normal transaction if one is active. */
      trans_commit_implicit(thd);
      thd->get_stmt_da()->set_overwrite_status(false);
      thd->mdl_context.release_transactional_locks();
    }
  }
  if (hint && hint->is_specified(IC_HINT_ROLLBACK)) {
     if (thd->is_error()) {
       trans_rollback_implicit(thd);
       thd->mdl_context.release_transactional_locks();
     }
  }
}

} /* namespace im */
