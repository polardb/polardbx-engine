/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/current_thd.h"
#include "sql/sql_class.h"
#include "sql/sql_implicit_common.h"

#include "sql/sql_implicit_element.h"


bool opt_enable_implicit_row_id = false;

/**
  Judge whether show the implicit object.

  Syntax:
    int CAN_ACCESS_IMPLICIT_OBJECT(field_name);

  @retval
    1   Show the field
    0   Hide the field
*/
longlong Item_func_can_access_implicit_object::val_int() {
  DBUG_ENTER("Item_func_can_access_implicit_object::val_int");

  String field_name;

  String *field_name_ptr = args[0]->val_str(&field_name);

  if (field_name_ptr == nullptr) {
    null_value = true;
    DBUG_RETURN(1);
  }

  field_name_ptr->c_ptr_safe();

  THD *thd = current_thd;

  if (ITEM_IS_IMPLICIT_AND_HIDDEN(thd, field_name_ptr->ptr()))
    DBUG_RETURN(0);

  DBUG_RETURN(1);
}

