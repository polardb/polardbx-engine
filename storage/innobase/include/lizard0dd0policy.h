/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0dd0policy.cc
 Lizard DDL policy

 Created 2024-07-11 by Jiyang.Zhang
 *******************************************************/
#ifndef LIZARD_DD_POLICY_INCLUDED
#define LIZARD_DD_POLICY_INCLUDED

#include "dd/types/table.h"
#include "sql/dd/lizard_policy_types.h"

namespace lizard {

/**
  Fill Indexes_policy from dd::Table or dd::Partition object.
  Which should be called when open a table.
*/
template <typename Table>
extern Indexes_policy dd_fill_indexes_policy(const Table *dd_table);

/**
  Fill table_policy from a dd::Table or dd::Partition object.
  This function should be called when open a table.

  @param[in,out]  table_policy       table policy to be filled
  @param[in]      dd_table           dd::table
*/
extern void dd_fill_table_policy(Table_policy &table_policy,
                                 const dd::Table &dd_table);

/**
  Create the Table_policy.

  @param[in]    table_hint      ddl table hint from handler
  @param[in]    table           dict_t table
  @param[in]    dd_table        The old parent table of a partitioned table.
  Must be provided if the ddl hint requires inheritance.
*/
extern const Table_policy ha_ddl_create_table_policy(
    const Ha_table_hint *table_hint, const dict_table_t *table,
    const dd::Table *dd_table = nullptr);

extern const Index_policy ha_ddl_create_index_policy(
    const Ha_index_hint *index_hint, const dict_table_t *table,
    const dict_index_t *index);
}  // namespace lizard

#endif
