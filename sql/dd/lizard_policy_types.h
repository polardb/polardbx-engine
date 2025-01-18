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

/** @file sql/dd/lizard_policy_types.h
 Lizard DDL policy types

 Created 2024-07-11 by Jiyang.Zhang
 *******************************************************/
#ifndef LIZARD_SQL_POLICY_TYPES_INCLUDED
#define LIZARD_SQL_POLICY_TYPES_INCLUDED

#include <vector>
#include <unordered_set>

#include "include/my_inttypes.h"

#include "sql/dd/properties.h"
#include "sql/dd/types/table.h"

class THD;
struct dict_table_t;
struct dict_index_t;
namespace dd {
class Table;
class Index;
}  // namespace dd

namespace lizard {

class Ha_ddl_policy;

/** Lizard Index Format:
 *
 * In order to speed up index scan, MVCC efficiency and so on, we define new
 * index format specially for InnoDB Btree
 *
 * 1) GPP (Guess Primary Page NO)
 *
 * 2) TXN (Transaction info)
 *
 * Currently, Only support visible innodb secondary btree index.
 * */

/** String of IFT option */
constexpr char OPTION_IFT[] = "IFT";

/** TABLE OPTION: Flashback Area */
constexpr char TABLE_OPTION_FBA[] = "flashback_area";

/** String of page type */
constexpr char PAGE_TYPE_STR[] = "page_type";

/** GPP format */
constexpr const ulonglong IFT_GPP = 1L << 0;

/** TXN format */
constexpr const ulonglong IFT_TXN = 1L << 1;

/** PANDA page type */
constexpr uint16_t PANDA_PAGE_TYPE = 40;

/**
  Index external DDL policy like GPP info.
*/
class Index_policy {
 public:
  Index_policy()
      : m_inited(false), m_gpp(0), m_page_type(1 /*FIL_PAGE_TYPE_UNUSED*/) {}

  bool has_gpp() const { return m_gpp; }

  uint16_t page_type() const { return m_page_type; }

  /**
   * Initializes the Index_policy with options based on the ddl policy and the
   * table/index information.
   * @param[in]     ddl_policy DDL policy.
   * @param[in]     table dict table.
   * @param[in]     index dict index.
   */
  void create(Ha_ddl_policy *ddl_policy, const dict_table_t *table,
              const dict_index_t *index);

  void restore(const dd::Properties &options,
               const dd::Properties &se_private_data);

  bool inited() const { return m_inited; }

 private:
  bool m_inited;
  unsigned int m_gpp : 1;
  /* Expected or real page type. Currently, only FIL_PAGE_INDEX_PANDA and
   * FIL_PAGE_TYPE_UNUSED can be used.*/
  uint16_t m_page_type;
};

typedef std::vector<Index_policy> Indexes_policy;

/**
  Table external DDL policy like flashback area info.
*/
class Table_policy {
 public:
  Table_policy() : m_inited(false), m_flashback_area(0) {}

  bool has_fba() const { return m_flashback_area; }

  void create(const Ha_ddl_policy *ddl_policy, const dict_table_t *table,
              const dd::Table *old_part_table = nullptr);

  void restore(const dd::Properties &options);

  bool inited() const { return m_inited; }

 private:
  bool m_inited;
  unsigned int m_flashback_area : 1;
};

class Ha_ddl_policy {
 public:
  Ha_ddl_policy(const THD *thd, bool inherit = false);

  bool hint_fba() const { return m_hint_fba; }

  bool hint_gpp() const { return m_hint_gpp; }

  bool should_inherit() const { return m_inherit; }

  bool hint_panda() const;

 private:
  /** ------Table options------- */
  /** Hint flashback area. */
  unsigned int m_hint_fba : 1;

  /** ------Index options------- */
  /** Hint GPP */
  unsigned int m_hint_gpp : 1;
  /** Hint PANDA */
  bool m_hint_panda;

  /** Indicates whether a partitioned table should inherit table options from
   * the parent table */
  bool m_inherit;
};

}  // namespace lizard

#endif
