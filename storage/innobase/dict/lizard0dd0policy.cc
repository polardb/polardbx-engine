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

/** @file dict/lizard0dd0policy.cc
 Lizard DDL policy

 Created 2024-07-11 by Jiyang.Zhang
 *******************************************************/

#include <sstream>
#include "sql/dd/types/partition.h"
#include "sql/dd/types/partition_index.h"
#include "sql/dd/types/table.h"
#include "sql/sql_class.h"

#include "dict0dd.h"
#include "dict0dict.h"
#include "dict0mem.h"
#include "ut0dbg.h"

#include "lizard0dd0policy.h"
#include "dict0dd.h"
#include "lizard0dict.h"

#include "my_rapidjson_size_t.h"
#include "rapidjson/document.h"

namespace lizard {

static bool can_gpp(const dict_table_t *table, const dict_index_t *index) {
  return !table->is_compressed() && !table->is_temporary() &&
         !table->is_intrinsic() && !dict_sys_t::is_dd_table_id(table->id) &&
         (DBUG_EVALUATE_IF("allow_dd_tables_have_gpp", true,
                           !table->is_system_table)) &&
         !dict_sys->is_permanent_table(table) && !(index->type & DICT_IBUF) &&
         !(index->type & DICT_SDI) && !(index->type & DICT_FTS) &&
         !dict_index_is_spatial(index) && !index->is_clustered();
}

/**
 * Detemines whether the root page type of the index should be upgraded to
 * PANDA.
 * @param[in]     table         dict table.
 * @param[in]     index         dict index.
 * @param[in]     index_hint    DDL index hint.
 * @return The upgraded root page type.
 */
static page_type_t upgrade_root_page_type(const dict_table_t *table,
                                          const dict_index_t *index,
                                          const Ha_index_hint *index_hint) {
  page_type_t index_type;
  if (dict_index_is_spatial(index)) {
    index_type = FIL_PAGE_RTREE;
  } else if (dict_index_is_sdi(index)) {
    index_type = FIL_PAGE_SDI;
  } else {
    index_type = FIL_PAGE_INDEX;
  }

  /* For normal b-tree index creation. */
  if (index_type == FIL_PAGE_INDEX && dict_index_is_unique(index) &&
      !index->is_clustered() && index_hint && index_hint->hint_panda() &&
      !dict_index_has_virtual(index) && !table->is_temporary() &&
      !table->is_compressed() && !dict_sys_t::is_dd_table_id(table->id) &&
      !table->is_system_table && !dict_sys->is_permanent_table(table) &&
      !index->is_multi_value()) {
    return FIL_PAGE_INDEX_PANDA;
  } else {
    return FIL_PAGE_TYPE_UNUSED;
  }
}

static bool can_fba(const dict_table_t *table) {
  return !table->is_temporary() && !table->is_intrinsic() &&
         !dict_sys_t::is_dd_table_id(table->id) && !table->is_system_table &&
         !dict_sys->is_permanent_table(table);
}

/**
 * Initializes the Index_policy with options based on the ddl hint and the
 * table/index information.
 * @param[in]     index_hint    DDL index hint.
 * @param[in]     table         dict table.
 * @param[in]     index         dict index.
 */
void Index_policy::create(const Ha_index_hint *index_hint,
                          const dict_table_t *table,
                          const dict_index_t *index) {
  ut_ad(!m_gpp);
  ut_ad(m_page_type == FIL_PAGE_TYPE_UNUSED);
  ut_a(m_inited == false);
  m_inited = true;

  if (index_hint && index_hint->hint_gpp()) {
    ut_ad(!(index->type & DICT_SDI));
    ut_ad(!(index->type & DICT_IBUF));

    m_gpp = can_gpp(table, index);
  }
  m_page_type = upgrade_root_page_type(table, index, index_hint);
}

void Index_policy::restore(const dd::Properties &options,
                           const dd::Properties &se_private_data) {
  ut_ad(!m_gpp);
  ut_ad(m_page_type == FIL_PAGE_TYPE_UNUSED);
  ut_a(m_inited == false);
  m_inited = true;
  ulonglong format = 0;

  if (options.exists(OPTION_IFT)) {
    options.get(OPTION_IFT, &format);
  }

  m_gpp = (format & IFT_GPP);

  if (se_private_data.exists(dd_index_key_strings[DD_INDEX_PAGE_TYPE])) {
    se_private_data.get(dd_index_key_strings[DD_INDEX_PAGE_TYPE], &m_page_type);
  }
  if (m_page_type != FIL_PAGE_INDEX_PANDA) {
    m_page_type = FIL_PAGE_TYPE_UNUSED;
  }
}

/**
  Initializes the Table_policy with options based on the ddl hint and the
  table information.

  @param[in]    var_hint      ddl hint from handler
  @param[in]    table           dict_t table
  @param[in]    dd_table        The old parent table of a partitioned table.
  Must be provided if the ddl hint requires inheritance.
*/
void Table_policy::create(const Ha_table_hint *table_hint,
                          const dict_table_t *table,
                          const dd::Table *dd_table) {
  ut_a(m_inited == false);
  m_inited = true;

  if (!table_hint) {
    return;
  }

  if (table_hint->should_inherit()) {
    /** Inherit table options from the parent table of partition. */
    ut_ad(dd_table && dd_table_is_partitioned(*dd_table));

    m_flashback_area = lizard::dd_table_options_has_fba(&dd_table->options());
    ut_ad(!m_flashback_area || can_fba(table));
  } else {
    m_flashback_area = (table_hint->hint_fba() && can_fba(table));
  }
}

void Table_policy::restore(const dd::Properties &options) {
  bool fba = false;

  if (options.exists(TABLE_OPTION_FBA)) {
    options.get(TABLE_OPTION_FBA, &fba);
  }

  m_flashback_area = fba;

  m_inited = true;
}

const Index_policy ha_ddl_create_index_policy(const Ha_index_hint *index_hint,
                                              const dict_table_t *table,
                                              const dict_index_t *index) {
  Index_policy index_policy;

  index_policy.create(index_hint, table, index);

  return index_policy;
}

/**
  Create the Table_policy.

  @param[in]    table_hint      ddl table hint from handler
  @param[in]    table           dict_t table
  @param[in]    dd_table        The old parent table of a partitioned table.
  Must be provided if the ddl hint requires inheritance.
*/
const Table_policy ha_ddl_create_table_policy(const Ha_table_hint *table_hint,
                                              const dict_table_t *table,
                                              const dd::Table *dd_table) {
  Table_policy table_policy;

  table_policy.create(table_hint, table, dd_table);
  return table_policy;
}

/**
 * Parse attribute value from secondary engine attribute
 * @param[in] secondary_engine_attribute Secondary engine attribute string
 * @param[in] attr_name Attribute name to parse
 * @return Hint type
 */
static Ha_se_attr_hint::Hint_type parse_se_attr(
    LEX_CSTRING secondary_engine_attribute, const char *attr_name) {
  if (secondary_engine_attribute.length == 0 ||
      secondary_engine_attribute.str == nullptr) {
    return Ha_se_attr_hint::HINT_NOT_FOUND;
  }

  rapidjson::Document doc;
  doc.Parse(secondary_engine_attribute.str, secondary_engine_attribute.length);

  if (doc.HasParseError() || !doc.IsObject()) {
    return Ha_se_attr_hint::HINT_NOT_FOUND;
  }

  std::string target_key = attr_name;
  std::transform(target_key.begin(), target_key.end(), target_key.begin(),
                 ::tolower);

  for (auto const &member : doc.GetObject()) {
    if (!member.name.IsString()) {
      continue;
    }

    std::string current_key_str = member.name.GetString();
    std::transform(current_key_str.begin(), current_key_str.end(),
                   current_key_str.begin(), ::tolower);

    if (current_key_str == target_key) {
      if (member.value.IsString()) {
        std::string val_str = member.value.GetString();
        std::transform(val_str.begin(), val_str.end(), val_str.begin(),
                       ::tolower);

        if (val_str == "true" || val_str == "1") {
          return Ha_se_attr_hint::HINT_TRUE;
        } else if (val_str == "false" || val_str == "0") {
          return Ha_se_attr_hint::HINT_FALSE;
        }
      }
    }
  }
  return Ha_se_attr_hint::HINT_NOT_FOUND;
}

Ha_se_attr_hint::Ha_se_attr_hint(LEX_CSTRING se_attr) {
  m_hint_gpp = parse_se_attr(se_attr, "gpp");
  m_hint_panda = parse_se_attr(se_attr, "panda index");
}

std::string Ha_se_attr_hint::to_string() const {
  std::ostringstream oss;
  oss << "{";
  if (m_hint_gpp != HINT_NOT_FOUND) {
    oss << "\"gpp\": ";
    oss << (m_hint_gpp ? "\"true\"" : "\"false\"");
    oss << ", ";
  }
  if (m_hint_panda != HINT_NOT_FOUND) {
    oss << "\"panda index\": ";
    oss << (m_hint_panda ? "\"true\"" : "\"false\"");
  }
  oss << "}";
  return oss.str();
}

Ha_var_hint::Ha_var_hint(const THD *thd, bool inherit)
    : m_hint_fba(0), m_hint_gpp(0), m_hint_panda(false), m_inherit(inherit) {
  if (thd->variables.opt_flashback_area) {
    ut_a(!m_hint_fba);
    m_hint_fba = 1;
  }

  if (thd->variables.opt_index_format_gpp_enabled) {
    ut_a(!m_hint_gpp);
    m_hint_gpp = 1;
  }

  if (thd->variables.opt_index_format_panda_enabled) {
    m_hint_panda = true;
  }
}

Ha_index_hint::Ha_index_hint(const Ha_se_attr_hint *se_attr_hint,
                             const Ha_var_hint *var_hint) {
  if (se_attr_hint &&
      se_attr_hint->hint_gpp() != Ha_se_attr_hint::HINT_NOT_FOUND) {
    m_hint_gpp = se_attr_hint->hint_gpp();
  } else {
    m_hint_gpp = var_hint && var_hint->hint_gpp();
  }

  if (se_attr_hint &&
      se_attr_hint->hint_panda() != Ha_se_attr_hint::HINT_NOT_FOUND) {
    m_hint_panda = se_attr_hint->hint_panda();
  } else {
    m_hint_panda = var_hint && var_hint->hint_panda();
  }
}

template <typename Table>
Indexes_policy dd_fill_indexes_policy(const Table *dd_table) {
  Indexes_policy indexes_policy;

  for (auto dd_index : dd_table->indexes()) {
    indexes_policy.emplace_back();
    indexes_policy.back().restore(dd_index->options(),
                                  dd_index->se_private_data());
  }

  return indexes_policy;
}

template Indexes_policy dd_fill_indexes_policy<dd::Table>(
    const dd::Table *dd_table);

template Indexes_policy dd_fill_indexes_policy<dd::Partition>(
    const dd::Partition *dd_table);

void dd_fill_table_policy(Table_policy &table_policy,
                          const dd::Table &dd_table) {
  table_policy.restore(dd_table.options());
}

}  // namespace lizard
