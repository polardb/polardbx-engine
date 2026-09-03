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

#include "sql/dd/types/partition.h"
#include "sql/dd/types/partition_index.h"
#include "sql/dd/types/table.h"
#include "sql/sql_class.h"

#include "dict0dd.h"
#include "dict0dict.h"
#include "dict0mem.h"
#include "ut0dbg.h"

#include "lizard0dd0policy.h"
#include "lizard0dict0dd.h"

#include "my_rapidjson_size_t.h"
#include "rapidjson/document.h"
#include "sql/dd/komodo0dd0policy0types.h"

namespace komodo {

static bool can_gpp(const dict_table_t *table, const dict_index_t *index) {
  return !table->is_compressed() && !table->is_temporary() &&
         !table->is_intrinsic() && !dict_sys_t::is_dd_table_id(table->id) &&
         (DBUG_EVALUATE_IF("allow_dd_tables_have_gpp", true,
                           !table->is_system_table)) &&
         !dict_sys->is_permanent_table(table) && !(index->type & DICT_IBUF) &&
         !(index->type & DICT_SDI) && !(index->type & DICT_FTS) &&
         !dict_index_is_spatial(index) && !index->is_clustered();
}

static bool can_fba(const dict_table_t *table) {
  return !table->is_temporary() && !table->is_intrinsic() &&
         !dict_sys_t::is_dd_table_id(table->id) && !table->is_system_table &&
         !dict_sys->is_permanent_table(table);
}

static bool can_panda(const dict_table_t *table, const dict_index_t *index,
                      uint16_t index_type) {
  return index_type == FIL_PAGE_INDEX && dict_index_is_unique(index) &&
         !index->is_clustered() && !dict_index_has_virtual(index) &&
         !table->is_temporary() && !table->is_compressed() &&
         !dict_sys_t::is_dd_table_id(table->id) && !table->is_system_table &&
         !dict_sys->is_permanent_table(table) && !index->is_multi_value();
}

static bool can_hippo(const dict_table_t *table, const dict_index_t *index,
                      uint16_t index_type) {
  return index_type == FIL_PAGE_INDEX && !dict_index_is_unique(index) &&
         !table->is_compressed() && !table->is_temporary() &&
         !dict_index_has_virtual(index) && !table->is_intrinsic() &&
         !dict_sys_t::is_dd_table_id(table->id) &&
         !dict_sys->is_permanent_table(table) && !(index->type & DICT_IBUF) &&
         !(index->type & DICT_SDI) && !(index->type & DICT_FTS) &&
         !dict_index_is_spatial(index) && !index->is_clustered() &&
         !table->is_system_table && !index->is_multi_value();
}

static bool can_spc(const dict_table_t *table, const dict_index_t *index,
                    uint16_t index_type) {
  return index_type == FIL_PAGE_INDEX && (UNIV_PAGE_SIZE == 16384) &&
         dict_table_is_comp(table) && !table->is_compressed() &&
         !table->is_temporary() && !table->is_intrinsic() &&
         !dict_sys_t::is_dd_table_id(table->id) && !table->is_system_table &&
         !dict_sys->is_permanent_table(table) &&
         !dict_index_has_virtual(index) && !(index->type & DICT_IBUF) &&
         !(index->type & DICT_SDI) && !(index->type & DICT_FTS) &&
         !dict_index_is_spatial(index) && !index->is_clustered() &&
         !dict_index_is_unique(index);
}

/** Try to upgrade root page type according to dict structure.
 *
 * @param[in]		dict_table_t
 * @param[in]		dict_index_t
 *
 * @retval	Fil page type which was defined in innodb engine.*/
uint16_t Dict_index_hint::upgrade_root_page_type(
    const dict_table_t *table, const dict_index_t *index) const {
  uint16_t index_type;
  if (dict_index_is_spatial(index)) {
    index_type = FIL_PAGE_RTREE;
  } else if (dict_index_is_sdi(index)) {
    index_type = FIL_PAGE_SDI;
  } else {
    index_type = FIL_PAGE_INDEX;
  }

  /* For normal b-tree index creation. */
  if (hint_panda() && can_panda(table, index, index_type)) {
    return FIL_PAGE_INDEX_PANDA;
  } else if (hint_spc() && can_spc(table, index, index_type)) {
    return FIL_PAGE_INDEX_SPC;
  } else if (hint_hippo() && can_hippo(table, index, index_type)) {
    /** TODO: check restrictions for creating hippo */
    return FIL_PAGE_INDEX_HIPPO;
  } else {
    return FIL_PAGE_TYPE_UNUSED;
  }
  return FIL_PAGE_TYPE_UNUSED;
}

/**
 * Parse attribute value from secondary engine attribute
 * @param[in] secondary_engine_attribute Secondary engine attribute string
 * @param[in] attr_name Attribute name to parse
 * @return Hint type
 */
static dd_se_attrs_t::Attr_type parse_se_attr(
    LEX_CSTRING secondary_engine_attribute, const char *attr_name) {
  if (secondary_engine_attribute.length == 0 ||
      secondary_engine_attribute.str == nullptr) {
    return dd_se_attrs_t::ATTR_NOT_FOUND;
  }

  rapidjson::Document doc;
  doc.Parse(secondary_engine_attribute.str, secondary_engine_attribute.length);

  if (doc.HasParseError() || !doc.IsObject()) {
    return dd_se_attrs_t::ATTR_NOT_FOUND;
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
          return dd_se_attrs_t::ATTR_TRUE;
        } else if (val_str == "false" || val_str == "0") {
          return dd_se_attrs_t::ATTR_FALSE;
        }
      }
    }
  }
  return dd_se_attrs_t::ATTR_NOT_FOUND;
}

/** Constructor of dd_se_attrs_t from dd se string. */
dd_se_attrs_t::dd_se_attrs_t(LEX_CSTRING se_attr) {
  m_attr_gpp = parse_se_attr(se_attr, "gpp");
  m_attr_panda = parse_se_attr(se_attr, "panda index");
  m_attr_hippo = parse_se_attr(se_attr, "hippo index");
  m_attr_spc = parse_se_attr(se_attr, "spc");
}

/** Constructor of dd_var_options_t */
dd_var_options_t::dd_var_options_t(const THD *thd, bool inherit)
    : m_option_fba(0),
      m_option_gpp(0),
      m_option_panda(false),
      m_option_hippo(false),
      m_option_spc(false),
      m_inherit(inherit) {
  /** Table option fba */
  if (thd->variables.opt_flashback_area) {
    ut_a(!m_option_fba);
    m_option_fba = 1;
  }

  /** Index option gpp */
  if (thd->variables.opt_index_format_gpp_enabled) {
    ut_a(!m_option_gpp);
    m_option_gpp = 1;
  }

  /** Index option panda */
  if (thd->variables.opt_index_format_panda_enabled) {
    m_option_panda = true;
  }

  /** Index option hippo */
  if (thd->variables.opt_index_format_hippo_enabled) {
    m_option_hippo = true;
  }

  /** Index option spc */
  if (thd->variables.opt_index_format_spc_enabled) {
    m_option_spc = true;
  }
}

/** Constructor */
/** Decide index hint come from se attributes and variable options */
Dict_index_hint::Dict_index_hint(const dd_se_attrs_t *se_attrs,
                                 const dd_var_options_t *var_options) {
  if (se_attrs && se_attrs->attr_gpp() != dd_se_attrs_t::ATTR_NOT_FOUND) {
    m_hint_gpp = se_attrs->attr_gpp();
  } else {
    m_hint_gpp = var_options && var_options->option_gpp();
  }

  if (se_attrs && se_attrs->attr_panda() != dd_se_attrs_t::ATTR_NOT_FOUND) {
    m_hint_panda = se_attrs->attr_panda();
  } else {
    m_hint_panda = var_options && var_options->option_panda();
  }

  if (se_attrs && se_attrs->attr_spc() != dd_se_attrs_t::ATTR_NOT_FOUND) {
    m_hint_spc = se_attrs->attr_spc();
  } else {
    m_hint_spc = var_options && var_options->option_spc();
  }

  if (se_attrs && se_attrs->attr_hippo() != dd_se_attrs_t::ATTR_NOT_FOUND) {
    m_hint_hippo = se_attrs->attr_hippo();
  } else {
    m_hint_hippo = var_options && var_options->option_hippo();
  }
}

/**
 * Initializes the Index_policy with options based on the ddl hint and the
 * table/index information.
 * @param[in]     index_hint    DDL index hint.
 * @param[in]     table         dict table.
 * @param[in]     index         dict index.
 */
Index_policy Dict_index_hint::invoke_policy(const dict_table_t *table,
                                            const dict_index_t *index) const {
  Index_policy policy;
  unsigned int gpp = 0;
  uint16_t page_type = FIL_PAGE_TYPE_UNUSED;

  if (hint_gpp()) {
    ut_ad(!(index->type & DICT_SDI));
    ut_ad(!(index->type & DICT_IBUF));
    gpp = can_gpp(table, index);
  }

  if (hint_root_page_type()) {
    page_type = upgrade_root_page_type(table, index);
  }

  /* Determine pd_capacity later. */
  policy.init(gpp, page_type, 0);
  return policy;
}

/**
  Initializes the Table_policy with options based on the ddl hint and the
  table information.

  @param[in]    table           dict_t table
  @param[in]    dd_table        The old parent table of a partitioned table.
  Must be provided if the ddl hint requires inheritance.
*/
Table_policy Dict_table_hint::invoke_policy(const dict_table_t *table,
                                            const dd::Table *dd_table) const {
  Table_policy table_policy;
  bool flashback_area;

  if (should_inherit()) {
    /** Inherit table options from the parent table of partition. */
    ut_ad(dd_table && dd_table_is_partitioned(*dd_table));

    flashback_area = lizard::dd_table_options_has_fba(&dd_table->options());
    ut_ad(!flashback_area || can_fba(table));
  } else {
    flashback_area = (hint_fba() && can_fba(table));
  }
  table_policy.init(flashback_area);
  return table_policy;
}

// Restore index policy from dd
void Index_policy::restore(const dd::Properties &options,
                           const dd::Properties &se_private_data) {
  ut_ad(!m_gpp);
  ut_ad(m_page_type == FIL_PAGE_TYPE_UNUSED);
  ut_a(m_inited == false);

  unsigned int gpp = 0;
  uint16_t page_type = FIL_PAGE_TYPE_UNUSED;
  ulonglong format = 0;
  uint16_t pd_capacity = 0;

  if (options.exists(OPTION_IFT)) {
    options.get(OPTION_IFT, &format);
  }

  gpp = (format & IFT_GPP);

  if (se_private_data.exists(dd_index_key_strings[DD_INDEX_PAGE_TYPE])) {
    se_private_data.get(dd_index_key_strings[DD_INDEX_PAGE_TYPE], &page_type);
  }
  if (se_private_data.exists(
          dd_index_key_strings[DD_INDEX_PAGE_DICT_CAPACITY])) {
    se_private_data.get(dd_index_key_strings[DD_INDEX_PAGE_DICT_CAPACITY],
                        &pd_capacity);
  }
  if (page_type != FIL_PAGE_INDEX_PANDA && page_type != FIL_PAGE_INDEX_HIPPO &&
      page_type != FIL_PAGE_INDEX_SPC) {
    page_type = FIL_PAGE_TYPE_UNUSED;
  }

  ut_ad((pd_capacity > 0) == (page_type == FIL_PAGE_INDEX_SPC));

  init(gpp, page_type, pd_capacity);
}

// Restore table policy from dd
void Table_policy::restore(const dd::Properties &options) {
  bool fba = false;

  if (options.exists(TABLE_OPTION_FBA)) {
    options.get(TABLE_OPTION_FBA, &fba);
  }

  init(fba);
}

}  // namespace komodo

namespace lizard {

const komodo::Index_policy ha_ddl_invoke_index_policy(
    const komodo::Dict_index_hint *index_hint, const dict_table_t *table,
    const dict_index_t *index) {
  if (index_hint) {
    return index_hint->invoke_policy(table, index);
  } else {
    komodo::Index_policy index_policy;
    index_policy.init(0, FIL_PAGE_TYPE_UNUSED, 0);
    return index_policy;
  }
}

/**
  Create the Table_policy.

  @param[in]    table_hint      ddl table hint from handler
  @param[in]    table           dict_t table
  @param[in]    dd_table        The old parent table of a partitioned table.
  Must be provided if the ddl hint requires inheritance.
*/
const komodo::Table_policy ha_ddl_invoke_table_policy(
    const komodo::Dict_table_hint *table_hint, const dict_table_t *table,
    const dd::Table *dd_table) {
  if (table_hint) {
    return table_hint->invoke_policy(table, dd_table);
  } else {
    komodo::Table_policy table_policy;
    table_policy.init(false);
    return table_policy;
  }
}

template <typename Table>
komodo::Indexes_policy dd_fill_indexes_policy(const Table *dd_table) {
  komodo::Indexes_policy indexes_policy;

  for (auto dd_index : dd_table->indexes()) {
    indexes_policy.emplace_back();
    indexes_policy.back().restore(dd_index->options(),
                                  dd_index->se_private_data());
  }

  return indexes_policy;
}

template komodo::Indexes_policy dd_fill_indexes_policy<dd::Table>(
    const dd::Table *dd_table);

template komodo::Indexes_policy dd_fill_indexes_policy<dd::Partition>(
    const dd::Partition *dd_table);

void dd_fill_table_policy(komodo::Table_policy &table_policy,
                          const dd::Table &dd_table) {
  table_policy.restore(dd_table.options());
}

}  // namespace lizard
