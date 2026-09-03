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

/** @file sql/dd/komodo0dd0policy0types.h
 Komodo DDL policy types

 Created 2024-07-11 by Jiyang.Zhang
 *******************************************************/
#ifndef komodo0dd0policy0types_h
#define komodo0dd0policy0types_h

#include <vector>
#include <unordered_set>

#include "include/my_inttypes.h"

#include "lex_string.h"
#include "sql/dd/properties.h"

class THD;
struct dict_table_t;
struct dict_index_t;
namespace dd {
class Table;
class Index;
}  // namespace dd

namespace komodo {

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

/** Secondary index page type with tid. */
constexpr uint16_t HIPPO_PAGE_TYPE = 41;

/** SPC page type */
constexpr uint16_t SPC_PAGE_TYPE = 42;

/** Secondary engine attributes which affect dictionary structure on table or
 * index*/
class dd_se_attrs_t {
 public:
  enum Attr_type { ATTR_NOT_FOUND = -1, ATTR_FALSE = 0, ATTR_TRUE = 1 };

  /* Attention: We can't solve the uninitialized problem in
   * key_info->m_se_attr*/
  static bool is_valid_attr(Attr_type attr) {
    return attr == ATTR_NOT_FOUND || attr == ATTR_FALSE || attr == ATTR_TRUE;
  }

  /** Constructor by dd se column */
  dd_se_attrs_t(LEX_CSTRING se_attrs);

  dd_se_attrs_t() = default;

  Attr_type attr_gpp() const {
    return is_valid_attr(m_attr_gpp) ? m_attr_gpp : ATTR_NOT_FOUND;
  }

  Attr_type attr_panda() const {
    return is_valid_attr(m_attr_panda) ? m_attr_panda : ATTR_NOT_FOUND;
  }

  Attr_type attr_hippo() const {
    return is_valid_attr(m_attr_hippo) ? m_attr_hippo : ATTR_NOT_FOUND;
  }

  Attr_type attr_spc() const {
    return is_valid_attr(m_attr_spc) ? m_attr_spc : ATTR_NOT_FOUND;
  }

  bool not_found() const {
    return m_attr_gpp == ATTR_NOT_FOUND && m_attr_panda == ATTR_NOT_FOUND &&
           m_attr_hippo == ATTR_NOT_FOUND &&
           m_attr_spc == ATTR_NOT_FOUND;
  }

  /** Print se attributes
   *
   * @reval	formated string */
  std::string to_string() const {
    std::ostringstream oss;
    oss << "{";
    if (m_attr_gpp != ATTR_NOT_FOUND) {
      oss << "\"gpp\": ";
      oss << (m_attr_gpp ? "\"true\"" : "\"false\"");
      oss << ", ";
    }
    if (m_attr_panda != ATTR_NOT_FOUND) {
      oss << "\"panda index\": ";
      oss << (m_attr_panda ? "\"true\"" : "\"false\"");
      oss << ", ";
    }
    if (m_attr_hippo != ATTR_NOT_FOUND) {
      oss << "\"hippo index\": ";
      oss << (m_attr_hippo ? "\"true\"" : "\"false\"");
      oss << ", ";
    }
    if (m_attr_spc != ATTR_NOT_FOUND) {
      oss << "\"spc\": ";
      oss << (m_attr_spc ? "\"true\"" : "\"false\"");
    }
    oss << "}";
    return oss.str();
  }

 private:
  /** gpp attribute that affect index */
  Attr_type m_attr_gpp{ATTR_NOT_FOUND};
  /** panda attribute that affect index */
  Attr_type m_attr_panda{ATTR_NOT_FOUND};
  /** hippo attribute that affect index */
  Attr_type m_attr_hippo{ATTR_NOT_FOUND};
  /** spc attribute that affect index */
  Attr_type m_attr_spc{ATTR_NOT_FOUND};
};

/** Variable options which affect dictionary structure on table or index. */
class dd_var_options_t {
 public:
  dd_var_options_t(const THD *thd, bool inherit);

  bool option_fba() const { return m_option_fba; }

  bool option_gpp() const { return m_option_gpp; }

  bool option_panda() const { return m_option_panda; }

  bool option_hippo() const { return m_option_hippo; }

  bool option_spc() const { return m_option_spc; }

  bool should_inherit() const { return m_inherit; }

 private:
  /** ------Table options------- */
  /** option flashback area. */
  unsigned int m_option_fba : 1;

  /** ------Index options------- */
  /** option GPP */
  unsigned int m_option_gpp : 1;

  /** option PANDA */
  bool m_option_panda;

  /** option HIPPO. */
  bool m_option_hippo;

  /** option SPC */
  bool m_option_spc;

  /** Indicates whether a partitioned table should inherit table options from
   * the parent table */
  bool m_inherit;
};

class Index_policy;

/** Decide index hint come from se attributes and variable options */
class Dict_index_hint {
 public:
  Dict_index_hint(const dd_se_attrs_t *se_attrs,
                  const dd_var_options_t *var_options);

  Dict_index_hint() = default;

  /** New index policy by hint and dict structure. */
  Index_policy invoke_policy(const dict_table_t *table,
                             const dict_index_t *index) const;

 private:
  bool hint_gpp() const { return m_hint_gpp; }

  bool hint_panda() const { return m_hint_panda; }

  bool hint_hippo() const { return m_hint_hippo; }

  bool hint_spc() const { return m_hint_spc; }

  bool hint_root_page_type() const { return (hint_panda() || hint_hippo() || hint_spc()); }

  uint16_t upgrade_root_page_type(const dict_table_t *table,
                                  const dict_index_t *index) const;


 private:
  bool m_hint_gpp{false};
  bool m_hint_panda{false};
  bool m_hint_hippo{false};
  bool m_hint_spc{false};
};

class Table_policy;

class Dict_table_hint {
 public:
  Dict_table_hint(const dd_var_options_t *var_options)
      : m_hint_fba(var_options->option_fba()),
        m_inherit(var_options->should_inherit()) {}

  Dict_table_hint() = default;

  /**
    Initializes the Table_policy with options based on the ddl hint and the
    table information.

    @param[in]    table           dict_t table
    @param[in]    dd_table        The old parent table of a partitioned table.
    Must be provided if the ddl hint requires inheritance.
  */
  Table_policy invoke_policy(const dict_table_t *table,
                             const dd::Table *dd_table) const;

 private:
  bool hint_fba() const { return m_hint_fba; }

  bool should_inherit() const { return m_inherit; }

 private:
  /** Hint flashback area. */
  bool m_hint_fba{false};
  /** Indicates whether a partitioned table should inherit table options
   * from the parent table */
  bool m_inherit{false};
};

/**
  Index external DDL policy like GPP info.
*/
class Index_policy {
 public:
  Index_policy()
      : m_inited(false), m_gpp(0), m_page_type(1 /*FIL_PAGE_TYPE_UNUSED*/) {}

  bool has_gpp() const { return m_gpp; }

  uint16_t page_type() const { return m_page_type; }

  uint16_t pd_capacity() const { return m_pd_capacity; }

  void restore(const dd::Properties &options,
               const dd::Properties &se_private_data);

  bool inited() const { return m_inited; }

  void init(unsigned int gpp, uint16_t page_type, uint16_t pd_capacity) {
    m_gpp = gpp;
    m_page_type = page_type;
    m_pd_capacity = pd_capacity;
    m_inited = true;
  }

 private:
  bool m_inited;

  unsigned int m_gpp : 1;
  /* Expected or real page type. Currently, only FIL_PAGE_INDEX_PANDA,
   * FIL_PAGE_INDEX_HIPPO, FIL_PAGE_INDEX_SPC and 
   * FIL_PAGE_TYPE_UNUSED can be used.*/
  uint16_t m_page_type;
  /** Page dict capacity */
  uint16_t m_pd_capacity;
};

using Indexes_policy = std::vector<Index_policy>;

/**
  Table external DDL policy like flashback area info.
*/
class Table_policy {
 public:
  Table_policy() : m_inited(false), m_flashback_area(0) {}

  bool has_fba() const { return m_flashback_area; }

  void restore(const dd::Properties &options);

  bool inited() const { return m_inited; }

  void init(unsigned int flashback_area) {
    m_flashback_area = flashback_area;
    m_inited = true;
  }

 private:
  bool m_inited;
  unsigned int m_flashback_area : 1;
};

}  // namespace komodo

#endif
