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

/** @file include/lizard0dict0dd.h
 Special Lizard data dictionary interface.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0dict0dd_h
#define lizard0dict0dd_h

#include "sql/dd/types/partition_index.h"

#include "lizard0dict0dict.h"
#include "lizard0rem0slc.h"

struct dict_table_t;
struct dict_index_t;

namespace komodo {
class Snapshot_vision;
class Index_policy;
class Table_policy;
}  // namespace komodo

namespace dd {
class Table;
class Index;
}  // namespace dd

/** mysql.innodb_dynamic_metadata special record slice context. */
class DD_slice_ctx final : public Slice_context_iface {
 public:
  /** Start from the inlined offsets array, no heap is needed until a record
  requires more offsets than REC_OFFS_NORMAL_SIZE. */
  explicit DD_slice_ctx() : m_dslc(), m_heap(nullptr) {
    m_offsets = m_offsets_;
    rec_offs_init(m_offsets_);
  }

  /** Release the heap that might be allocated for large offsets array. */
  ~DD_slice_ctx() {
    if (m_heap) {
      mem_heap_free(m_heap);
    }
  }

  /** Interpret index record (special for mysql.innodb_dynamic_metadata).
   *
   * @param[in]		record
   * @param[in]		index
   * @param[in]		offsets size to allocate.
   * @param[in]		index fields.
   *
   * @retval		contigous record slice. (only for special table). */
  DSlice *get_offsets(creg_t reg, const dict_index_t *index, ulint n_alloc,
                      ulint n_fields);

 private:
  DSlice m_dslc;                          /** Reusable record slice. */
  ulint m_offsets_[REC_OFFS_NORMAL_SIZE]; /** Inlined offsets array. */
  mem_heap_t *m_heap;                     /** Heap for large offsets array. */
  ulint *m_offsets; /** Pointer to real offsets. */
};

namespace lizard {

/**
  Add the lizard columns into data dictionary in server

  @param[in,out]	dd_table	data dictionary cache object
  @param[in,out]  primary   data dictionary primary key
*/
void dd_table_add_txn_columns(dd::Table *dd_table, dd::Index *primary);

/**
  Init txn_desc with the creator trx when created.

  @param[in]      index       the index being created
  @param[in]      trx         creator transaction
  @return         DB_SUCCESS  Success
*/
dberr_t dd_index_init_txn_desc(dict_index_t *index, trx_t *trx);

/**
  Fill index txn information by from se_private_data.

  @param[in,out]  index       the index being opened.
  @param[in]      p           se_private_data from the mysql.indexes record.
  @return         true if failed
*/
bool dd_index_fill_txn_desc(dict_index_t *index, const dd::Properties &p);

/**
  Check if a index should be seen by a transaction.

  @param[in]      index             the index being opened.
  @param[in]      trx               transaction.
  @param[in]      snapshot_vision   AS OF snapshot vision if the query is a
  flashback query, nullptr for a normal read.
  @return         true if visible
*/
bool dd_index_modification_visible(
    dict_index_t *index, const trx_t *trx,
    const komodo::Snapshot_vision *snapshot_vision);

/**
  Instantiate index related metadata about GPP...
  @param[in]      index_policy                 index config info about GPP
  @param[in]      table                        InnoDB table definition
  @param[in,out]  index                        dict_index_t to fill
  @param[out]     expected_or_real_page_type   exptected page type of
  dict_index_t that we want to create, or the real page type in restored
*/
extern void dd_index_fill_format(const komodo::Index_policy &index_policy,
                                 const dict_table_t *table,
                                 dict_index_t *index,
                                 page_type_t *expected_or_real_page_type);

/**
  Write lizard metadata of a index to dd::Index for regular table or
  dd::Partition_index for partition tables.

  @param[in,out]  dd_options  dd_options which might carry GPP info
  @param[in]      index           InnoDB index object
*/
extern void dd_index_write_format(dd::Properties *dd_options,
                                  const dict_index_t *index);

/**
  Copy lizard metadata of a index to dd::Index for regular table or
  dd::Partition_index for partition tables.

  @param[out]     new_dd_options  new dd options to be modified.
  @param[in]      old_dd_options  old dd options from DD
*/
extern void dd_index_copy_format(dd::Properties &new_dd_options,
                                 const dd::Properties &old_dd_options);

/**
  Exchange IFT option between Partition Index and Swap Index.
  @param[in,out]   part_dd_options  dd options from dd::Partition_index
  @param[in,out]   swap_dd_options  dd options from dd::Index from Swap Table
*/
extern void dd_index_exchange_format(dd::Properties &part_dd_options,
                                     dd::Properties &swap_dd_options);

/**
  Judge whether the flashback area option is turned on in dd options.

  @param[in]      options     dd options from dd::Table or dd::Partition_table
  @return         true if the flashback area option exists and is turned on,
  false if it is turned off or absent (as for a legacy table).
*/
extern bool dd_table_options_has_fba(const dd::Properties *options);

/**
  Instantiate table related metadata about flashback area
  @param[in]      table_policy  table config info
  @param[in]      table         InnoDB table definition, dict_table_t to fill
*/
extern void dd_table_fill_fba(const komodo::Table_policy &table_policy,
                              dict_table_t *table);

/**
  Write lizard metadata of a table to dd::Table or dd::Partition_table for
  partition tables.

  @param[in]          table          innodb dict table
  @param[in,out]      dd_table       dd::table or dd::partition
*/
template <typename Table>
extern void dd_table_write_fba(const dict_table_t *table, Table *dd_table);

/**
  Copy the flashback area option from an old dd table to a new dd table.

  @param[in]          old_dd_tab     old dd::table or dd::partition
  @param[in,out]      new_dd_tab     new dd::table or dd::partition
*/
template <typename Table>
extern void dd_table_copy_fba(const Table &old_dd_tab, Table &new_dd_tab);

/**
  Check whether the flashback area option is consistent between the DD object
  and the InnoDB table object.

  @param[in,out]  table             Innodb table object
  @param[in]      dd_table          dd::table

  @return `true` if the flashback area options are consistent, `false`
  otherwise.
*/
extern bool dd_table_check_fba(const dict_table_t *table,
                               const dd::Table &dd_table);

/**
  Exchange flashback area option between Partition table and Swap table.
  @param[in,out]   part_dd_options  dd options from dd::Partition_table
  @param[in,out]   swap_dd_options  dd options from dd::table
*/
extern void dd_table_exchange_fba(dd::Properties &part_dd_options,
                                  dd::Properties &swap_dd_options);

/** Retrieve root page type from dd index se private data.
 *
 * @param[in]	dict index
 * @param[in]	se private data
 *
 * @retval	root page type. */
extern page_type_t dd_index_get_page_type(
    const dict_index_t *index, const dd::Properties &se_private_data);

/** Retrieve page dictionary capacity from dd index se private data.
 *
 * @param[in]	dict index
 * @param[in]	se private data
 *
 * @retval	page dictionary capacity, 0 if it is not stored, as for an
 * index that is not prefix compressed. */
extern uint16_t dd_index_get_pd_capacity(const dict_index_t *index,
                                         const dd::Properties &se_private_data);

/**
  Validate IFT option from dd::Index or dd::Partition_index is equal to infos in
  dict_index_t.

  @param[in]      dd_options  dd_options from dd::Index or dd::Partition_index
  @param[in]      index       InnoDB index object
  @return false if totally equal, true if not equal.
*/
extern bool dd_index_format_match_check(const dd::Properties &options,
                                        const dict_index_t *index);

}  // namespace lizard

#endif  // lizard0dict0dd_h
