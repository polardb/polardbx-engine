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

/** @file dict/lizard0dict0dd.cc
 Special Lizard data dictionary interface.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#include "data0data.h"
#include "dict0dd.h"
#include "dict0mem.h"
#include "my_dbug.h"
#include "row0mysql.h"

#include "lizard0dict0dd.h"
#include "lizard0gcs0read0types.h"
#include "lizard0txn.h"
#include "lizard0txn0rec.h"
#include "lizard0undo.h"

/** Interpret index record (special for mysql.innodb_dynamic_metadata).
 *
 * @param[in]		record
 * @param[in]		index
 * @param[in]		offsets size to allocate.
 * @param[in]		index fields.
 *
 * @retval		contigous record slice. (only for special table). */
DSlice *DD_slice_ctx::get_offsets(creg_t reg, const dict_index_t *index,
                                  ulint n_alloc, ulint n_fields) {
  ut_ad(!rec_get_spc_flag(reg.rec, index));
  ut_ad(n_alloc >= n_fields);

  if (UNIV_UNLIKELY(rec_offs_get_n_alloc(m_offsets) < n_alloc)) {
    if (UNIV_LIKELY(!m_heap)) {
      m_heap = mem_heap_create(n_alloc * sizeof(ulint), UT_LOCATION_HERE);
    }
    m_offsets =
        static_cast<ulint *>(mem_heap_alloc(m_heap, n_alloc * sizeof(ulint)));
    rec_offs_set_n_alloc(m_offsets, n_alloc);
  }
  rec_offs_set_n_fields(m_offsets, n_fields);

  rec_init_offsets_comp_ordinary(reg.rec, false, index, m_offsets);

  m_dslc.set_offsets(reg, m_offsets, index);
  return &m_dslc;
}

/** Resolve the real SCN/GCN of the index's transaction source into a
txn_rec_t. If the source is still active, its real state is looked up from
the undo log under dict_sys->mutex and cached back into the selected
txn_index_t (double-checked locking). This is NOT a cheap getter.

Defined here rather than in lizard0dict0mem.cc because that file is also part
of the minimal innodb_zipdecompress build, which cannot include the heavy
txn/undo headers this resolution needs.

@param[in]  use_cross_ddl  true to use the creator source
(creator_trx_id / creator_txn, the per-index AS OF anchor), false to use
the runtime source (trx_id / txn, the legacy uniform path).
@return     resolved txn_rec_t with real scn/gcn values */
txn_rec_t dict_index_t::resolve_txn(bool use_cross_ddl) {
  trx_id_t src_tid = 0;
  txn_index_t *src_txn = nullptr;
  txn_rec_t rec;
  /* Select the txn source. The (trx_id, txn_index) pairing is an invariant
  owned here: creator_trx_id goes with creator_txn, trx_id with txn. Local
  names are kept distinct from the members to avoid confusion. */
  src_tid = use_cross_ddl ? creator_trx_id : trx_id;
  src_txn = use_cross_ddl ? &creator_txn : &txn;

  /* Seed the record from the cached source; scn/gcn are unknown for now. */
  rec = {src_tid, SCN_NULL, src_txn->uba.load(), GCN_NULL};

  if (undo_addr_t::decode_active(rec.undo_ptr)) {
    /* The cached uba still points at an active slot, so the real commit
    state is not cached yet. Serialize resolution under dict_sys->mutex. */
    mutex_enter(&dict_sys->mutex);

    if (!undo_addr_t::decode_active(src_txn->uba.load())) {
      /* Another thread resolved and cached it while we waited for the
      mutex; just read the cached commit state back. */
      rec.scn = src_txn->scn.load();
      rec.gcn = src_txn->gcn.load();
      rec.undo_ptr = src_txn->uba.load();
      mutex_exit(&dict_sys->mutex);
      ut_ad(rec.is_whole_committed());
    } else {
      /* We are the first to resolve: look up the real scn/gcn from the undo
      log and cache them back into the source so later calls hit the fast
      path above. */
      lizard::txn_rec_real_state(&rec, Cache_hint::KEEP_OLD, ccr_t::CCR_ALL);
      src_txn->scn.store(rec.scn);
      src_txn->gcn.store(rec.gcn);
      ut_ad(undo_addr_t::decode_slot(src_txn->uba.load()) ==
            undo_addr_t::decode_slot(rec.undo_ptr));
      src_txn->uba.store(rec.undo_ptr);
      mutex_exit(&dict_sys->mutex);
    }
  } else {
    /* The cached uba is already committed, no undo lookup or lock needed. */
    ut_ad(src_txn->is_whole_committed());
    rec.scn = src_txn->scn.load();
    rec.gcn = src_txn->gcn.load();
    ut_ad(rec.is_whole_committed());
  }

  return rec;
}

namespace lizard {

/**
  Init txn_desc with the creator trx when created.

  @param[in]      index       the index being created
  @param[in]      trx         creator transaction
  @return         DB_SUCCESS  Success
*/
dberr_t dd_index_init_txn_desc(dict_index_t *index, trx_t *trx) {
  dberr_t err = DB_SUCCESS;
  ut_ad(index->table);
  ut_ad(!mutex_own(&trx->undo_mutex));

  /** If a table is temporary, or even intrinsic, its dict info will not
  be written to dict tables. */
  if (!index->table->is_temporary()) {
    mutex_enter(&trx->undo_mutex);
    err = trx_always_assign_txn_slot(trx);
    mutex_exit(&trx->undo_mutex);

    if (err == DB_SUCCESS) {
      ut_ad(trx->txn_desc.is_cmmt_initial());
      ut_ad(trx->txn_desc.is_slot_alloced());
      ut_ad(!trx->txn_desc.is_slot_committed());

      index->txn.copy_from_txn_desc(trx->txn_desc);
      index->creator_txn.copy_from_txn_desc(trx->txn_desc);
    }
  }

  return err;
}

/**
  Check if a index is visible to an AS OF (flashback) snapshot.

  @param[in]      index             the index being opened.
  @param[in]      snapshot_vision   AS OF snapshot vision
  @return         true if visible
*/
static bool dd_index_visible_by_snapshot(
    dict_index_t *index, const komodo::Snapshot_vision *snapshot_vision) {
  bool use_cross_ddl = false;
  ut_ad(snapshot_vision);

  use_cross_ddl = snapshot_vision->is_cross_ddl();
  /* Clustered index visibility under cross_ddl is decided by row versions
  later. A rebuild boundary is reported there as the same unified AS OF
  definition error. */
  if (use_cross_ddl && index->is_clustered()) {
    return true;
  }
  /* Resolve the AS OF txn source:
     - cross_ddl ON  : creator_* (per-index AS OF anchor)
     - cross_ddl OFF : trx_id / txn (legacy uniform path) */
  txn_rec_t rec_txn = index->resolve_txn(use_cross_ddl);
  return snapshot_vision->modification_visible(&rec_txn);
}

/**
  Check if a index is visible to the read view of a normal (non AS OF)
  transaction.

  @param[in]      index       the index being opened.
  @param[in]      trx         transaction.
  @return         true if visible
*/
static bool dd_index_visible_by_trx_vision(dict_index_t *index,
                                           const trx_t *trx) {
  ut_ad(trx);
  ut_ad(!trx->vision.is_active() || !trx->vision.is_asof());
  /* Legacy uniform path: resolve the runtime txn source (trx_id / txn). */
  txn_rec_t rec_txn = index->resolve_txn(false);
  /* check_consistent=false: concurrent is_usable() can read SCN/UBA
     from separate atomics without a lock, so they may be inconsistent. */
  return (!trx->vision.is_active() || trx->vision.modifications_visible(
                                          &rec_txn, index->table->name, false));
}
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
    const komodo::Snapshot_vision *snapshot_vision) {
  ut_ad(trx);
  return snapshot_vision ? dd_index_visible_by_snapshot(index, snapshot_vision)
                         : dd_index_visible_by_trx_vision(index, trx);
}

/**
  Fill index txn information from se_private_data.

  @param[in,out]  index       the index being opened.
  @param[in]      p           se_private_data from the mysql.indexes record.
  @return         true if failed
*/
bool dd_index_fill_txn_desc(dict_index_t *index, const dd::Properties &p) {
  undo_ptr_t uba = 0;
  scn_t scn = SCN_NULL;
  gcn_t gcn = GCN_NULL;

  if (p.get(dd_index_key_strings[DD_INDEX_UBA], &uba) ||
      p.get(dd_index_key_strings[DD_INDEX_SCN], &scn))
    return true;

  /** GCN didn't add into properties first time, so judge it firstly. */
  if (p.exists(dd_index_key_strings[DD_INDEX_GCN])) {
    p.get(dd_index_key_strings[DD_INDEX_GCN], &gcn);
  }

  index->txn.uba = uba;
  index->txn.scn = scn;
  index->txn.gcn = gcn;

  /** Creator metadata is a stable four-tuple. trx_id/uba may
  be all that is known before the DDL commits; scn/gcn are filled after
  resolution and must survive later DD write-back/restart. */
  undo_ptr_t creator_uba = UNDO_PTR_NULL;
  scn_t creator_scn = SCN_NULL;
  gcn_t creator_gcn = GCN_NULL;

  if (p.exists(dd_index_key_strings[DD_INDEX_CREATOR_TRX_ID])) {
    if (p.get(dd_index_key_strings[DD_INDEX_CREATOR_TRX_ID],
              &index->creator_trx_id) ||
        p.get(dd_index_key_strings[DD_INDEX_CREATOR_UBA], &creator_uba) ||
        p.get(dd_index_key_strings[DD_INDEX_CREATOR_SCN], &creator_scn) ||
        p.get(dd_index_key_strings[DD_INDEX_CREATOR_GCN], &creator_gcn)) {
      return true;
    }
  } else {
    index->creator_trx_id = index->trx_id;
    creator_uba = index->txn.uba.load();
    creator_scn = index->txn.scn.load();
    creator_gcn = index->txn.gcn.load();
  }

  index->creator_txn.uba = creator_uba;
  index->creator_txn.scn = creator_scn;
  index->creator_txn.gcn = creator_gcn;

  return false;
}

/**
  Add the lizard columns into data dictionary in server

  @param[in,out]	dd_table	data dictionary cache object
  @param[in,out]  primary   data dictionary primary key
*/
void dd_table_add_txn_columns(dd::Table *dd_table, dd::Index *primary) {
  ut_ad(dd_table && primary);

  dd::Column *db_scn_id = dd_add_hidden_column(
      dd_table, "DB_SCN_ID", DATA_SCN_ID_LEN, dd::enum_column_types::LONGLONG);
  dd_add_hidden_element(primary, db_scn_id);

  dd::Column *db_undo_ptr =
      dd_add_hidden_column(dd_table, "DB_UNDO_PTR", DATA_UNDO_PTR_LEN,
                           dd::enum_column_types::LONGLONG);
  dd_add_hidden_element(primary, db_undo_ptr);

  dd::Column *db_gcn_id = dd_add_hidden_column(
      dd_table, "DB_GCN_ID", DATA_GCN_ID_LEN, dd::enum_column_types::LONGLONG);

  dd_add_hidden_element(primary, db_gcn_id);
}

/**
  Instantiate index related metadata about GPP...
  @param[in]      index_policy                 index config info about GPP
  @param[in]      table                        InnoDB table definition
  @param[in,out]  index                        dict_index_t to fill
  @param[out]     expected_or_real_page_type   exptected page type of
  dict_index_t that we want to create, or the real page type in restored
*/
void dd_index_fill_format(const komodo::Index_policy &index_policy,
                          const dict_table_t *table, dict_index_t *index,
                          page_type_t *expected_or_real_page_type) {
  /** Promise only fill once. */
  ut_a(index->is_gstored() == false);
  ut_a(index->n_s_gfields == 0);
  ut_a(index_policy.inited());

  if (index_policy.has_gpp()) {
    ut_ad(!table->is_compressed());
    ut_ad(!table->is_temporary());
    ut_ad((DBUG_EVALUATE_IF("allow_dd_tables_have_gpp", true,
                           !table->is_system_table)));
    ut_ad(!table->is_intrinsic());

    ut_ad(!(index->type & DICT_IBUF));
    ut_ad(!(index->type & DICT_SDI));
    ut_ad(!dict_index_is_spatial(index));
    ut_ad(!index->is_clustered());
  }

  index->set_gstored(index_policy.has_gpp());

  if (index_policy.page_type() == FIL_PAGE_INDEX_PANDA) {
    ut_ad(!table->is_system_table);
  }

  *expected_or_real_page_type = index_policy.page_type();

#ifdef UNIV_DEBUG
  /* Validate root page type. */
  switch (*expected_or_real_page_type) {
    case FIL_PAGE_INDEX_PANDA:
      ut_ad(!index->is_clustered());
      ut_ad(dict_index_is_unique(index));
      break;
    case FIL_PAGE_INDEX_HIPPO:
      ut_ad(!index->is_clustered());
      break;
    case FIL_PAGE_INDEX_SPC:
      ut_ad(!dict_index_is_unique(index));
      ut_ad(dict_table_is_comp(table));
      ut_ad(!table->is_compressed());
      ut_ad(!table->is_temporary());
      ut_ad(!table->is_intrinsic());
      break;
    case FIL_PAGE_TYPE_UNUSED:
      break;
    default:
      ut_ad(0);
  }
#endif
}

/**
  Write lizard metadata of a index to dd::Index for regular table or
  dd::Partition_index for partition tables.

  @param[in,out]  options     dd options which might carry GPP info
  @param[in]      index       InnoDB index object
*/
void dd_index_write_format(dd::Properties *options, const dict_index_t *index) {
  ulonglong format = 0;

  /* ut_a(!dd_index_options_has_ift(options)); */

  if (index->is_gstored()) {
    format |= komodo::IFT_GPP;
  }

  options->set(komodo::OPTION_IFT, format);
}

/**
  Copy lizard metadata of a index to dd::Index for regular table or
  dd::Partition_index for partition tables.

  @param[out]     new_dd_options  new dd options to be modified.
  @param[in]      old_dd_options  old dd options from DD
*/
void dd_index_copy_format(dd::Properties &new_dd_options,
                          const dd::Properties &old_dd_options) {
  ulonglong format = 0;

  ut_a(!new_dd_options.exists(komodo::OPTION_IFT));

  if (!old_dd_options.exists(komodo::OPTION_IFT)) {
    return;
  }

  old_dd_options.get(komodo::OPTION_IFT, &format);
  new_dd_options.set(komodo::OPTION_IFT, format);
}

/**
  Exchange IFT option between Partition Index and Swap Index.
  @param[in,out]   part_dd_options  dd options from dd::Partition_index
  @param[in,out]   swap_dd_options  dd options from dd::Index from Swap Table
*/
void dd_index_exchange_format(dd::Properties &part_dd_options,
                              dd::Properties &swap_dd_options) {
  ulonglong part_format = 0;
  ulonglong swap_format = 0;
  if (part_dd_options.exists(komodo::OPTION_IFT)) {
    part_dd_options.get(komodo::OPTION_IFT, &part_format);
  }
  if (swap_dd_options.exists(komodo::OPTION_IFT)) {
    swap_dd_options.get(komodo::OPTION_IFT, &swap_format);
  }
  part_dd_options.set(komodo::OPTION_IFT, swap_format);
  swap_dd_options.set(komodo::OPTION_IFT, part_format);
}

/**
  Judge whether the flashback area option is turned on in dd options.

  @param[in]      options     dd options from dd::Table or dd::Partition_table
  @return         true if the flashback area option exists and is turned on,
  false if it is turned off or absent (as for a legacy table).
*/
bool dd_table_options_has_fba(const dd::Properties *options) {
  bool fba = false;

  if (options->exists(komodo::TABLE_OPTION_FBA)) {
    options->get(komodo::TABLE_OPTION_FBA, &fba);
    return fba;
  } else {
    return false;
  }
}

/**
  Instantiate table related metadata about flashback area
  @param[in]      table_policy  table config info
  @param[in]      table         InnoDB table definition, dict_table_t to fill
*/
void dd_table_fill_fba(const komodo::Table_policy &table_policy,
                       dict_table_t *table) {
  /** Promise only fill once. */
  ut_a(table->is_2pp == false);
  ut_a(table_policy.inited());

  if (table_policy.has_fba()) {
    ut_ad(!table->is_temporary());
    ut_ad(!table->is_system_table);
    ut_ad(!table->is_intrinsic());
  }

  table->is_2pp = table_policy.has_fba();
}

/**
  Write lizard metadata of a table to dd::Table or dd::Partition_table for
  partition tables.

  @param[in]          table          innodb dict table
  @param[in,out]      dd_table       dd::table or dd::partition
*/
template <typename Table>
void dd_table_write_fba(const dict_table_t *table, Table *dd_table) {
  if (!dd_table_is_partitioned(dd_table->table()) ||
      dd_part_is_first(reinterpret_cast<dd::Partition *>(dd_table))) {
    dd_table->table().options().set(komodo::TABLE_OPTION_FBA, table->is_2pp);
  }
}

template void dd_table_write_fba<dd::Table>(const dict_table_t *table,
                                            dd::Table *dd_table);

template void dd_table_write_fba<dd::Partition>(const dict_table_t *table,
                                                dd::Partition *dd_table);

/**
  Copy the flashback area option from an old dd table to a new dd table.

  @param[in]          old_dd_tab     old dd::table or dd::partition
  @param[in,out]      new_dd_tab     new dd::table or dd::partition
*/
template <typename Table>
void dd_table_copy_fba(const Table &old_dd_tab, Table &new_dd_tab) {
  if (!dd_table_is_partitioned(new_dd_tab.table()) ||
      dd_part_is_first(reinterpret_cast<dd::Partition *>(&new_dd_tab))) {
    new_dd_tab.table().options().set(
        komodo::TABLE_OPTION_FBA,
        dd_table_options_has_fba(&old_dd_tab.table().options()));
  }
}

template void dd_table_copy_fba<dd::Table>(const dd::Table &old_dd_tab,
                                           dd::Table &new_dd_tab);

template void dd_table_copy_fba<dd::Partition>(const dd::Partition &old_dd_tab,
                                               dd::Partition &new_dd_tab);

/**
  Check whether the flashback area option is consistent between the DD object
  and the InnoDB table object.

  @param[in]      table             Innodb table object
  @param[in]      dd_table          dd::table

  @return `true` if the flashback area options are consistent, `false`
  otherwise.
*/
bool dd_table_check_fba(const dict_table_t *table, const dd::Table &dd_table) {
  return (table->is_2pp == dd_table_options_has_fba(&dd_table.options()));
}

/**
  Exchange flashback area option between Partition table and Swap table.
  @param[in,out]   part_dd_options  dd options from dd::Partition_table
  @param[in,out]   swap_dd_options  dd options from dd::table
*/
void dd_table_exchange_fba(dd::Properties &part_dd_options,
                           dd::Properties &swap_dd_options) {
  bool part_fba = false;
  bool swap_fba = false;
  if (part_dd_options.exists(komodo::TABLE_OPTION_FBA)) {
    part_dd_options.get(komodo::TABLE_OPTION_FBA, &part_fba);
  }

  if (swap_dd_options.exists(komodo::TABLE_OPTION_FBA)) {
    swap_dd_options.get(komodo::TABLE_OPTION_FBA, &swap_fba);
  }

  part_dd_options.set(komodo::TABLE_OPTION_FBA, swap_fba);
  swap_dd_options.set(komodo::TABLE_OPTION_FBA, part_fba);
}

/** Retrieve root page type from dd index se private data.
 *
 * @param[in]	dict index
 * @param[in]	se private data
 *
 * @retval	root page type. */
page_type_t dd_index_get_page_type(const dict_index_t *index,
                                   const dd::Properties &se_private_data) {
  page_type_t page_type;
  const char *key;

  page_type = dict_index_legacy_ptype(index);
  key = dd_index_key_strings[DD_INDEX_PAGE_TYPE];

  if (se_private_data.exists(key)) {
    se_private_data.get(key, &page_type);
  }

  return page_type;
}

/** Retrieve page dictionary capacity from dd index se private data.
 *
 * @param[in]	dict index
 * @param[in]	se private data
 *
 * @retval	page dictionary capacity, 0 if it is not stored, as for an
 * index that is not prefix compressed. */
uint16_t dd_index_get_pd_capacity(const dict_index_t *index,
                                  const dd::Properties &se_private_data) {
  uint16_t pd_capacity = 0;
  const char *key;
  key = dd_index_key_strings[DD_INDEX_PAGE_DICT_CAPACITY];
  if (se_private_data.exists(key)) {
    se_private_data.get(key, &pd_capacity);
  }
  return pd_capacity;
}

/**
  Validate IFT option from dd::Index or dd::Partition_index is equal to infos in
  dict_index_t.

  @param[in]      dd_options  dd_options from dd::Index or dd::Partition_index
  @param[in]      index       InnoDB index object
  @return false if totally equal, true if not equal.
*/
bool dd_index_format_match_check(const dd::Properties &options,
                                 const dict_index_t *index) {
  ulonglong format = 0;
  if (options.exists(komodo::OPTION_IFT)) {
    options.get(komodo::OPTION_IFT, &format);
  }
  /* 1. komodo::IFT_GPP */
  if (((format & komodo::IFT_GPP) && !index->is_gstored()) ||
      (!(format & komodo::IFT_GPP) && index->is_gstored())) {
    return true;
  }
  /* 2. TODO */
  return false;
}

}  // namespace lizard
