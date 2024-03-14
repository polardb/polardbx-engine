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

/** @file sample/lizard0sample.cc
  Lizard Sample.

 Created 2023-10-13 by Jiyang.zhang
 *******************************************************/
#include <functional>

#include "lizard0sample.h"
#include "raii/sentry.h"

namespace lizard {

std::uniform_real_distribution<double> Sampler::m_distribution(0, 100);

template <class Functor>
class RAII {
 public:
  RAII(Functor &func) : m_func(func) {}
  ~RAII() { m_func(); }

 private:
  Functor &m_func;
};

/*******************************************************
 *              Base Class: Sampler
 ********************************************************/
Sampler::Sampler(int sampling_seed, double sampling_percentage,
                 enum_sampling_method sampling_method)
    : m_random_generator(sampling_seed),
      m_sampling_method(sampling_method),
      m_sampling_percentage(sampling_percentage),
      m_sampling_seed(sampling_seed),
      m_backup_isolation_level(TRX_ISO_READ_UNCOMMITTED),
      m_index(nullptr),
      m_trx(nullptr),
      m_prebuilt(nullptr),
      m_inited(false),
      m_n_sampled(0) {
  ut_ad(sampling_method == enum_sampling_method::USER);
}

dberr_t Sampler::init(trx_t *trx, dict_index_t *index,
                      row_prebuilt_t *prebuilt) {
  ut_a(trx);
  m_index = index;
  m_trx = trx;
  m_prebuilt = prebuilt;

  /** Backup isolation level. */
  m_backup_isolation_level = m_trx->isolation_level;
  m_trx->isolation_level = TRX_ISO_READ_UNCOMMITTED;

  open_at_index_left_side();

  m_inited = true;

  return DB_SUCCESS;
}

bool Sampler::skip() {
  if (m_sampling_percentage == 0.00) {
    return (true);
  } else if (m_sampling_percentage == 100.00) {
    return (false);
  }

  bool ret = false;

  switch (m_sampling_method) {
    case enum_sampling_method::USER: {
      double rand = m_distribution(m_random_generator);

      DBUG_PRINT("histogram_sampler_buffering_print",
                 ("-> New page. Random value generated - %lf", rand));

      /* Check if the records in the block needs to be read for sampling. */
      if (rand > m_sampling_percentage) {
        ret = true;
      }
    } break;

    default:
      ut_d(ut_error);
      break;
  }

  return (ret);
}

/*******************************************************
 *                   RecordSampler                      *
 ********************************************************/
class RecordSampler : public Sampler {
 public:
  explicit RecordSampler(int sampling_seed, double sampling_percentage,
                         enum_sampling_method sampling_method);

  /** Destructor. */
  ~RecordSampler() override { m_leaf_pcur.close(); }

  virtual dberr_t next(uchar *buf) override;

  virtual const std::string get_sample_mode() override { return "rec-mode"; }

 private:
  virtual void open_at_index_left_side() override;

  virtual void end() override {
    m_leaf_pcur.close();
    if (m_n_sampled != 0) {
      export_vars.innodb_polarx_rec_mode_sample_records += m_n_sampled;
    }
    Sampler::end();
  }

  btr_pcur_t m_leaf_pcur;
};

RecordSampler::RecordSampler(int sampling_seed, double sampling_percentage,
                             enum_sampling_method sampling_method)
    : Sampler(sampling_seed, sampling_percentage, sampling_method) {
  memset((void *)&m_leaf_pcur, 0, sizeof(btr_pcur_t));
  m_leaf_pcur.reset();
}

void RecordSampler::open_at_index_left_side() {
  mtr_t mtr;

  mtr_start(&mtr);
  m_leaf_pcur.open_at_side(true, m_index, BTR_SEARCH_LEAF, false, 0, &mtr);
  /* The posistion will be BTR_PCUR_BEFORE for the first record of the tree. */
  m_leaf_pcur.store_position(&mtr);

  mtr_commit(&mtr);
}

dberr_t RecordSampler::next(uchar *mysql_buf) {
  mtr_t mtr;
  dberr_t moved;
  const rec_t *rec;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  mem_heap_t *offset_heap = nullptr;

  rec_offs_init(offsets_);

  mtr_start(&mtr);

  raii::Sentry<> guard([this, &mtr, &offset_heap] {
    m_leaf_pcur.store_position(&mtr);
    if (offset_heap) {
      mem_heap_free(offset_heap);
    }
    mtr_commit(&mtr);
  });

  ut_a(m_leaf_pcur.m_old_stored);
  m_leaf_pcur.restore_position(BTR_SEARCH_LEAF, &mtr, UT_LOCATION_HERE);

next_rec:
  /** The key is to avoid the first record of the tree being skipped forever. */
  do {
    moved = m_leaf_pcur.move_to_next_user_rec(&mtr);
  } while (moved == DB_SUCCESS && skip());

  if (moved != DB_SUCCESS) {
    ut_a(m_leaf_pcur.is_after_last_in_tree(&mtr));
    return DB_END_OF_INDEX;
  }
  ut_ad(m_leaf_pcur.is_on_user_rec());

  rec = m_leaf_pcur.get_rec();
  offsets = rec_get_offsets(rec, m_index, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &offset_heap);

  if (!row_sel_store_mysql_rec(mysql_buf, m_prebuilt, rec, nullptr, true,
                               m_index, m_prebuilt->index, offsets, false,
                               nullptr, m_prebuilt->blob_heap)) {
    /** Currently, only the blob fields in the intermediate state are seen,
        will come into here */
    goto next_rec;
  }
  m_n_sampled++;

  return DB_SUCCESS;
}

class SamplePageCursor {
 public:
  SamplePageCursor()
      : m_cur(),
        m_block(nullptr),
        m_is_after_last_on_page(true),
        m_mtr(),
        m_index(nullptr),
        m_prebuilt(nullptr) {}

  ~SamplePageCursor() {
    /** Sometime the executor will directly call sample_end(), for example
    max_statement_time. */
    if (m_mtr.is_active()) {
      mtr_commit(&m_mtr);
    }
  }

  void init(dict_index_t *index, row_prebuilt_t *prebuilt) {
    m_index = index;
    m_prebuilt = prebuilt;
  }

  void open(btr_pcur_t *father) {
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *node_ptr_offsets = offsets_;

    mem_heap_t *heap = nullptr;

    rec_offs_init(offsets_);
    const rec_t *node_ptr = page_cur_get_rec(father->get_page_cur());
    node_ptr_offsets =
        rec_get_offsets(node_ptr, m_index, node_ptr_offsets, ULINT_UNDEFINED,
                        UT_LOCATION_HERE, &heap);

    ut_a(is_after_last_on_page());

    mtr_start(&m_mtr);

    m_block = btr_node_ptr_get_child(node_ptr, m_index, node_ptr_offsets,
                                     &m_mtr, RW_S_LATCH);

    page_cur_set_before_first(m_block, &m_cur);

    page_cur_move_to_next(&m_cur);

    /** At least an element. */
    ut_a(!page_cur_is_after_last(&m_cur));
    m_is_after_last_on_page = false;

    if (heap) {
      mem_heap_free(heap);
    }
  }

  bool fetch_next_rec(uchar *buf) {
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *offsets = offsets_;
    rec_offs_init(offsets_);
    mem_heap_t *heap = nullptr;

    const rec_t *rec = page_cur_get_rec(&m_cur);
    offsets = rec_get_offsets(rec, m_index, offsets, ULINT_UNDEFINED,
                              UT_LOCATION_HERE, &heap);

    bool success = row_sel_store_mysql_rec(
        buf, m_prebuilt, rec, nullptr, true, m_index, m_prebuilt->index,
        offsets, false, nullptr, m_prebuilt->blob_heap);

    page_cur_move_to_next(&m_cur);

    if (page_cur_is_after_last(&m_cur)) {
      exhausted_records();
    }

    if (heap) {
      mem_heap_free(heap);
    }

    return success;
  }

  bool is_after_last_on_page() const { return m_is_after_last_on_page; }

 private:
  void exhausted_records() {
    new (&m_cur) page_cur_t();
    m_block = nullptr;
    m_is_after_last_on_page = true;
    mtr_commit(&m_mtr);
  }

  page_cur_t m_cur;
  buf_block_t *m_block;
  bool m_is_after_last_on_page;
  mtr_t m_mtr;
  dict_index_t *m_index;
  row_prebuilt_t *m_prebuilt;
};

class SamplePCursorNonLeaf {
 public:
  SamplePCursorNonLeaf() : m_index(nullptr), m_is_after_last_in_tree(false) {
    memset((void *)&m_pcur, 0, sizeof(btr_pcur_t));
    m_pcur.reset();
  }

  ~SamplePCursorNonLeaf() { m_pcur.close(); }

  void init(ulint in_read_level, dict_index_t *index) {
    m_pcur.init(in_read_level);
    m_index = index;
  }

  void open_at_index_left_side() {
    mtr_t mtr;

    mtr_start(&mtr);

    raii::Sentry<> guard([&mtr] { mtr_commit(&mtr); });

    /** Latch SX on index to ensure that the height of the tree is at
    least 1. */
    mtr_sx_lock(dict_index_get_lock(m_index), &mtr, UT_LOCATION_HERE);

    if (btr_height_get(m_index, &mtr) < m_pcur.m_read_level) {
      /** Mark as exhausted. */
      exhausted_tree_records(nullptr);
      return;
    }
    m_pcur.open_at_side(true, m_index, BTR_SEARCH_TREE | BTR_ALREADY_S_LATCHED,
                        false, m_pcur.m_read_level, &mtr);

    /* The posistion will be BTR_PCUR_ON for the first record of the tree. */
    m_pcur.move_to_next_on_page();

    store_position(&mtr);

    m_is_after_last_in_tree = false;

    return;
  }

  void store_position(mtr_t *mtr) {
    m_pcur.store_position(mtr);

    ut_a(m_pcur.m_rel_pos == BTR_PCUR_ON || m_pcur.is_after_last_in_tree(mtr));
  }

  /** Restore branch position.
  @param[in]  mtr
  @return false if cannot restored because dwarf tree, or is
                btr_pcur_is_after_last_in_tree.
          true if restored successfully. */
  bool restore_position(mtr_t *mtr) {
    constexpr auto MODE = BTR_SEARCH_LEAF;
    const auto relative = m_pcur.m_rel_pos;

    ut_ad(mtr_memo_contains_flagged(mtr, dict_index_get_lock(m_index),
                                    MTR_MEMO_SX_LOCK));
    ut_ad(m_pcur.m_read_level >= 1);

    if (btr_height_get(m_index, mtr) < m_pcur.m_read_level) {
      return false;
    }
    bool equal = m_pcur.restore_position(MODE, mtr, UT_LOCATION_HERE);

#ifdef UNIV_DEBUG
    if (m_pcur.m_pos_state == BTR_PCUR_IS_POSITIONED_OPTIMISTIC) {
      ut_ad(m_pcur.m_rel_pos == BTR_PCUR_BEFORE ||
            m_pcur.m_rel_pos == BTR_PCUR_AFTER);
    } else {
      ut_ad(m_pcur.m_pos_state == BTR_PCUR_IS_POSITIONED);
      ut_ad((m_pcur.m_rel_pos == BTR_PCUR_ON) == m_pcur.is_on_user_rec());
    }
#endif /* UNIV_DEBUG */

    /** For now, the following conditions are met:
    1. the tree is at least two level
    2. the branch at least one user record.
    3. the cursor can only be BTR_PCUR_ON. */
    switch (relative) {
      case BTR_PCUR_ON:
        if (!equal) {
          m_pcur.move_to_next_user_rec(mtr);
        }
        break;

      case BTR_PCUR_AFTER:
      case BTR_PCUR_BEFORE_FIRST_IN_TREE:
      case BTR_PCUR_AFTER_LAST_IN_TREE:
      case BTR_PCUR_BEFORE:
        ut_error;
        break;
      case BTR_PCUR_UNSET:
        break;
    }
    return !m_pcur.is_after_last_in_tree(mtr);
  }

  dberr_t move_to_next_user_rec(mtr_t *mtr) {
    ut_ad(mtr_memo_contains_flagged(mtr, dict_index_get_lock(m_index),
                                    MTR_MEMO_SX_LOCK));
    return m_pcur.move_to_next_user_rec(mtr);
  }

  /** return true if there are no records left. */
  bool is_after_last_in_tree() const { return m_is_after_last_in_tree; }

  bool is_on_user_rec() const { return m_pcur.is_on_user_rec(); }

  btr_pcur_t *get_btr_pcur() noexcept { return &m_pcur; }

  void exhausted_tree_records(mtr_t *mtr) {
    if (mtr) {
      ut_a(m_pcur.is_after_last_in_tree(mtr));
    }
    m_is_after_last_in_tree = true;
  }

 private:
  dict_index_t *m_index;
  btr_pcur_t m_pcur;
  bool m_is_after_last_in_tree;
};

/*******************************************************
 *                   BlockSampler
 ********************************************************/
class BlockSampler : public Sampler {
 public:
  explicit BlockSampler(int sampling_seed, double sampling_percentage,
                        enum_sampling_method sampling_method);

  /** Destructor. */
  ~BlockSampler() override {}

  virtual dberr_t init(trx_t *trx, dict_index_t *index,
                       row_prebuilt_t *prebuilt) override;

  virtual dberr_t next(uchar *buf) override;

  virtual const std::string get_sample_mode() override { return "block-mode"; }

 private:
  virtual void end() override {
    if (m_n_sampled != 0) {
      export_vars.innodb_polarx_block_mode_sample_records += m_n_sampled;
    }
    Sampler::end();
  }

  virtual void open_at_index_left_side() override {
    m_node_pcur.open_at_index_left_side();
  }

  dberr_t fetch_next_leaf_node();

  SamplePageCursor m_leaf_cur;

  SamplePCursorNonLeaf m_node_pcur;
};

BlockSampler::BlockSampler(int sampling_seed, double sampling_percentage,
                           enum_sampling_method sampling_method)
    : Sampler(sampling_seed, sampling_percentage, sampling_method) {}

dberr_t BlockSampler::init(trx_t *trx, dict_index_t *index,
                           row_prebuilt_t *prebuilt) {
  m_leaf_cur.init(index, prebuilt);
  m_node_pcur.init(1, index);
  return Sampler::init(trx, index, prebuilt);
}

dberr_t BlockSampler::next(uchar *mysql_buf) {
  dberr_t err;
  mtr_t mtr;

  do {
    if (m_leaf_cur.is_after_last_on_page()) {
      if ((err = fetch_next_leaf_node()) != DB_SUCCESS) {
        return err;
      }
    }
  } while (!m_leaf_cur.fetch_next_rec(mysql_buf));

  m_n_sampled++;

  return DB_SUCCESS;
}

dberr_t BlockSampler::fetch_next_leaf_node() {
  dberr_t moved;
  mtr_t mtr;

  /** Pre. Check if have exhausted all tree records. */
  if (m_node_pcur.is_after_last_in_tree()) {
    return DB_END_OF_INDEX;
  }

  mtr_start(&mtr);

  raii::Sentry<> guard([this, &mtr] {
    m_node_pcur.store_position(&mtr);
    mtr_commit(&mtr);
  });

  /** 1. Latch SX on the index. */
  mtr_sx_lock(dict_index_get_lock(m_index), &mtr, UT_LOCATION_HERE);

  /** 2. restore position. */
  if (!m_node_pcur.restore_position(&mtr)) {
    /** Found a drawf tree, or is btr_pcur_is_after_last_in_tree.() */
    m_node_pcur.exhausted_tree_records(nullptr);
    return DB_END_OF_INDEX;
  }

  /** 3. Now position at a uncheck-skip and unopened user record. Check skip()
  and open the leaf cursor, and latch S on it in an independent mtr. */
  moved = DB_SUCCESS;
  while (moved == DB_SUCCESS && skip()) {
    moved = m_node_pcur.move_to_next_user_rec(&mtr);
  }

  if (m_node_pcur.is_on_user_rec()) {
    ut_ad(moved == DB_SUCCESS);
    m_leaf_cur.open(m_node_pcur.get_btr_pcur());

    /** 4. Move to next uncheck-skip and unopened user record. */
    moved = m_node_pcur.move_to_next_user_rec(&mtr);
    if (moved != DB_SUCCESS) {
      /** Mark as end so next time will return DB_END_OF_INDEX */
      ut_ad(!m_node_pcur.is_on_user_rec());
      m_node_pcur.exhausted_tree_records(&mtr);
    }

    return DB_SUCCESS;
  } else {
    ut_ad(moved == DB_END_OF_INDEX);
    return DB_END_OF_INDEX;
  }
}

Sampler *create_sampler(int sampling_seed, double sampling_percentage,
                        dict_index_t *index) {
  mtr_t mtr;
  mtr_start(&mtr);
  mtr_sx_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);

  /* Read pages from one level above the leaf page. */
  ulint read_level = btr_height_get(index, &mtr);

  mtr_commit(&mtr);

  DBUG_EXECUTE_IF("polarx_sample_force_rec_mode", read_level = 0;);
  DBUG_EXECUTE_IF("polarx_sample_force_block_mode", read_level = 1;);

  if (read_level >= 1) {
    return ut::new_withkey<BlockSampler>(UT_NEW_THIS_FILE_PSI_KEY,
                                         sampling_seed, sampling_percentage,
                                         enum_sampling_method::USER);
  } else {
    return ut::new_withkey<RecordSampler>(UT_NEW_THIS_FILE_PSI_KEY,
                                          sampling_seed, sampling_percentage,
                                          enum_sampling_method::USER);
  }
}

}  // namespace lizard
