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

#ifndef btr0sample_h
#define btr0sample_h

#include "btr0pcur.h"

extern bool btr_sample_enabled;
extern ulint sample_advise_pages;

struct btr_sample_t {
  enum scan_mode_t { NO_SAMPLE, SAMPLE_BY_REC, SAMPLE_BY_BLOCK };

  row_prebuilt_t *prebuilt{nullptr};
  scan_mode_t scan_mode{NO_SAMPLE};
  bool enabled{false};

  void init(row_prebuilt_t *row_prebuilt);
  void enable();
  void reset();
  void on_switch_part(); /* Used in partition table */

  void open_curor(dict_index_t *index, mtr_t *mtr);
  bool restore_position(mtr_t *mtr);
  bool move_to_next(mtr_t *mtr);

  void decide_scan_mode(buf_block_t *root, ulint tree_height);
  bool sample_to_next_via_rec(mtr_t *mtr);
  bool sample_to_next_via_blk(mtr_t *mtr);
  bool sample_to_next_leaf_by_parent_pcur(mtr_t *mtr, bool &found);
  bool sample_to_next_leaf_by_leaf_pcur(mtr_t *mtr);
  bool restore_leaf_pcur(mtr_t *mtr);
  bool restore_pcur_optimistic(btr_pcur_t *pcur, bool leaf, mtr_t *mtr,
                               bool &succ);
  bool restore_leaf_pessimistic(mtr_t *mtr, bool hint_sampling);
  void search_to_leaf(const dtuple_t *tuple, page_cur_mode_t mode,
                      bool need_sampling, mtr_t *mtr);
  bool search_next_sampled_leaf(page_cur_t *page_cursor, mtr_t *mtr);
  void get_leaf_node_by_nodeptr(rec_t *node_ptr, bool at_left, mtr_t *mtr);
  bool skip() const;
};

#define btr_sample_pcur_open(sample, index, mtr) sample->open_curor(index, mtr)

#define btr_sample_pcur_restore(sample, mtr) sample->restore_position(mtr)

#define btr_sample_pcur_next(sample, mtr) sample->move_to_next(mtr)

#endif /* !btr0sample_h */
