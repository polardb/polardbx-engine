/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0trx0history.h
  Already committed trx history.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0trx0history_h
#define lizard0trx0history_h

#include "lizard0trx0erase0types.h"
#include "lizard0trx0purge0types.h"

#ifdef UNIV_PFS_MUTEX
/* cycle sys mutex PFS key */
extern mysql_pfs_key_t history_sys_mutex_key;
#endif

namespace lizard {

/** Heap for rseg history list. */
struct pe_heap_t {
 public:
  pe_heap_t() : purge_heap(nullptr), erase_heap(nullptr) {}

  pe_heap_t(purge_heap_t *purge_heap_arg, erase_heap_t *erase_heap_arg)
      : purge_heap(purge_heap_arg), erase_heap(erase_heap_arg) {}

  virtual ~pe_heap_t() {
    ut_ad(purge_heap == nullptr && erase_heap == nullptr);
  }

 public:
  purge_heap_t *purge_heap;
  erase_heap_t *erase_heap;

  void allocate_heap();

  void reset_heap() {
    purge_heap = nullptr;
    erase_heap = nullptr;
  }

  /** Add into cycle or purge heap when scan rseg->history list.*/
  void push_purge(trx_rseg_t *rseg, scn_t scn);

  size_t purge_size() { return purge_heap->size(); }
};

/** Entrance of cycle/purge/erase sys. */
struct trx_history_t {
  /** Set of all rseg queue when booting. */
  std::vector<trx_rseg_t *> rsegs_queue;

  /** heap for cycle/purge/erase. */
  pe_heap_t hp;

  /** Protect rsegs_queue if parallel scan undo when booting. */
  ib_mutex_t mutex;

 public:
  pe_heap_t *allocate_heap() {
    hp.allocate_heap();
    return &hp;
  }
};

/** History memory structure. */
extern trx_history_t *history_sys;

/** Entrance of cycle/purge/erase sys. */
extern void trx_history_sys_mem_create();

/** Initialize cycle/purge/erase system. */
extern void trx_history_sys_initialize(pe_heap_t *hp);

/** Close cycle/purge/erase system. */
extern void trx_history_sys_close();

/** Calculate rsegment status of undo tablespace.
 *
 * @param[in/out]	status array.
 * */
void trx_history_trunc_status(std::vector<komodo::trunc_status_t> &array);

void trx_history_purge_status(komodo::purge_status_t &status);

/** Initialize secondary list [free list / semi purge list] when booting. */
extern void trx_rseg_init_secondary_list(trx_rseg_t *rseg,
                                         trx_rsegf_t *rseg_hdr, pe_heap_t *hp,
                                         mtr_t *mtr);

extern void trx_rseg_init_history_len();
extern ulint trx_rseg_history_len();
extern void trx_rseg_incre_history_len(trx_rseg_t *rseg, ulint len);

}  // namespace lizard

#endif
