/*****************************************************************************
Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0read0types.h
  Lizard Vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#ifndef lizard0read0types_h
#define lizard0read0types_h

#include <algorithm>

#include "ut0lst.h"
#include "ut0mutex.h"

#include "lizard0scn0types.h"
#include "lizard0undo0types.h"
#include "trx0types.h"

namespace lizard {

/** SCN print format */
#define TRX_SCN_FMT IB_ID_FMT

/** Vision list uninfinite size */
constexpr ulint VISION_LIST_IDX_NULL = std::numeric_limits<ulint>::max();

/** Vision list mutex type */
typedef ib_mutex_t VisionListMutex;

class Vision {
 public:
  explicit Vision();
  virtual ~Vision() {}

 private:
  /* Disable copying */
  Vision(const Vision &) = delete;
  Vision(const Vision &&) = delete;
  Vision &operator=(const Vision &) = delete;

  /**
    Return scn of the vision
    @retval   scn of the vision
  */
  scn_t snapshot_scn() const { return m_snapshot_scn; }
  /**
    Write the limits to the file.

    @param     file		file to write to */
  void print_limits(FILE *file) const {
    fprintf(file,
            "Trx vision will not see trx with"
            " scn >= " TRX_SCN_FMT "\n",
            m_snapshot_scn);
  }

  /** Check whether the changes by id are visible.
  @param[in]	trx_zeus
  @return whether the view sees the modifications of id. True if visible */
  bool modifications_visible(txn_rec_t *txn_info) const
      MY_ATTRIBUTE((warn_unused_result));

  /**
    Whether Vision can see the target trx id,
    if the target trx id is less than the least
    active trx, then it will see.

    @param       id		transaction to check
    @retval      true  if view sees transaction id
  */
  bool sees(trx_id_t id) const {
    ut_ad(id < TRX_ID_MAX && m_creator_trx_id < TRX_ID_MAX);
    ut_ad(m_list_idx != VISION_LIST_IDX_NULL);
    return id < m_up_limit_id;
  }

#ifdef UNIV_DEBUG
  /**
    Less or equal compare
    @param    rhs		  view to compare with
    @retval   true    if this view is less than or equal rhs
  */
  bool le(const Vision *rhs) const {
    return (m_snapshot_scn <= rhs->m_snapshot_scn);
  }
#endif /* UNIV_DEBUG */

 private:
  /** All RW transactions that SCN number is less than m_snapshot_scn
  have been commited */
  scn_t m_snapshot_scn;

  /** The index of the list that it belongs to, used to find which
  mutex should be holded. */
  ulint m_list_idx;

  /** The creator of the view, and is used to determine visibility */
  trx_id_t m_creator_trx_id;

  /** All trx whose trx_id is smaller than m_up_limit_id is seen. Only used
  by secondary index */
  trx_id_t m_up_limit_id;

  /** Whether in vision list. */
  bool m_in_list;

  UT_LIST_NODE_T(Vision) list;

  friend class VisionContainer;
};

}  // namespace lizard

#endif
