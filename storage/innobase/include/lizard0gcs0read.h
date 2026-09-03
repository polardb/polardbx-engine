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

/** @file include/lizard0gcs0read.h
 Lizard vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#ifndef lizard0gcs0read_h
#define lizard0gcs0read_h

#include "univ.i"

#include "lizard0gcs0read0types.h"

/** How many lists in MVCC */
#define MVCC_N_LISTS 32

#ifdef UNIV_PFS_MUTEX
/* MVCC list mutex key */
extern mysql_pfs_key_t mvcc_list_mutex_key;
#endif

namespace lizard {

/** Whether to use commit snapshot history to search
 * a suitable up_limit_id to decide sees on secondary index */

/** Only verify commit snapshot correct. */
extern bool srv_vision_use_commit_snapshot_debug;

class MVCC {
  typedef UT_LIST_BASE_NODE_T(Vision, list) vision_list_t;

  class VisionList {
   public:
    VisionList();

    ~VisionList();

    /**
      Add an element

      @param[in]		the element will be added
    */
    inline void add_element(Vision *vision);

    /**
      Remove an element

      @param[in]		the element will be released
    */
    inline void remove_element(Vision *vision);

    /**
      Get the first element scn of the list,
      must hold the mutex latch.

      @retval   the first element of the list
    */
    inline scn_t first_element_scn();

   private:
    /** mutex to protect the list */
    VisionListMutex m_mutex;
    /** Active vision, not like MVCC::m_vies, it's impossible that
    closed visions is in the list. */
    vision_list_t m_vision_list;
  };

 public:
  explicit MVCC(ulint _n_lists);

  /** Open a new vision
   *
   * @param[in/out]		vision
   * @param[in]			transaction creator */
  void vision_open(Vision *vision, trx_id_t creator_trx_id);
  /**
    Release the corresponding element.

    @param[in]		the element will be released
  */
  void vision_release(Vision *vision);
  /**
    Get the earliest vision, return newest scn if not exist.

    @retval	earliest vision snapshot scn.
  */
  scn_t clone_oldest_vision();

  /**
    Get total size of all active vision

    @retval       total size of all active vision
  */
  ulint size() const;

  bool inited() const { return m_inited; }

 private:
  /** Disable copy */
  MVCC(MVCC const &) = delete;
  MVCC(MVCC const &&) = delete;
  void operator=(MVCC const &) = delete;

 private:
  /** How many partitioned lists */
  ulint m_n_lists;

  /** Index of VisionList iterator, increment atomically */
  std::atomic<ulint> m_counter;

  /** Size of all active visions. It will be inc/dec atomically. */
  std::atomic<ulint> m_size;

  /** All pratitioned lists */
  std::vector<VisionList> m_lists;

  /** All the list has been created */
  bool m_inited;
};

}  // namespace lizard

#endif
