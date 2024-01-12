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

/** @file include/lizard0read0read.h
 Lizard vision

 Created 3/30/2020 zanye.zjy
 *******************************************************/

#ifndef lizard0read0read_h
#define lizard0read0read_h

#include "lizard0read0types.h"

/** How many lists in VisionContainer */
#define VISION_CONTAINER_N_LISTS 32

#ifdef UNIV_PFS_MUTEX
/* Vision container list mutex key */
extern mysql_pfs_key_t lizard_vision_list_mutex_key;
#endif

namespace lizard {

/** Whether to use commit snapshot history to search
 * a suitable up_limit_id to decide sees on secondary index */

/** Only verify commit snapshot correct. */
extern bool srv_vision_use_commit_snapshot_debug;

class VisionContainer {
  typedef UT_LIST_BASE_NODE_T(Vision, list) vision_list_t;

  class VisionList {
   public:
    VisionList();

    ~VisionList();

    /**
      Add a element

      @retval		a new empty vision obj
      @deprecated    use add_element instead
    */
    inline Vision *new_element();

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
  explicit VisionContainer(ulint _n_lists);

  /**
    Add a element.

    @param[in]		the transaction to assign vision
  */
  void vision_open(trx_t *trx);

  /**
    Release the corresponding element.

    @param[in]		the element will be released
  */
  void vision_release(Vision *vision);

  /**
    Get the earliest undestructed element.

    @param[out]		the earliest entry undestructed element
  */
  void clone_oldest_vision(Vision *vision);

  /**
    Get total size of all active vision

    @retval       total size of all active vision
  */
  ulint size() const;

  bool inited() const { return m_inited; }
  void init() { m_inited = true; }

 private:
  /** Disable copy */
  VisionContainer(VisionContainer const &) = delete;
  VisionContainer(VisionContainer const &&) = delete;
  void operator=(VisionContainer const &) = delete;

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

/** Global visions */
extern VisionContainer *vision_container;

/**
  Assign vision for an active trancaction.

  @param[in]		the transaction to assign vision
*/
void trx_vision_open(trx_t *trx);

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
void trx_vision_release(Vision *vision);

/**
  Get the earliest undestructed element.

  @param[out]		the earliest entry undestructed element
*/
void trx_clone_oldest_vision(Vision *vision);

/**
  Get total size of all active vision

  @retval       total size of all active vision
*/
ulint trx_vision_container_size();

/** New and init the vision_container */
void trx_vision_container_init();

/** Destroy the vision_container */
void trx_vision_container_destroy();

}  // namespace lizard

#endif
