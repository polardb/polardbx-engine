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

/** @file include/lizard0gcs0purge.h
 Global Change System clean module advanced by purge sys and erase sys.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gcs0clean_h
#define lizard0gcs0clean_h

#include "univ.i"

#include "lizard0cmmt0types.h"

namespace lizard {
/** GCS clean system which is advanced by purge sys or erase sys.
 * It will persist commit number which is purged or erased.*/
class CClean {
  /** Gcn number cleaner by purge_sys or erase_sys */
  class GcnCleaner {
   public:
    GcnCleaner(ulint poffset) : m_poffset(poffset), m_gcn(0), m_inited(false) {}

    virtual ~GcnCleaner() {}

    void init();
    /**
      Attention:

      If flush commit number > m_purged_xcn, it will flush commit number into
      lizard tablespace, but it didn't sync the redo of modification.  if
      related undo content has been purged, it mean that those undo's redo
      has been synced, because the redo of flushing commit number is prior of
      undo's redo. so it's unnecessary to sync it specially.
    */
    void advance(gcn_t gcn);

    gcn_t get() const;

   private:
    gcn_t read();

    void write(gcn_t gcn);

   private:
    /** page offset within gcs file header. */
    ulint m_poffset;
    /** gcn cleaned number. */
    std::atomic<gcn_t> m_gcn;
    /** inited by purge sys. */
    bool m_inited;
  };

  using ScnCleaner = std::atomic<scn_t>;
  using UtcCleaner = utc_t;

 public:
  explicit CClean(ulint purged_poffset, ulint erased_poffset)
      : m_purged_scn(0),
        m_purged_us(0),
        m_purged_gcn(purged_poffset),
        m_erased_scn(0),
        m_erased_us(0),
        m_erased_gcn(erased_poffset) {}

  virtual ~CClean() {}

  /** Init purge when purge system boot. */
  void init_purge();

  /** Advance scn/gcn by purge system. */
  void advance_purge(const commit_order_t &ommt) {
    ut_ad(ommt.scn != SCN_NULL && ommt.scn != 0);
    ut_ad(ommt.gcn != GCN_NULL);
    m_purged_scn.store(ommt.scn, std::memory_order_relaxed);
    m_purged_gcn.advance(ommt.gcn);
    m_purged_us = ommt.us;
  }

  void init_erase();

  /** Advance scn/gcn by erase system. */
  void advance_erase(const commit_order_t &ommt) {
    ut_ad(ommt.scn != SCN_NULL && ommt.scn != 0);
    ut_ad(ommt.gcn != GCN_NULL);
    m_erased_scn.store(ommt.scn, std::memory_order_relaxed);
    m_erased_gcn.advance(ommt.gcn);
    m_erased_us = ommt.us;
  }

  scn_t purged_scn() const {
    return m_purged_scn.load(std::memory_order_relaxed);
  }
  gcn_t purged_gcn() const { return m_purged_gcn.get(); }

  commit_order_t purged_position() const {
    return {m_purged_scn.load(), m_purged_us, m_purged_gcn.get()};
  }

  commit_order_t erased_position() const {
    return {m_erased_scn.load(), m_erased_us, m_erased_gcn.get()};
  }

  scn_t erased_scn() const {
    return m_erased_scn.load(std::memory_order_relaxed);
  }

  gcn_t erased_gcn() const { return m_erased_gcn.get(); }

 private:
  /** scn number cleaned by purge sys.*/
  ScnCleaner m_purged_scn;
  UtcCleaner m_purged_us;
  /** gcn number cleaned by purge sys.*/
  GcnCleaner m_purged_gcn;

  /** scn number cleaned by erased sys.*/
  ScnCleaner m_erased_scn;
  UtcCleaner m_erased_us;
  /** gcn number cleaned by erased sys.*/
  GcnCleaner m_erased_gcn;
};
}  // namespace lizard

#endif
