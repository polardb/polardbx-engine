/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#ifndef lizard0mysql_h
#define lizard0mysql_h

#include "lizard0scn.h"
#include "lizard0read0types.h"
#include "sql/lizard/lizard_snapshot.h"

struct row_prebuilt_t;

namespace lizard {

/** Whether to enable use as of query (true by default) */
extern bool srv_force_normal_query_if_fbq;

/** The max tolerable lease time of a snapshot */
extern ulint srv_scn_valid_volumn;

class Vision;
class Snapshot_vision;

/**
  as of query context in row_prebuilt_t
*/
struct asof_query_context_t {
  /** If it has been set, only set once */
  bool m_is_assigned;

  /** Snapshot vision that is used by asof query. */
  Snapshot_vision *m_snapshot_vision;

  /** TODO: ban constructor <16-10-20, zanye.zjy> */
  explicit asof_query_context_t()
      : m_is_assigned(false), m_snapshot_vision(nullptr) {}

  ~asof_query_context_t() { release_vision(); }

  bool is_assigned() { return m_is_assigned; }

  bool is_asof_query() const { return m_snapshot_vision != nullptr; }

  Snapshot_vision *snapshot_vision() const { return m_snapshot_vision; }

  bool is_asof_scn() const {
    return m_snapshot_vision &&
           m_snapshot_vision->type() == Snapshot_type::AS_OF_SCN;
  }
  bool is_asof_gcn() const {
    return m_snapshot_vision &&
           m_snapshot_vision->type() == Snapshot_type::AS_OF_GCN;
  }

  void assign_vision(Snapshot_vision *v) {
    ut_ad(v && v->is_vision());
    m_snapshot_vision = v;
    /** Only set once */
    m_is_assigned = true;
  }

  /** Judge whether snapshot vision is tool old.

      @retval		true	too old
      @retval		false	normal
  */
  bool too_old() { return m_snapshot_vision && m_snapshot_vision->too_old(); }

  void release_vision() {
    m_is_assigned = false;
    m_snapshot_vision = nullptr;
  }
};

/**
  Try to convert flashback query in mysql to as-of query in innodb.

  @param[in/out]    prebuilt    row_prebuilt_t::m_scn_query could be modified

  @return           dberr_t     DB_SUCCESS if convert successfully or it's not
                                a flashback query.
                                ERROR: DB_AS_OF_INTERNAL, DB_SNAPSHOT_OUT_OF_RANGE,
                                DB_AS_OF_TABLE_DEF_CHANGED
*/
dberr_t prebuilt_bind_flashback_query(row_prebuilt_t *prebuilt);

/**
  Reset row_prebuilt_t::m_scn_query, query block level.

  @param[in/out]    prebuilt    row_prebuilt_t

  @return           dberr_t     DB_SUCCESS, or DB_SNAPSHOT_OUT_OF_RANGE.
*/
dberr_t prebuilt_unbind_flashback_query(row_prebuilt_t *prebuilt);
}
#endif
