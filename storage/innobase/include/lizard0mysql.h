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

struct row_prebuilt_t;

namespace lizard {

/** Whether to enable use as of query (true by default) */
extern bool srv_scn_valid_enabled;

/** The max tolerable lease time of a snapshot */
extern ulint srv_scn_valid_volumn;

class Vision;

/**
  as of query context in row_prebuilt_t
*/
struct asof_query_context_t {
  /** If it has been set, only set once */
  bool m_is_set;

  /** Determine if it is a as of query */
  bool m_is_asof_query;

  /** SCN_NULL if it's not a as-of query */
  scn_t m_scn;

  gcn_t m_gcn;

  /** TODO: ban constructor <16-10-20, zanye.zjy> */
  explicit asof_query_context_t()
      : m_is_set(false),
        m_is_asof_query(false),
        m_scn(SCN_NULL),
        m_gcn(GCN_NULL) {}

  ~asof_query_context_t() { reset(); }

  bool is_set() { return m_is_set; }

  bool is_asof_query() const { return m_is_asof_query; }

  bool is_asof_scn() const { return m_is_asof_query && m_scn != SCN_NULL; }
  bool is_asof_gcn() const { return m_is_asof_query && m_gcn != GCN_NULL; }

  void set(scn_t scn, gcn_t gcn) {
    ut_ad(scn == SCN_NULL || gcn == GCN_NULL);

    if (scn != SCN_NULL) {
      m_is_asof_query = true;
      m_scn = scn;
    }
    if (gcn != GCN_NULL) {
      m_is_asof_query = true;
      m_gcn = gcn;
    }
    /** Only set once */
    m_is_set = true;
  }

  void reset() {
    m_is_set = false;
    m_is_asof_query = false;
    m_scn = SCN_NULL;
    m_gcn = GCN_NULL;
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
dberr_t convert_fbq_ctx_to_innobase(row_prebuilt_t *prebuilt);

/**
  Reset row_prebuilt_t::m_scn_query, query block level.

  @param[in/out]    prebuilt    row_prebuilt_t

  @return           dberr_t     DB_SUCCESS, or DB_SNAPSHOT_OUT_OF_RANGE.
*/
dberr_t reset_prebuilt_flashback_query_ctx(row_prebuilt_t *prebuilt);

}

#endif
