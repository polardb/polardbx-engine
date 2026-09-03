/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file storage/perfschema/komodo_pfs_stat.h
  Komodo fifo cache.

 Created 2026-03-06 by Jiyang.zhang
 *******************************************************/
#ifndef komodo0pfs0stat_h
#define komodo0pfs0stat_h

#include "pfs_stat.h"

namespace komodo {

struct PFS_spc_stat {
  PFS_single_stat m_rec_bytes;
  PFS_single_stat m_row_bytes;
  PFS_single_stat m_comp_recs;
  PFS_single_stat m_uncomp_recs;
  PFS_single_stat m_dict_slots;
  PFS_single_stat m_dict_unused_slots;
  PFS_single_stat m_dict_bytes;
  PFS_single_stat m_spc_page;
  PFS_single_stat m_rebuild_bytes;

  inline void aggregate_dict(ulonglong n_slots, ulonglong unused_slots,
                             ulonglong data_bytes, ulonglong spc_page) {
    m_dict_slots.aggregate_value(n_slots);
    m_dict_unused_slots.aggregate_value(unused_slots);
    m_dict_bytes.aggregate_value(data_bytes);
    m_spc_page.aggregate_counted(spc_page);
  }

  inline void aggregate_recs(ulonglong rec_bytes, ulonglong row_bytes,
                             ulonglong comp_recs, ulonglong uncomp_recs) {
    m_rec_bytes.aggregate_value(rec_bytes);
    m_row_bytes.aggregate_value(row_bytes);
    m_comp_recs.aggregate_value(comp_recs);
    m_uncomp_recs.aggregate_value(uncomp_recs);
  }

  inline void aggregate_rebuild_size(ulonglong size_rebuild) {
    m_rebuild_bytes.aggregate_value(size_rebuild);
  }

  /** TODO: might all spc index ? <06-03-26, zanye.zjy> */
  // inline void aggregate(const PFS_spc_stat *stat) {
  //   m_rec_bytes.aggregate(&stat->m_rec_bytes);
  //   m_row_bytes.aggregate(&stat->m_row_bytes);
  //   m_comp_recs.aggregate(&stat->m_comp_recs);
  //   m_uncomp_recs.aggregate(&stat->m_uncomp_recs);
  //   m_dict_slots.aggregate(&stat->m_dict_slots);
  //   m_dict_unused_slots.aggregate(&stat->m_dict_unused_slots);
  //   m_dict_bytes.aggregate(&stat->m_dict_bytes);
  // }

  inline void reset(void) {
    m_rec_bytes.reset();
    m_row_bytes.reset();
    m_comp_recs.reset();
    m_uncomp_recs.reset();
    m_dict_slots.reset();
    m_dict_unused_slots.reset();
    m_dict_bytes.reset();
    m_rebuild_bytes.reset();
  }
};

}  // namespace komodo

#endif
