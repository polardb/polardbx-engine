/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2015, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#pragma once

#include "xengine/listener.h"

namespace myx {

class Xdb_ddl_manager;

class Xdb_event_listener : public xengine::common::EventListener {
public:
  Xdb_event_listener(const Xdb_event_listener &) = delete;
  Xdb_event_listener &operator=(const Xdb_event_listener &) = delete;

  explicit Xdb_event_listener(Xdb_ddl_manager *const ddl_manager)
      : m_ddl_manager(ddl_manager) {}

  void OnCompactionCompleted(xengine::db::DB *db,
                             const xengine::common::CompactionJobInfo &ci) override;
  void OnFlushCompleted(xengine::db::DB *db,
                        const xengine::common::FlushJobInfo &flush_job_info) override;
  void OnExternalFileIngested(
      xengine::db::DB *db,
      const xengine::common::ExternalFileIngestionInfo &ingestion_info) override;

private:
  Xdb_ddl_manager *m_ddl_manager;

  void update_index_stats(const xengine::table::TableProperties &props);
};

} // namespace myx
