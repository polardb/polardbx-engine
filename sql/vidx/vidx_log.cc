/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "vidx/vidx_log.h"

#include <atomic>
#include <chrono>

#include "sql/consensus/consensus_err.h"  // xp::error / xp::warn
#include "sql/sql_class.h"                // THD
#include "sql/table.h"                    // TABLE

namespace vidx {

namespace {

/* Per-function 1-Hz throttling.  Each of log_slave_dml_err /
   log_slave_dml_warn / log_dml_warn has a single static last_ns
   shared by ALL callers of that function.  Under a sustained
   lock-wait-timeout storm or dup_key retry storm, one log line per
   attempt would flood error.log within seconds and push out unrelated
   diagnostics.  Acceptable cost: at most one line lost per second per
   function — operators can still spot the elevated rate via the
   Vidx_invalidate_skip_* / save_retry_dup_key counters in SHOW STATUS,
   which are NOT throttled. */
inline bool log_admit(std::atomic<int64_t> *last_ns) {
  const int64_t now_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t prev = last_ns->load(std::memory_order_relaxed);
  if (now_ns - prev < 1'000'000'000LL) return false;
  /* Race-tolerant: lost CAS just means another thread already admitted
     for this window; either way we suppress. */
  return last_ns->compare_exchange_strong(prev, now_ns,
                                          std::memory_order_relaxed);
}

}  // namespace

void log_slave_dml_err(const TABLE *table, const char *func, const char *op,
                       int err) {
  if (!table->in_use->slave_thread) return;
  static std::atomic<int64_t> last_ns{0};
  if (!log_admit(&last_ns)) return;
  xp::error(ER_XP_0) << func << ": " << op << " failed"
                     << ", err=" << err << ", table=" << table->s->db.str
                     << "." << table->s->table_name.str;
}

void log_slave_dml_warn(const TABLE *table, const char *func, const char *op,
                        int err) {
  if (!table->in_use->slave_thread) return;
  static std::atomic<int64_t> last_ns{0};
  if (!log_admit(&last_ns)) return;
  xp::warn(ER_XP_0) << func << ": " << op << " tolerated"
                    << ", err=" << err << ", table=" << table->s->db.str
                    << "." << table->s->table_name.str;
}

void log_dml_warn(const TABLE *table, const char *func, const char *op,
                  int err) {
  static std::atomic<int64_t> last_ns{0};
  if (!log_admit(&last_ns)) return;
  xp::warn(ER_XP_0) << func << ": " << op << " failed"
                    << ", err=" << err << ", table=" << table->s->db.str
                    << "." << table->s->table_name.str;
}

}  // namespace vidx
