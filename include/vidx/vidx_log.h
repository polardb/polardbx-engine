#ifndef VIDX_LOG_INCLUDED
#define VIDX_LOG_INCLUDED

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

/*
  Forward declarations only — keep this header light so additional
  TUs can include it without pulling sql_class.h / table.h.
  Implementation lives in vidx_log.cc.
*/
struct TABLE;

namespace vidx {

/* Slave-side verbose error logging for vidx auxiliary-table DML.
   On primary threads we rely on the engine's own error path to surface
   the failure; on the replica applier we additionally emit a structured
   log line so apply-pipeline anomalies are diagnosable from the error
   log without enabling DBUG. */
void log_slave_dml_err(const TABLE *table, const char *func, const char *op,
                       int err);

/* Slave-side tolerated-failure logging (lock-wait timeouts that the
   applier intentionally swallows to keep apply progressing). */
void log_slave_dml_warn(const TABLE *table, const char *func, const char *op,
                        int err);

/* DML-path failure logging WITHOUT slave-thread gating — used by paths
   that can fire on both primary and replica (e.g. dup-key recovery in
   clear_stale_tref_node).  Same 1Hz per-call-site throttling as the
   slave-only variants so a sustained failure storm cannot flood
   error.log; counterpart counters in SHOW STATUS remain unthrottled
   and provide the precise rate. */
void log_dml_warn(const TABLE *table, const char *func, const char *op,
                  int err);

}  // namespace vidx

#endif  // VIDX_LOG_INCLUDED
