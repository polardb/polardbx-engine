#!/usr/bin/env sysbench
-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- Single-row DELETE benchmark
--
-- Workload per event:
--   - DELETE one random row by primary key (id)
--   - Measures delete throughput on a table with HNSW vector index
--
-- Note: Rows will be depleted over time. For sustained testing, consider
-- using a large --table_size or combining with periodic re-prepare.
--
-- Recommended usage:
--   sysbench vidx_delete.lua --skip_trx=on run
-- ----------------------------------------------------------------------

-- Add script directory to module search path so vidx_oltp_common.lua
-- can be found regardless of the current working directory.
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")
if script_dir then
   package.path = script_dir .. "?.lua;" .. package.path
end

require("vidx_oltp_common")

function prepare_statements()
   prepare_delete_inserts()  -- this prepares the "deletes" prepared statement
end

function event()
   local tnum = get_table_num()
   local id = get_id()

   param[tnum].deletes[1]:set(id)
   stmt[tnum].deletes:execute()
end
