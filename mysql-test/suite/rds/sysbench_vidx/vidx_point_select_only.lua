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
-- ORDER BY + LIMIT benchmark WITHOUT vector index
--
-- Workload per event:
--   - SELECT id, VECTOR_DIM(embedding) as distance0,
--          VECTOR_DIM(VEC_FROMTEXT('...')) AS distance
--     FROM sbtest ORDER BY distance LIMIT K
--   - Same query shape and data access pattern as vidx_ann_only.lua:
--     reads embedding column, parses VEC_FROMTEXT, ORDER BY + LIMIT
--   - But uses VECTOR_DIM instead of VEC_DISTANCE, no vector index
--   - Isolates the vector index acceleration benefit
--
-- Recommended usage:
--   sysbench vidx_point_select_only.lua --skip_trx=on \
--     --vector_ann_selects=1 --vector_ann_limit=10 run
-- ----------------------------------------------------------------------

-- Add script directory to module search path so vidx_oltp_common.lua
-- can be found regardless of the current working directory.
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")
if script_dir then
   package.path = script_dir .. "?.lua;" .. package.path
end

require("vidx_oltp_common")

function prepare_statements()
   prepare_vector_noindex_selects()
end

function event()
   execute_vector_noindex_selects()
end
