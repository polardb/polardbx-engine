-- Copyright (c) 2020, Alibaba Group Holding Limited
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

-- http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

require("scan_check_common")

local l_event_id = 0

function init()
  assert(event ~= nil, "Event is nil!")
  assert(sysbench.opt.row_num_per_trx % 3 == 0, "Trx_num_per_group should be the multiple of 3!")
  --assert(sysbench.opt.padding_size < 1024, "Padding size should be less than 1024")
  assert((sysbench.opt.insert_thread_num + sysbench.opt.update_thread_num) < sysbench.opt.threads, 
        "insert thread number plus update thread number should be less than threads number")
end

function event(t_id)
  if t_id < sysbench.opt.insert_thread_num then
    insert_event(t_id)
  elseif t_id < sysbench.opt.insert_thread_num + sysbench.opt.update_thread_num then
    update_event(t_id)
  else
    select_event(t_id)
  end
end

local l_event_id = 0
local exceed_limit = false
local insert_event_cnt = 0
function insert_event(t_id)
  if exceed_limit == false then
    local trx_id = min_trx_id + sysbench.opt.insert_thread_num * l_event_id + t_id

    begin()
    execute_delete_trx(trx_id)
    execute_insert_data(trx_id)
    commit()

    l_event_id = l_event_id + 1

    insert_event_cnt = insert_event_cnt + 1
    if insert_event_cnt % 100 == 0 then
      report_trx_id(t_id, trx_id)
      exceed_limit = exceed_sst_file_limit()
    end
  else
    print(string.format("Sst file number exceeds sst_file_limit(%d), stop inserting data!", sysbench.opt.sst_file_limit))
    os.execute("sleep 10")
  end
end

local update_event_cnt = 0
function update_event(t_id)
  if min_trx_id == 0 then
    print("No data, min_trx_id is 0, sleep for 10 sec ...")
    os.execute("sleep 10")
    get_min_trx_id()
  else
    -- 20% trx_update, 40% single_update, 40% transfer update
    local up_rand = sysbench.rand.uniform(1, 100) 
    local trx_id = sysbench.rand.uniform(0, min_trx_id)
    if up_rand < 20 then
      trx_update(trx_id)
    elseif up_rand < 60 then
      single_update(trx_id)
    else
      transfer_update(trx_id) 
    end

    update_event_cnt = update_event_cnt + 1
    if update_event_cnt % 1000 == 0 then
      get_min_trx_id()
    end
  end
end

local select_event_cnt = 0
function select_event(t_id)
  if min_trx_id == 0 then
    print("No data, min_trx_id is 0, sleep for 10 sec ...")
    os.execute("sleep 10")
    get_min_trx_id()
  else 
    local trx_id = sysbench.rand.uniform(0, min_trx_id)
    -- get
    begin()
    execute_get(trx_id)
    commit()
    -- scan
    execute_scan_check(trx_id)
    -- single update
    begin()
    execute_select_single_update(trx_id)
    commit()
    -- transfer update
    begin()
    execute_select_transfer_update(trx_id)
    commit()

    select_event_cnt = select_event_cnt + 1
    if select_event_cnt % 1000 == 0 then
      get_min_trx_id()
    end
  end
end

function single_update(trx_id)
  local will_rollback = sysbench.rand.uniform(1, 100)
  begin()
  execute_single_update(trx_id)
  if will_rollback < sysbench.opt.update_rollback_pct then
    rollback()
  else 
    commit()
  end
end

function trx_update(trx_id)
  local will_rollback = sysbench.rand.uniform(1, 100)
  begin()
  execute_trx_update(trx_id)
  -- check WriteBatch
  execute_scan_check(trx_id)
  if will_rollback < sysbench.opt.update_rollback_pct then
    rollback()
  else 
    commit()
    execute_scan_check(trx_id)
  end
end

function transfer_update(trx_id)
  local will_rollback = sysbench.rand.uniform(1, 100)
  begin()
  execute_transfer_update(trx_id)
  if will_rollback < sysbench.opt.update_rollback_pct then
    rollback()
  else 
    commit()
  end
end
