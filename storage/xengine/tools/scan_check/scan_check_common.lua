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

ffi = require("ffi")

function init()
  assert(event ~= nil, "Event is nil!")
  assert(sysbench.opt.row_num_per_trx % 3 == 0, "Trx_num_per_group should be the multiple of 3!")
  assert(sysbench.opt.padding_size < 1024, "Padding size should be less than 1024")
end

if sysbench.cmdline.command == nil then
  error("Command is required. Supported commands: prepare, run, help!")
end

sysbench.cmdline.options = {
  trx_num_per_group = {"Trx number per group", 9},
  row_num_per_trx = {"Row number per trx", 9},
  padding_size = {"Padding row size", 32},
  mysql_storage_engine = {"Storage engine", "xengine"},
  update_rollback_pct = {"Update transtions rollback precentage", 10},
  sst_file_limit = {"Sst files limit", 10},
  insert_thread_num = {"Insert thread number, for scan_check_mixed.lua", 5},
  update_thread_num = {"Update thread number, for scan_check_mixed.lua", 3}
}

function cmd_prepare()
  local drv = sysbench.sql.driver()
  local con = drv:connect()
  assert(drv:name() == "mysql", "MySQL only!")
  create_table(con)
end

function cmd_cleanup()
  local drv = sysbench.sql.driver()
  local con = drv:connect()
  drop_table(con)
end

sysbench.cmdline.commands = {
  prepare = {cmd_prepare},
  cleanup = {cmd_cleanup}
}

function drop_table(con)
  con:query("drop table if exists scan_check")
  con:query("drop table if exists scan_check_stat")
  con:query("drop table if exists scan_check_update")
end

function create_table(con)
  local query 

  con:query("drop table if exists scan_check")
  con:query("drop table if exists scan_check_stat")
  con:query("drop table if exists scan_check_update")
  
  print("Creating tbale scan_check ...")
  query = string.format([[
    create table scan_check (
      group_id bigint,
      trx_id bigint,
      row_id bigint,
      v1 bigint,
      v1_verify char(20),
      v2 bigint,
      v2_verify varchar(20),
      v3 bigint,
      v3_verify decimal(20,4),
      v4 bigint,
      v5 bigint,
      gmt_create timestamp(6) not null default now(6),
      gmt_modified timestamp(6) not null default now(6),
      v4_modified timestamp(6) not null default now(6),
      v5_modified timestamp(6) not null default now(6),
      pad longblob,
      primary key (group_id, trx_id, row_id),
      key scan_check_sk1 (trx_id, row_id, v1),
      key scan_check_sk2 (trx_id, row_id, v2),
      key scan_check_sk3 (trx_id, row_id, v3),
      key scan_check_sk4 (trx_id, row_id, v4),
      key scan_check_sk5 (trx_id, row_id, v5),
      unique key scan_check_uk1 (group_id, row_id, v1),
      unique key scan_check_uk2 (group_id, row_id, v2),
      unique key scan_check_uk3 (group_id, row_id, v3),
      unique key scan_check_uk4 (group_id, row_id, v4),
      unique key scan_check_uk5 (group_id, row_id, v5)
    ) engine = %s]], sysbench.opt.padding_size, sysbench.opt.mysql_storage_engine)
  con:query(query)

  print("Creating table scan_check_update ...")
  query = string.format([[
    create table scan_check_update (
      trx_id bigint,
      update_num bigint,
      gmt_modified timestamp(6) not null default now(6),
      primary key (trx_id)
    ) engine = %s]], sysbench.opt.mysql_storage_engine)
  con:query(query)

  print("Creating table scan_check_stat ...")
  query = string.format([[
    create table scan_check_stat (
      tid bigint,
      trx_id bigint,
      gmt_create timestamp(6) not null default now(6),
      gmt_modified timestamp(6) not null default now(6),
      primary key (tid)
    ) engine = %s]], sysbench.opt.mysql_storage_engine)
  con:query(query)
end

local insert_stmt_defs = {
  insert_data = "insert into scan_check " ..
      "(group_id, trx_id, row_id, v1, v1_verify, v2, v2_verify, v3, v3_verify, v4, v5, pad) " ..
      "values (%d, %d, %d, %d, '%s', %d, '%s', %d, %s, %d, %d, %s)",

  replace_stat = "replace into scan_check_stat (tid, trx_id, gmt_modified) " ..
      "values (%d, %d, now(6))",

  delete_trx = "delete from scan_check where group_id = %d and trx_id = %d",

  delete_trx2 = "delete from scan_check_update where trx_id = %d"
}

local trx_update_stmt_defs = {
  lock_trx = "select * from scan_check where group_id = %d and trx_id = %d for update",

  update_trx = "update scan_check set v1 = %d, v1_verify = '%s'," .. 
      "v2 = %d, v2_verify = '%s', v3 = %d, v3_verify = %s, " ..
      "gmt_modified = now(6) where group_id = %d and trx_id = %d and row_id = %d",
}

local single_update_stmt_defs = {
  lock_trx_update = "select * from scan_check_update where trx_id = %d for update",
  
  single_update = "update scan_check set v4 = v4 + %d, v4_modified = now(6) " ..
      "where group_id = %d and trx_id = %d and row_id = %d",

  update_num = "insert into scan_check_update values (%d, %d, now(6)) " ..
      "on duplicate key update update_num = update_num + %d, gmt_modified = now(6)" 
}

local transfer_update_stmt_defs = {
  lock_trx = "select * from scan_check where group_id = %d and trx_id = %d for update",

  update_from = "update scan_check set v5 = v5 - %d, v5_modified = now(6) " ..
      " where group_id = %d and trx_id = %d and row_id = %d",

  update_to = "update scan_check set v5 = v5 + %d, v5_modified = now(6) " ..
      " where group_id = %d and trx_id = %d and row_id = %d"
}

local get_stmt_defs = {
  lock_trx = "select * from scan_check where group_id = %d and trx_id = %d lock in share mode",

  get = "select v1, v1_verify, v2, v2_verify, v3, v3_verify from scan_check " ..
      "where group_id = %d and trx_id = %d and row_id = %d"
}

local multi_get_stmt_defs = {
  multi_get = "select sum(v1), sum(v2), sum(v3) from scan_check " ..
      "where (group_id, trx_id, row_id) in ((%d, %d, %d), (%d, %d, %d), (%d, %d, %d))"
}

local scan_pk_stmt_defs = {
  pk_ref = "select sum(v1) from scan_check force index(primary) where group_id = %d",

  pk_range_g = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id > %d",

  pk_range_l = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id < %d",

  pk_range_ge = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id >= %d",

  pk_range_le = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id <= %d",

  pk_range_b = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id > %d and trx_id < %d",

  pk_range_be = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id >= %d and trx_id <= %d",

  pk_range_in = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id in (%d, %d, %d)",

  pk_range_nin = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id not in (%d, %d, %d)",

  pk_ref_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d group by trx_id order by trx_id desc",

  pk_range_g_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id > %d group by trx_id order by trx_id desc",

  pk_range_l_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id < %d group by trx_id order by trx_id desc",

  pk_range_ge_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id >= %d group by trx_id order by trx_id desc",

  pk_range_le_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id <= %d group by trx_id order by trx_id desc",

  pk_range_b_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id > %d and trx_id < %d group by trx_id order by trx_id desc",

  pk_range_be_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id >= %d and trx_id <= %d group by trx_id order by trx_id desc",

  pk_range_in_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id in (%d, %d, %d) group by trx_id order by trx_id desc",

  pk_range_nin_desc = "select sum(v1) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id not in (%d, %d, %d) group by trx_id order by trx_id desc"
}

local scan_sk_stmt_defs = {
  sk_ref = "select sum(v1) + sum(v3) from scan_check " ..
      "force index(scan_check_sk1) where trx_id = %d", 

  sk_range_g = "select sum(v3) from scan_check " ..
      "force index(scan_check_sk2) where trx_id = %d and row_id > %d",

  sk_range_l = "select sum(v3) from scan_check " ..
      "force index(scan_check_sk2) where trx_id = %d and row_id < %d",

  sk_range_ge = "select sum(v3) from scan_check " ..
      "force index(scan_check_sk2) where trx_id = %d and row_id >= %d",

  sk_range_le = "select sum(v3) from scan_check " ..
      "force index(scan_check_sk2) where trx_id = %d and row_id <= %d",

  sk_range_b = "select sum(v3) from scan_check " ..
      "force index(scan_check_sk2) where trx_id = %d and row_id > %d and row_id < %d",

  sk_range_be = "select sum(v3) from scan_check " ..
      "force index(scan_check_sk2) where trx_id = %d and row_id >= %d and row_id <= %d",

  sk_range_in = "select sum(v2) from scan_check " ..
      "force index(scan_check_sk3) where trx_id = %d and row_id in (%d, %d, %d)",

  sk_range_nin = "select sum(v2) from scan_check " ..
      "force index(scan_check_sk3) where trx_id = %d and row_id not in (%d, %d, %d)"
}

local scan_uk_stmt_defs = {
  uk_ref = "select sum(v1) + sum(v3) from scan_check " ..
      "force index(scan_check_uk1) where group_id = %d",

  uk_range_g = "select sum(v3) from scan_check " ..
      "force index(scan_check_uk2) where group_id = %d and row_id > %d",

  uk_range_l = "select sum(v3) from scan_check " ..
      "force index(scan_check_uk2) where group_id = %d and row_id < %d",

  uk_range_ge = "select sum(v3) from scan_check " ..
      "force index(scan_check_uk2) where group_id = %d and row_id >= %d",

  uk_range_le = "select sum(v3) from scan_check " ..
      "force index(scan_check_uk2) where group_id = %d and row_id <= %d",

  uk_range_b = "select sum(v3) from scan_check " ..
      "force index(scan_check_uk2) where group_id = %d and row_id > %d and row_id < %d",

  uk_range_be = "select sum(v3) from scan_check " ..
      "force index(scan_check_uk2) where group_id = %d and row_id >= %d and row_id <= %d",

  uk_range_in = "select sum(v2) from scan_check " ..
      "force index(scan_check_uk3) where group_id = %d and row_id in (%d, %d, %d)",

  uk_range_nin = "select sum(v2) from scan_check " ..
      "force index(scan_check_uk3) where group_id = %d and row_id not in (%d, %d, %d)",
}

local select_single_update_stmt_defs = {
  lock_trx = "select update_num from scan_check_update where trx_id = %d lock in share mode",

  get = "select v4 from scan_check force index(primary) " ..
      "where group_id = %d and trx_id = %d and row_id = %d",

  scan_pk = "select sum(v4) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id = %d",

  scan_sk = "select sum(v4) from scan_check force index(scan_check_sk4) " ..
      "where trx_id = %d",

  scan_uk = "select sum(v4) from scan_check force index(scan_check_uk4) " ..
      "where group_id = %d and trx_id = %d"
}

local select_transfer_update_stmt_defs = {
  lock_trx = "select * from scan_check where group_id = %d and trx_id = %d lock in share mode",

  get = "select v5 from scan_check force index(primary) " ..
      "where group_id = %d and trx_id = %d and row_id = %d",

  scan_pk = "select sum(v5) from scan_check force index(primary) " ..
      "where group_id = %d and trx_id = %d",

  scan_sk = "select sum(v5) from scan_check force index(scan_check_sk5) " ..
      "where trx_id = %d",

  scan_uk = "select sum(v5) from scan_check force index(scan_check_uk5) " ..
      "where group_id = %d and trx_id = %d"
}

local key_consistency_stmt_defs = {
  set_iso = "set tx_isolation = 'REPEATABLE-READ'",
  pk_cnt = "select count(1) from scan_check force index(primary)",
  sk1_cnt = "select count(1) from scan_check force index(scan_check_sk1)",
  sk2_cnt = "select count(1) from scan_check force index(scan_check_sk2)",
  sk3_cnt = "select count(1) from scan_check force index(scan_check_sk3)",
  sk4_cnt = "select count(1) from scan_check force index(scan_check_sk4)",
  sk5_cnt = "select count(1) from scan_check force index(scan_check_sk5)",
  uk1_cnt = "select count(1) from scan_check force index(scan_check_uk1)",
  uk2_cnt = "select count(1) from scan_check force index(scan_check_uk2)",
  uk3_cnt = "select count(1) from scan_check force index(scan_check_uk3)",
  uk4_cnt = "select count(1) from scan_check force index(scan_check_uk4)",
  uk5_cnt = "select count(1) from scan_check force index(scan_check_uk5)",
}

function begin()
  con:query("begin")
end

function commit()
  con:query("commit")
end

function rollback()
  con:query("rollback")
end

function get_min_trx_id()
  min_trx_id = tonumber(con:query_row("select min(trx_id) from scan_check_stat"))
  if min_trx_id == nil then
    min_trx_id = 0
  end
end

function report_trx_id(t_id, trx_id)
  execute_replace_stat(t_id, trx_id)
end

function exceed_sst_file_limit()
  local sst_file_num = tonumber(con:query_row("select count(1) from information_schema.XENGINE_DATA_FILE"))
  local ret = false
  if sst_file_num > sysbench.opt.sst_file_limit then
    ret = true
  end
  return ret
end

-- sum(v1s[i]) = 0
function gen_v1s()
  local v1s = {}
  local total_p_v1 = 0
  for i = 0, sysbench.opt.row_num_per_trx - 2 do
    v1s[i] = sysbench.rand.uniform(1, max_rand_value)
    total_p_v1 = total_p_v1 + v1s[i]
  end
  v1s[sysbench.opt.row_num_per_trx - 1] = -total_p_v1
  return v1s
end

-- sum(v2s[i]) = 0, for each group that i % 3 are equal
function gen_v2s()
  local v2s = {}
  local total_p_v2 = 0
  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    if i % 3 == 0 then
      if (math.floor(i / 3) * 3 + 3) >= sysbench.opt.row_num_per_trx then
        -- last i, which i % 3 == 0
        v2s[i] = -total_p_v2
      else
        v2s[i] = sysbench.rand.uniform(1, max_rand_value) 
        total_p_v2 = total_p_v2 + v2s[i]
      end
    else
      v2s[i] = v2s[math.floor(i / 3) * 3]
    end
  end
  return v2s
end

-- sum(v3s[i]) = 0, for each group that i / 3 are equal
function gen_v3s()
  local v3s = {}
  local total_p_v3 = 0
  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    if i % 3 == 2 then
      v3s[i] = -(v3s[i - 1] + v3s[i - 2])
    else 
      v3s[i] = sysbench.rand.uniform(1, max_rand_value)
    end
  end
  return v3s
end

function execute_insert_data(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx
  local v1s = gen_v1s()
  local v2s = gen_v2s()
  local v3s = gen_v3s()
  local pad_string = string.format("REPEAT('MYSQLMYSQL', %d)",
      sysbench.rand.uniform(sysbench.opt.padding_size / 2, sysbench.opt.padding_size))

  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    execute_stmt(insert_stmt_defs.insert_data, group_id, trx_id, row_id,
        v1s[i], v1s[i], v2s[i], v2s[i], v3s[i], v3s[i], 0, 0, pad_string)
    row_id = row_id + 1
  end

end

function execute_delete_trx(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  execute_stmt(insert_stmt_defs.delete_trx, group_id, trx_id)
  execute_stmt(insert_stmt_defs.delete_trx2, trx_id)
end

function execute_trx_update(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx
  local v1s = gen_v1s()
  local v2s = gen_v2s()
  local v3s = gen_v3s()

  execute_stmt(trx_update_stmt_defs.lock_trx, group_id, trx_id)
  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    execute_stmt(trx_update_stmt_defs.update_trx, v1s[i], v1s[i], 
        v2s[i], v2s[i], v3s[i], v3s[i], group_id, trx_id, row_id)
    row_id = row_id + 1
  end
end

function execute_single_update(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx 
                 + sysbench.rand.uniform(0, sysbench.opt.row_num_per_trx - 1)
  local rand_num = sysbench.rand.uniform(1, max_rand_value)

  execute_stmt(single_update_stmt_defs.lock_trx_update, trx_id)
  execute_stmt(single_update_stmt_defs.single_update, rand_num, group_id, trx_id, row_id)
  execute_stmt(single_update_stmt_defs.update_num, trx_id, rand_num, rand_num, trx_id)
end

function execute_transfer_update(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx 
                 + sysbench.rand.uniform(0, math.floor(sysbench.opt.row_num_per_trx / 2))
  local rand_num = sysbench.rand.uniform(1, max_rand_value)

  execute_stmt(transfer_update_stmt_defs.lock_trx, group_id, trx_id)
  execute_stmt(transfer_update_stmt_defs.update_from, rand_num, group_id, trx_id, row_id)
  execute_stmt(transfer_update_stmt_defs.update_to, rand_num, group_id, trx_id, row_id + 1)
end

--function execute_key_consistency()
  --begin()
  --con:query(key_consistency_stmt_defs.set_iso)
  --local pk_cnt = tonumber(con:query_row(key_consistency_stmt_defs.pk_cnt))
  --local sk1_cnt = tonumber(con:query_row(key_consistency_stmt_defs.sk1_cnt))
  --local sk2_cnt = tonumber(con:query_row(key_consistency_stmt_defs.sk2_cnt))
  --local sk3_cnt = tonumber(con:query_row(key_consistency_stmt_defs.sk3_cnt))
  --local sk4_cnt = tonumber(con:query_row(key_consistency_stmt_defs.sk4_cnt))
  --local sk5_cnt = tonumber(con:query_row(key_consistency_stmt_defs.sk5_cnt))
  --local uk1_cnt = tonumber(con:query_row(key_consistency_stmt_defs.uk1_cnt))
  --local uk2_cnt = tonumber(con:query_row(key_consistency_stmt_defs.uk2_cnt))
  --local uk3_cnt = tonumber(con:query_row(key_consistency_stmt_defs.uk3_cnt))
  --local uk4_cnt = tonumber(con:query_row(key_consistency_stmt_defs.uk4_cnt))
  --local uk5_cnt = tonumber(con:query_row(key_consistency_stmt_defs.uk5_cnt))
  --commit()
  --assert_report("pk vs sk1", pk_cnt, sk1_cnt)
  --assert_report("pk vs sk2", pk_cnt, sk2_cnt)
  --assert_report("pk vs sk3", pk_cnt, sk3_cnt)
  --assert_report("pk vs sk4", pk_cnt, sk4_cnt)
  --assert_report("pk vs sk5", pk_cnt, sk5_cnt)
  --assert_report("pk vs uk1", pk_cnt, uk1_cnt)
  --assert_report("pk vs uk2", pk_cnt, uk2_cnt)
  --assert_report("pk vs uk3", pk_cnt, uk3_cnt)
  --assert_report("pk vs uk4", pk_cnt, uk4_cnt)
  --assert_report("pk vs uk5", pk_cnt, uk5_cnt)
--end

function assert_report(query, res, expected_res)
  assert(res == expected_res, string.format("ERROR: not consensus! Query: %s, res: %d, expected_res: %d", 
      query, res, expected_res))
end

function execute_get(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx
  local v1_sum = 0
  local v2_sum = 0
  local v3_sum = 0
  local row

  execute_stmt(get_stmt_defs.lock_trx, group_id, trx_id)
  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    local res = execute_stmt(get_stmt_defs.get, group_id, trx_id, row_id)
    for i = 1, res.nrows do
      row = res:fetch_row()
      if row[1] then
        assert_report(string.format(get_stmt_defs.get, group_id, trx_id, row_id), 
        tonumber(row[1]), tonumber(row[2]))
        assert_report(string.format(get_stmt_defs.get, group_id, trx_id, row_id), 
        tonumber(row[3]), tonumber(row[4]))
        assert_report(string.format(get_stmt_defs.get, group_id, trx_id, row_id), 
        tonumber(row[5]), tonumber(row[6]))
        v1_sum = v1_sum + tonumber(row[1])
        v2_sum = v2_sum + tonumber(row[3])
        v3_sum = v3_sum + tonumber(row[5])
      end
    end
    row_id = row_id + 1
  end
  assert_report(string.format(get_stmt_defs.get, group_id, trx_id, row_id), v1_sum, 0)
  assert_report(string.format(get_stmt_defs.get, group_id, trx_id, row_id), v2_sum, 0)
  assert_report(string.format(get_stmt_defs.get, group_id, trx_id, row_id), v3_sum, 0)
end

function execute_select_single_update(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx
  local v4_sum = 0

  local expected_num = tonumber(con:query_row(string.format(select_single_update_stmt_defs.lock_trx, trx_id)))
  if expected_num == nil then
    expected_num = 0
  end
  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    local res = execute_stmt(select_single_update_stmt_defs.get, group_id, trx_id, row_id)
    for i = 1, res.nrows do
      row = res:fetch_row()
      if row[1] then
        v4_sum = v4_sum + tonumber(row[1])
      end
    end
    row_id = row_id + 1
  end
  assert_report(string.format(select_single_update_stmt_defs.get, group_id, trx_id, row_id), v4_sum, expected_num)
  execute_stmt_and_check(select_single_update_stmt_defs.scan_pk, expected_num, group_id, trx_id)
  execute_stmt_and_check(select_single_update_stmt_defs.scan_sk, expected_num, trx_id)
  execute_stmt_and_check(select_single_update_stmt_defs.scan_uk, expected_num, group_id, trx_id)
end

function execute_select_transfer_update(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local row_id = trx_id * sysbench.opt.row_num_per_trx
  local v5_sum = 0

  execute_stmt(select_transfer_update_stmt_defs.lock_trx, group_id, trx_id)
  for i = 0, sysbench.opt.row_num_per_trx - 1 do
    local res = execute_stmt(select_transfer_update_stmt_defs.get, group_id, trx_id, row_id)
    for i = 1, res.nrows do
      row = res:fetch_row()
      if row[1] then
        v5_sum = v5_sum + tonumber(row[1])
      end
    end
    row_id = row_id + 1
  end
  assert_report(string.format(select_transfer_update_stmt_defs.get, group_id, trx_id, row_id), v5_sum, 0)
  execute_stmt_and_check(select_transfer_update_stmt_defs.scan_pk, 0, group_id, trx_id)
  execute_stmt_and_check(select_transfer_update_stmt_defs.scan_sk, 0, trx_id)
  execute_stmt_and_check(select_transfer_update_stmt_defs.scan_uk, 0, group_id, trx_id)
end

function execute_stmt(stmt_fmt, ...)
  local query = string.format(stmt_fmt, ...)
  --print(query)
  return con:query(query)
end


function execute_stmt_and_check(stmt_fmt, expected_res, ...)
  local query = string.format(stmt_fmt, ...)
  local res = con:query(query)
  local row
  for i = 1, res.nrows do
    row = res:fetch_row() 
    if row[1] then
      assert_report(query, tonumber(row[1]), expected_res)
    end
  end
end

function execute_scan_check_pk(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local res
  local trx_d3 = math.floor(sysbench.opt.trx_num_per_group / 3)
  local g_trx_id = group_id * sysbench.opt.trx_num_per_group + trx_d3
  local l_trx_id = group_id * sysbench.opt.trx_num_per_group + trx_d3 * 2

  execute_stmt_and_check(scan_pk_stmt_defs.pk_ref, 0, group_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_g, 0, group_id, g_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_l, 0, group_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_ge, 0, group_id, g_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_le, 0, group_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_b, 0, group_id, g_trx_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_be, 0, group_id, g_trx_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_in, 0, group_id, 
      group_id * sysbench.opt.trx_num_per_group, g_trx_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_nin, 0, group_id, 
      group_id * sysbench.opt.trx_num_per_group, g_trx_id, l_trx_id)
end

function execute_scan_check_pk_desc(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local res
  local trx_d3 = math.floor(sysbench.opt.trx_num_per_group / 3)
  local g_trx_id = group_id * sysbench.opt.trx_num_per_group + trx_d3
  local l_trx_id = group_id * sysbench.opt.trx_num_per_group + trx_d3 * 2

  execute_stmt_and_check(scan_pk_stmt_defs.pk_ref_desc, 0, group_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_g_desc, 0, group_id, g_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_l_desc, 0, group_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_ge_desc, 0, group_id, g_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_le_desc, 0, group_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_b_desc, 0, group_id, g_trx_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_be_desc, 0, group_id, g_trx_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_in_desc, 0, group_id, 
      group_id * sysbench.opt.trx_num_per_group, g_trx_id, l_trx_id)
  execute_stmt_and_check(scan_pk_stmt_defs.pk_range_nin_desc, 0, group_id, 
      group_id * sysbench.opt.trx_num_per_group, g_trx_id, l_trx_id)
end

function execute_scan_check_sk(trx_id)
  local res
  local s_row_id = trx_id * sysbench.opt.row_num_per_trx
  local row_d3 = math.floor(sysbench.opt.row_num_per_trx / 3)
  local g_row_id = s_row_id + row_d3 - 1
  local l_row_id = s_row_id + row_d3 * 2

  execute_stmt_and_check(scan_sk_stmt_defs.sk_ref, 0, trx_id)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_g, 0, trx_id, g_row_id)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_l, 0, trx_id, l_row_id)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_ge, 0, trx_id, g_row_id + 1)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_le, 0, trx_id, l_row_id - 1)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_b, 0, trx_id, g_row_id, l_row_id)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_be, 0, trx_id, g_row_id + 1, l_row_id - 1)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_in, 0, trx_id, s_row_id,
      s_row_id + row_d3, s_row_id + row_d3 * 2)
  execute_stmt_and_check(scan_sk_stmt_defs.sk_range_nin, 0, trx_id, s_row_id,
      s_row_id + row_d3, s_row_id + row_d3 * 2)
end

function execute_scan_check_uk(trx_id)
  local group_id = math.floor(trx_id / sysbench.opt.trx_num_per_group)
  local res
  local row_d3 = math.floor(sysbench.opt.row_num_per_trx / 3)
  local s_row_id = trx_id * sysbench.opt.row_num_per_trx
  local g_row_id = s_row_id + row_d3 - 1
  local l_row_id = s_row_id + row_d3 * 2

  execute_stmt_and_check(scan_uk_stmt_defs.uk_ref, 0, group_id)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_g, 0, group_id, g_row_id)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_l, 0, group_id, l_row_id)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_ge, 0, group_id, g_row_id + 1)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_le, 0, group_id, l_row_id - 1)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_b, 0, group_id, g_row_id, l_row_id)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_be, 0, group_id, g_row_id + 1, l_row_id - 1)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_in, 0, group_id, s_row_id,
      s_row_id + row_d3, s_row_id + row_d3 * 2)
  execute_stmt_and_check(scan_uk_stmt_defs.uk_range_nin, 0, group_id, s_row_id,
      s_row_id + row_d3, s_row_id + row_d3 * 2)
end

function execute_scan_check(trx_id)
  execute_scan_check_pk(trx_id)
  execute_scan_check_pk_desc(trx_id)
  execute_scan_check_sk(trx_id)
  execute_scan_check_uk(trx_id)
end

function execute_replace_stat(t_id, trx_id)
  execute_stmt(insert_stmt_defs.replace_stat, t_id, trx_id)
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()

  max_rand_value = 2^25
  min_trx_id = 0
  --pad_string = string.rep("#", sysbench.opt.padding_size)
  get_min_trx_id()
end

function thread_done()
  con:disconnect()
end
