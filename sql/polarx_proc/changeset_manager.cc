//
// Created by wumu on 2022/10/19.
//

#include "changeset_manager.h"

namespace im {
ChangesetManager gChangesetManager;

std::atomic_bool ChangesetManager::changeset_start(false);

int ChangesetManager::start_track(const std::string &table_name,
                                  u_int64_t memory_limit) {
  sql_print_information("changeset-manager start: table=%s memoryLimit=%ld",
                        table_name.data(), memory_limit);

  if (current_thd->db().str == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "please use database first");
    return 1;
  }

  if (memory_limit <= 0 || memory_limit > MAX_MEMORY_LIMIT) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "changeset start failed, error memory limit param");
    return 1;
  }

  std::string db_name(current_thd->db().str, current_thd->db().length);
  DBTableName full_table_name(db_name, table_name);
  if (has_changeset(full_table_name)) {
    std::string err =
        std::string("changeset already in progress: ").append(table_name);
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0), err.c_str());
    return 1;
  }

  TABLE *table;
  int err = 0;
  if ((err = open_table(table_name, &table))) {
    return err;
  }

  mysql_unlock_some_tables(current_thd, &table, 1);

  init_changeset(full_table_name, memory_limit);

  ha_commit_trans(current_thd, false, true);
  close_thread_tables(current_thd);

  return 0;
}

int ChangesetManager::stop_track(const std::string &table_name) {
  if (current_thd->db().str == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "please use database first");
    return 1;
  }

  std::string db_name(current_thd->db().str, current_thd->db().length);
  DBTableName full_table_name(db_name, table_name);
  if (!has_changeset(full_table_name)) {
    return 1;
  }

  // lock the table
  TABLE *table;
  int err = 0;
  if ((err = open_table(table_name, &table, TL_WRITE))) {
    my_printf_error(ER_CHANGESET_COMMAND_ERROR,
                    "lock table when stop track failed", 0);
    return err;
  }

  { erase_changeset(full_table_name); }

  if (has_changeset(full_table_name)) {
    sql_print_information("stop track changeset failed %s\n",
                          table_name.c_str());
  } else {
    sql_print_information("stop track changeset %s", table_name.c_str());
  }

  mysql_unlock_some_tables(current_thd, &table, 1);

  ha_commit_trans(current_thd, false, true);
  close_thread_tables(current_thd);

  return 0;
}

void ChangesetManager::close_changeset(const std::string &db_name, const std::string &table_name) {
  DBTableName full_table_name(db_name, table_name);
  if (!has_changeset(full_table_name)) {
    return ;
  }
  erase_changeset(full_table_name);
  if (has_changeset(full_table_name)) {
    sql_print_information("close changeset failed %s.%s\n", db_name.c_str(), table_name.c_str());
  } else {
    sql_print_information("close changeset %s.%s", db_name.c_str(), table_name.c_str());
  }
}

int ChangesetManager::fence_change(const std::string &table_name) {
  if (current_thd->db().str == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "please use database first");
    return 1;
  }
  std::string db_name(current_thd->db().str, current_thd->db().length);
  DBTableName full_table_name(db_name, table_name);
  if (!has_changeset(full_table_name)) {
    return 1;
  }

  // lock the table
  TABLE *table;
  int err = 0;
  if ((err = open_table(table_name, &table, TL_WRITE))) {
    my_printf_error(ER_CHANGESET_COMMAND_ERROR,
                    "lock table when stop track failed", 0);
    return err;
  }

  set_changeset_stop(full_table_name);

  mysql_unlock_some_tables(current_thd, &table, 1);

  ha_commit_trans(current_thd, false, true);
  close_thread_tables(current_thd);

  return 0;
}

int ChangesetManager::fetch_change(
    const std::string &table_name, bool delete_last_cs,
    std::vector<ChangesetResult *> &changes,
    TABLE_SHARE *table_share) {
  if (current_thd->db().str == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "please use database first");
    return 1;
  }
  std::string db_name(current_thd->db().str, current_thd->db().length);
  DBTableName full_table_name(db_name, table_name);
  if (!has_changeset(full_table_name)) {
    return 1;
  }

  fetch_changeset(full_table_name, delete_last_cs, changes, table_share);
  return 0;
}

int ChangesetManager::fetch_times(const std::string &table_name,
                                  Changeset::Stats &stats) {
  if (current_thd->db().str == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "please use database first");
    return 1;
  }
  std::string db_name(current_thd->db().str, current_thd->db().length);
  DBTableName full_table_name(db_name, table_name);
  if (!has_changeset(full_table_name)) {
    return 1;
  }

  stats = fetch_changeset_stats(full_table_name);
  return 0;
}

void ChangesetManager::get_primary_keys(uchar const *record, KEY *key_info,
                                        std::string &pk) {
  if (record == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "record is nullptr");
    return;
  }

  uint length, offset;
  for (uint i = 0; i < key_info->actual_key_parts; ++i) {
    offset = key_info->key_part[i].offset;
    length = key_info->key_part[i].length;
    pk.append((const char *)record + offset, length);
  }
}

void ChangesetManager::write_row_to_cache(THD *thd, TABLE *table,
                                          uchar const *record) {
  if (!is_changeset_enable()) {
    return;
  }

  TABLE_SHARE *table_meta = table->s;
  std::string db_name(table_meta->db.str, table_meta->db.length);
  std::string table_name(table_meta->table_name.str,
                         table_meta->table_name.length);
  DBTableName full_table_name(db_name, table_name);
  std::string pk;

  if (is_changeset_stop(full_table_name)) {
    return;
  }

  ChangeSetCache *cs = get_changeset_from_cache(thd, full_table_name);

  // extract primary key from record
  KEY *key_info = &table_meta->key_info[table_meta->primary_key];

  get_primary_keys(record, key_info, pk);

  // store record into changeset
  cs->add_insert(pk);
}

void ChangesetManager::update_row_to_cache(THD *thd, TABLE *table,
                                           const uchar *beforeRecord,
                                           const uchar *afterRecord) {
  if (!is_changeset_enable()) {
    return;
  }

  TABLE_SHARE *table_meta = table->s;
  std::string db_name(table_meta->db.str, table_meta->db.length);
  std::string table_name(table_meta->table_name.str,
                         table_meta->table_name.length);
  DBTableName full_table_name(db_name, table_name);

  std::string pk_delete;
  std::string pk_insert;

  if (is_changeset_stop(full_table_name)) {
    return;
  }

  ChangeSetCache *cs = get_changeset_from_cache(thd, full_table_name);

  // extract primary key from record
  KEY *key_info = &table_meta->key_info[table_meta->primary_key];

  get_primary_keys(beforeRecord, key_info, pk_delete);
  get_primary_keys(afterRecord, key_info, pk_insert);

  cs->add_update(pk_delete, pk_insert);
}

void ChangesetManager::delete_row_to_cache(THD *thd, TABLE *table,
                                           uchar const *record) {
  if (!is_changeset_enable()) {
    return;
  }

  TABLE_SHARE *table_meta = table->s;
  std::string db_name(table_meta->db.str, table_meta->db.length);
  std::string table_name(table_meta->table_name.str,
                         table_meta->table_name.length);
  DBTableName full_table_name(db_name, table_name);

  std::string pk;

  if (is_changeset_stop(full_table_name)) {
    return;
  }

  ChangeSetCache *cs = get_changeset_from_cache(thd, full_table_name);

  // extract primary key from record
  KEY *key_info = &table_meta->key_info[table_meta->primary_key];

  get_primary_keys(record, key_info, pk);

  cs->add_delete(pk);
}

void ChangesetManager::set_save_point(THD *thd, my_off_t pos) {
  if (!is_changeset_enable() || thd->changeset_map.empty()) {
    return;
  }

  for (auto &item : thd->changeset_map) {
    auto &cache = item.second;
    if (cache == nullptr) {
      continue;
    }

    cache->add_save_point(pos);
  }
}

void ChangesetManager::rollback_to_save_point(THD *thd, my_off_t pos) {
  if (!is_changeset_enable() || thd->changeset_map.empty()) {
    return;
  }

  for (auto &item : thd->changeset_map) {
    auto &cache = item.second;
    if (cache == nullptr) {
      continue;
    }

    cache->rollback_to_save_point(pos);
  }
}

void ChangesetManager::commit_change(THD *thd) {
  if (!is_changeset_enable() || thd->changeset_map.empty()) {
    erase_changeset_cache_by_thd(thd);
    return;
  }

  // if not leader, close all changeset
  if (consensus_log_manager.get_status() !=
      Consensus_Log_System_Status::BINLOG_WORKING) {
    sql_print_information("current dn is not leader, clean changeset start");
    erase_all_changeset();
    sql_print_information("clean all changeset finish");
    return;
  }

  for (auto &item : thd->changeset_map) {
    auto &full_table_name = item.first;
    auto &cache = item.second;
    if (cache == nullptr) {
      continue;
    }

    commit(full_table_name, cache);
  }

  erase_changeset_cache_by_thd(thd);
}

void ChangesetManager::rollback_change(THD *thd) {
  erase_changeset_cache_by_thd(thd);
}

int ChangesetManager::open_table(const std::string &table_name, TABLE **output,
                                 thr_lock_type lock_type) {
  THD *thd = current_thd;
  LEX_CSTRING db = thd->db();
  Table_ref tables(db.str, db.length, table_name.c_str(), table_name.size(),
                   table_name.c_str(), lock_type);

  tables.open_strategy = Table_ref::OPEN_IF_EXISTS;

  // if (open_tables(thd, &tables_p, &counter, 0)) {
  if (!open_n_lock_single_table(thd, &tables, tables.lock_descriptor().type,
                                0)) {
    close_thread_tables(thd);
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "changeset open table failed");
    return 1;
  }
  if (!tables.table) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "changeset open table failed");
    return 1;
  }

  *output = tables.table;
  tables.table->use_all_columns();

  // no primary-key exists
  if (tables.table->s->primary_key == MAX_KEY) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "table must primary_key");
    return 1;
  }

  return 0;
}

inline ChangeSetCache *ChangesetManager::get_changeset_from_cache(
    THD *thd, const DBTableName &full_table_name) {
  if (thd->changeset_map.find(full_table_name) == thd->changeset_map.end()) {
    thd->changeset_map.emplace(
        full_table_name,
        std::unique_ptr<ChangeSetCache>(new ChangeSetCache()));
  }

  return thd->changeset_map[full_table_name].get();
}

void inline ChangesetManager::erase_changeset_cache_by_thd(THD *thd) {
  thd->changeset_map.clear();
}

inline Changeset *ChangesetManager::get_changeset_ptr(
    const DBTableName &full_table_name) {
  auto it = changeset_map.find(full_table_name);
  if (it == changeset_map.end()) {
    return nullptr;
  }

  return it->second.get();
}

bool inline ChangesetManager::is_changeset_stop(
    const DBTableName &full_table_name) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, false, 2000);
  auto cs = get_changeset_ptr(full_table_name);
  if (cs == nullptr) {
    return true;
  }
  return cs->is_stop();
}

void ChangesetManager::set_changeset_stop(const DBTableName &full_table_name) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, false, 2000);
  auto cs = get_changeset_ptr(full_table_name);
  if (cs == nullptr) {
    return;
  }
  cs->set_stop(true);
}

inline void ChangesetManager::commit(const DBTableName &full_table_name,
                                     std::unique_ptr<ChangeSetCache> &cache) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, false, 2000);
  auto cs = get_changeset_ptr(full_table_name);
  if (cs == nullptr) {
    return;
  }

  if (cs->is_stop()) {
    // do nothing
  } else {
    cs->append_changeset(cache);
  }
}

void ChangesetManager::fetch_changeset(
    const DBTableName &full_table_name, bool delete_last_cs,
    std::vector<ChangesetResult *> &res, TABLE_SHARE *table_share) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, false, 2000);
  auto cs = get_changeset_ptr(full_table_name);
  if (cs == nullptr) {
    return;
  }

  cs->fetch_pk(delete_last_cs, res, table_share);
}

Changeset::Stats ChangesetManager::fetch_changeset_stats(
    const DBTableName &full_table_name) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, false, 2000);
  auto cs = get_changeset_ptr(full_table_name);
  if (cs == nullptr) {
    return Changeset::Stats();
  }

  return cs->update_stats();
}

bool ChangesetManager::has_changeset(const DBTableName &full_table_name) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, false, 2000);

  return changeset_map.count(full_table_name);
}

void ChangesetManager::erase_changeset(const DBTableName &full_table_name) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, true, 2000);

  changeset_map.erase(full_table_name);

  if (changeset_map.empty()) {
    changeset_start.store(false);
  }
}

void ChangesetManager::erase_all_changeset() {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, true, 2000);
  changeset_map.clear();
  changeset_start.store(false);
}

void ChangesetManager::init_changeset(const DBTableName &full_table_name,
                                      u_int64_t memory_limit) {
  polarx_rpc::CautoSpinRWLock lock(rw_lock, true, 2000);
  if (changeset_map.empty()) {
    changeset_start.store(true);
  }

  changeset_map.emplace(full_table_name,
                        std::unique_ptr<Changeset>(new Changeset()));

  auto &cs = changeset_map[full_table_name];
  cs->init(full_table_name);
  cs->memory_limit = memory_limit;
}

}  // namespace im
