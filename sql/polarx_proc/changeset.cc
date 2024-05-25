#include <memory>

#include "changeset.h"

namespace im {

LEX_CSTRING POLARX_PROC_SCHEMA = {C_STRING_WITH_LEN("polarx")};

int Changeset::init(const DBTableName &full_table_name) {
  MutexLock lock(&mutex);

  this->stop = false;
  this->imm_empty = true;
  this->full_table_name = full_table_name;
  sql_print_information("init changeset for table %s.%s",
                        full_table_name.db_name.c_str(),
                        full_table_name.tb_name.c_str());
  return 0;
}

void Changeset::close() {
  MutexLock lock(&mutex);
  // make sure background thread do not work
  while (!imm_empty.load(std::memory_order_acquire)) {
    cv.Wait();
  }

  rm_all_changeset_files();
}

/**
 * @brief Clear stats info
 */
void Changeset::clear() {
  MutexLock lock(&mutex);

  stats_.insert_count = 0;
  stats_.delete_count = 0;
  stats_.update_count = 0;
}

/**
 * swap of pk map
 */
void Changeset::swap_pk() {
  mem_pk_map.swap(tmp_map);

  tmp_memory_size.store(memory_size);
  memory_size.store(0);
}

/**
 * session changeset ---> global changeset
 * when trans commit
 * may be called by muti thread
 */
void Changeset::append_changeset(std::unique_ptr<ChangeSetCache> &cache) {
  MutexLock lock(&mutex);

  if (cache == nullptr) {
    return;
  }

  memory_check_and_flush();

  for (auto &it : cache->current_pks) {
    auto &change = it.second;
    if (change->get_change_type() == DELETE) {
      // include memory info
      pk_map_delete(change, true);
    } else if (change->get_change_type() == INSERT) {
      // include memory info
      pk_map_insert(change, true);
    } else {
      sql_print_information(
          "changeset commit error, type is not insert nor delete");
    }
  }

  // only normal stats
  stats_.insert_count += cache->insert_count;
  stats_.delete_count += cache->delete_count;
  stats_.update_count += cache->update_count;
}

/**
 * @brief generate changeset file name
 */
const char *Changeset::get_next_file_name() {
  char *file_name_buffer = (char *)my_malloc(key_memory_CS_FILE_NAME, 128,
                                             MYF(MY_WME | ME_FATALERROR));
  memset(file_name_buffer, 0, 128);
  sprintf(file_name_buffer, "./%s/%s_%d.pkcs", full_table_name.db_name.c_str(),
          full_table_name.tb_name.c_str(), file_name_num);
  file_name_num++;
  return file_name_buffer;
}

/**
 * @brief imm pk map of changeset ---> changeset file
 */
void flush_imm_table(void *changeset) {
  auto *cs = (Changeset *)changeset;
  MutexLock lock(&cs->mutex);
  assert(!cs->imm_pk_map.empty());

  const char *file_name = cs->get_next_file_name();

  sql_print_information("changeset flush start, file name: %s, size: %ld",
                        file_name,
                        cs->imm_memory_size.load(std::memory_order_acquire));

  {
    cs->mutex.Unlock();

    File fd;
    if ((fd = my_open(file_name, O_CREAT | O_WRONLY | O_APPEND, MYF(MY_WME))) <
        0) {
      my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "open file failed");
      return;
    }

    FileHeader fileHeader{0, 0};
    fileHeader.pk_length = cs->imm_pk_map.begin()->first.length();
    fileHeader.pk_size =
        cs->imm_memory_size.load() / (cs->imm_pk_map.begin()->first.length() + CHANGESET_PK_HEADER_SIZE);
    my_write(fd, reinterpret_cast<const uchar *>(&fileHeader),
             sizeof(fileHeader), MYF(MY_NABP));

    unsigned int pk_num = 0;
    for (auto &item : cs->imm_pk_map) {
      auto pk = item.first;
      auto change = item.second.get();

      // only pk
      while (change != nullptr) {
        pk_num++;
        ChangeType t = change->get_change_type();
        my_write(fd, reinterpret_cast<const uchar *>(&t), sizeof(t),
                 MYF(MY_NABP));
        my_write(fd, reinterpret_cast<const uchar *>(pk.data()), pk.length(),
                 MYF(MY_NABP));

        change = change->get_next();
      }
    }

    my_close(fd, MYF(0));

    assert(pk_num == fileHeader.pk_size);

    cs->imm_pk_map.clear();

    cs->mutex.Lock();
  }

  sql_print_information("changeset flush finish, file name: %s, size: %ld",
                        file_name,
                        cs->imm_memory_size.load(std::memory_order_acquire));

  cs->file_list.emplace_back(file_name, true);
  cs->imm_empty.store(true);
  // memory info
  cs->imm_memory_size.store(0);

  // signal wait
  cs->cv.SignalAll();
}

void Changeset::memory_check_and_flush() {
  // check changeset memory
  if (memory_size.load(std::memory_order_acquire) < memory_limit) {
    return;
  }

  if (!imm_empty.load(std::memory_order_acquire)) {
    // wait imm background flush
    sql_print_information("wait background flush");

    cv.Wait();
  } else {
    imm_empty.store(false);
    // swap table and imm table
    mem_pk_map.swap(imm_pk_map);

    // swap memory info
    imm_memory_size.store(memory_size);
    memory_size.store(0);

    sql_print_information("add background flush task");
    thread_pool->Schedule(&flush_imm_table, this);
  }
}

/**
 * @brief remove the file or pk map of changeset which has been fetched last
 * once
 */
void Changeset::rm_old_snapshot() {
  if (tmp_file_name != nullptr) {
    /* Remove the changeset file */
    my_delete(tmp_file_name, MYF(0));
    // free file name ptr
    my_free(const_cast<char *>(tmp_file_name));
  }

  tmp_file_name = nullptr;
  // map.clear() will call ~Change()
  tmp_map.clear();
  tmp_memory_size.store(0);
}

/**
 * @brief remove all files generated by changeset
 */
void Changeset::rm_all_changeset_files() {
  if (tmp_file_name != nullptr) {
    /* Remove the changeset file */
    my_delete(tmp_file_name, MYF(0));
    // free file name ptr
    my_free(const_cast<char *>(tmp_file_name));
  }

  for (auto &file_name : file_list) {
    /* Remove the changeset file */
    my_delete(file_name.first, MYF(0));
    // free file name ptr
    my_free(const_cast<char *>(file_name.first));
  }
}

void Changeset::fetch_pk(bool delete_last_cs,
                         std::vector<ChangesetResult *> &res,
                         TABLE_SHARE *table_share) {
  MutexLock lock(&mutex);

  // free last fetch changeset data
  if (delete_last_cs) {
    rm_old_snapshot();
  }

  // old to new fetch
  // 1. tmp file
  // 2. tmp mem table
  // 3. history files
  // 4. imm memory table (imm_pk_map)
  // 5. memory table (mem_pk_map)
  if (tmp_file_name != nullptr) {
    get_result_list(tmp_file_name, res, table_share);
  } else if (!tmp_map.empty()) {
    get_result_list(tmp_map, res, table_share);
  } else if (!file_list.empty()) {
    auto &item = file_list.front();

    assert(item.second);

    item.second = false;

    tmp_file_name = item.first;

    get_result_list(tmp_file_name, res, table_share);

    file_list.pop_front();
  } else if (!imm_empty.load(std::memory_order_acquire)) {
    // wait imm background flush
    sql_print_information("wait imm background flush");

    cv.Wait();

    assert(file_list.size() == 1);

    auto &item = file_list.front();

    assert(item.second);

    item.second = false;

    tmp_file_name = item.first;

    get_result_list(tmp_file_name, res, table_share);

    file_list.pop_front();
  } else {
    // swap tmp
    swap_pk();

    get_result_list(tmp_map, res, table_share);
  }
}

void Changeset::get_result_list(
    std::unordered_map<std::string, std::unique_ptr<Change>> &pk_map,
    std::vector<ChangesetResult *> &res, TABLE_SHARE *table_share) {
  uint primary_key = table_share->primary_key;
  KEY *key_info = &table_share->key_info[primary_key];

  mutex.Unlock();

  for (auto &item : pk_map) {
    auto pk = item.first;
    auto change = item.second.get();
    auto *key = (uchar *)pk.data();

    // only pk
    while (change != nullptr) {
      std::list<Field *> pk_field =
          make_pk_fields(key_info, key, current_thd->mem_root);
      res.push_back(new ChangesetResult(change->get_change_type(), pk_field));
      change = change->get_next();
    }
  }

  mutex.Lock();
}

void Changeset::get_result_list(const char *file_name,
                                std::vector<ChangesetResult *> &res,
                                TABLE_SHARE *table_share) {
  uint primary_key = table_share->primary_key;
  KEY *key_info = &table_share->key_info[primary_key];

  mutex.Unlock();

  File fd;
  if ((fd = my_open(file_name, O_RDONLY, MYF(MY_WME))) < 0) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "open file failed");
    return;
  }

  FileHeader fileHeader{0, 0};
  my_read(fd, reinterpret_cast<uchar *>(&fileHeader), sizeof(fileHeader),
          MYF(0));

  auto buffer =
      (uchar *)my_malloc(key_memory_CS_RESULT_BUFFER, fileHeader.pk_length,
                         MYF(MY_WME | ME_FATALERROR));

  for (unsigned int i = 0; i < fileHeader.pk_size; ++i) {
    ChangeType t;

    my_read(fd, reinterpret_cast<uchar *>(&t), sizeof(t), MYF(0));
    my_read(fd, reinterpret_cast<uchar *>(buffer), fileHeader.pk_length,
            MYF(0));

    std::list<Field *> pk_field =
        make_pk_fields(key_info, buffer, current_thd->mem_root);

    res.push_back(new ChangesetResult(t, pk_field));
  }

  my_free(buffer);

  my_close(fd, MYF(0));

  mutex.Lock();
}

int Changeset::open_table(THD *thd, const std::string &db,
                          const std::string &tb, TABLE **output) {
  Table_ref tables(db.c_str(), db.length(), tb.c_str(), tb.length(), tb.c_str(),
                   TL_READ);
  tables.open_strategy = Table_ref::OPEN_IF_EXISTS;

  if (!open_n_lock_single_table(thd, &tables, tables.lock_descriptor().type,
                                0)) {
    sql_print_information("open table %s.%s err 2", db.c_str(), tb.c_str());
    close_thread_tables(thd);
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "changeset open table failed");
    return 1;
  } else {
    *output = tables.table;
    tables.table->use_all_columns();
  }
  return 0;
}

uint64_t Changeset::get_approximate_memory_size() const {
  return memory_size.load(std::memory_order_acquire) +
         imm_memory_size.load(std::memory_order_acquire) +
         tmp_memory_size.load(std::memory_order_acquire);
}

Changeset::Changeset()
    : memory_limit(MAX_MEMORY_SIZE),
      stop(false),
      file_name_num(0),
      tmp_file_name(nullptr),
      cv(&mutex) {
  memory_size.store(0);
  imm_memory_size.store(0);
  memset(&stats_, 0, sizeof(stats_));
}

Changeset::~Changeset() { close(); }

std::list<Field *> Changeset::make_pk_fields(KEY *key_info, uchar *pk,
                                             MEM_ROOT *mem_root) {
  std::list<Field *> ret;
  for (uint i = 0; i < key_info->actual_key_parts; ++i) {
    // make field
    Field *field = key_info->key_part[i].field->clone(mem_root);
    uint16 length = key_info->key_part[i].length;

    uchar *buffer = (uchar *)my_malloc(key_memory_CS_RESULT_BUFFER, length,
                                       MYF(MY_WME | ME_FATALERROR));
    memcpy(buffer, pk, length);

    field->set_field_ptr(buffer);
    // add result
    ret.push_back(field);
    // pk next
    pk += length;
  }
  return ret;
}

void Changeset::pk_map_delete(std::unique_ptr<Change> &change, bool mem_c) {
  auto &pk = change->get_primary_key();
  auto it = mem_pk_map.find(pk);

  if (it != mem_pk_map.end()) {
    auto cur = (*it).second.get();
    while (cur != nullptr) {
      cur = cur->get_next();
      // memory info
      memory_size.fetch_sub(pk.length() + CHANGESET_PK_HEADER_SIZE);
    }
    mem_pk_map.erase(it);
  }

  // memory info
  if (mem_c) {
    memory_size.fetch_add(pk.length() + CHANGESET_PK_HEADER_SIZE);
    if (change->get_next() != nullptr) {
      memory_size.fetch_add(pk.length() + CHANGESET_PK_HEADER_SIZE);
    }
  }

  mem_pk_map.emplace(pk, std::move(change));
}

void Changeset::pk_map_insert(std::unique_ptr<Change> &change, bool mem_c) {
  auto &pk = change->get_primary_key();
  auto it = mem_pk_map.find(pk);

  if (it == mem_pk_map.end()) {
    mem_pk_map.emplace(pk, std::move(change));
  } else {
    if ((*it).second->get_change_type() != DELETE) {
      sql_print_information(
          "pk: %s, change_type: %d, changeset pk: %s, next ptr: %p", pk.c_str(),
          (*it).second->get_change_type(),
          (*it).second->get_primary_key().c_str(), (*it).second->get_next());
    }
    assert((*it).second->get_change_type() == DELETE);
    assert((*it).second->get_next() == nullptr);

    (*it).second->set_next(change);
  }

  // memory info
  if (mem_c) memory_size.fetch_add(pk.length() + CHANGESET_PK_HEADER_SIZE);
}

void Changeset::add_delete(const std::string &pk) {
  auto change = std::unique_ptr<Change>(new Change(DELETE, pk));

  pk_map_delete(change);
  stats_.delete_count++;
}

void Changeset::add_insert(const std::string &pk) {
  auto change = std::unique_ptr<Change>(new Change(INSERT, pk));

  pk_map_insert(change);
  stats_.insert_count++;
}

void Changeset::add_update(const std::string &pk_before,
                           const std::string &pk_after) {
  auto change_delete = std::unique_ptr<Change>(new Change(DELETE, pk_before));
  auto change_insert = std::unique_ptr<Change>(new Change(INSERT, pk_after));

  pk_map_delete(change_delete);
  pk_map_insert(change_insert);
  stats_.update_count++;
}

Changeset::Stats Changeset::update_stats() {
  MutexLock lock(&mutex);
  stats_.memory_size = get_approximate_memory_size();
  stats_.file_count = file_list.size();
  if (tmp_file_name != nullptr) {
    stats_.file_count += 1;
  }
  return stats_;
}

}  // namespace im
