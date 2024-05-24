//
// Created by wumu on 2022/10/19.
//

#ifndef MYSQL_CHANGESET_COMMON_H
#define MYSQL_CHANGESET_COMMON_H

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include <mysql.h>

#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"
#include "sql/psi_memory_key.h"

#define CHANGESET_PK_HEADER_SIZE 56

namespace im {

// 1024 * 1024 * 4
static const uint64_t MAX_MEMORY_SIZE = 1 << 22;

// 1024 * 1024 * 32
static const uint64_t MAX_MEMORY_LIMIT = 1 << 25;

enum ChangeType { DELETE, INSERT };

struct FileHeader {
  unsigned int pk_size;
  unsigned int pk_length;
};

class DBTableName {
 public:
  DBTableName() {}
  DBTableName(std::string a, std::string b)
      : db_name(std::move(a)), tb_name(std::move(b)) {}

  std::string db_name;
  std::string tb_name;

  bool operator==(DBTableName t) const {
    return t.db_name == db_name && t.tb_name == tb_name;
  }

  bool operator<(DBTableName t) const {
    if (db_name == t.db_name) {
      return tb_name < t.tb_name;
    }
    return db_name < t.db_name;
  }
};

/**
 * @brief Change encapsulate primary key of changed rows
 *
 */
class Change {
 public:
  Change(ChangeType t, std::string pk)
      : pk(std::move(pk)), type(t), next(nullptr) {}

  std::string &get_primary_key() { return this->pk; }

  ChangeType get_change_type() { return type; }

  Change *get_next() { return next.get(); }

  void set_next(std::unique_ptr<Change> &ptr) { this->next = std::move(ptr); }

  void *operator new(size_t size) {
    return my_malloc(key_memory_CS_PRIMARY_KEY_CACHE, size,
                     MYF(MY_WME | ME_FATALERROR));
  }

  void operator delete(void *ptr) { my_free(ptr); }

 private:
  std::string pk;
  ChangeType type;  // type of this change
  std::unique_ptr<Change> next;
};

class ChangeSetCache {
 public:
  uint64_t insert_count;
  uint64_t update_count;
  uint64_t delete_count;

  my_off_t current_pos;
  mutable std::list<std::pair<my_off_t, std::unique_ptr<Change>>> current_pks;

  void *operator new(size_t size) {
    return my_malloc(key_memory_CS_PRIMARY_KEY_CACHE, size,
                     MYF(MY_WME | ME_FATALERROR));
  }

  void operator delete(void *ptr) { my_free(ptr); }

  ChangeSetCache() {
    insert_count = 0;
    update_count = 0;
    delete_count = 0;
    current_pos = 0;
  }

  void add_save_point(my_off_t pos) { current_pos = pos; }

  void rollback_to_save_point(my_off_t pos) {
    for (auto it = current_pks.begin(); it != current_pks.end(); ++it) {
      if ((*it).first >= pos) {
        current_pks.erase(it--);
      }
    }
  }

  void add_delete(const std::string &pk) {
    auto change = std::unique_ptr<Change>(new Change(DELETE, pk));
    current_pks.emplace_back(current_pos, std::move(change));
    delete_count++;
  }

  void add_insert(const std::string &pk) {
    auto change = std::unique_ptr<Change>(new Change(INSERT, pk));
    current_pks.emplace_back(current_pos, std::move(change));
    insert_count++;
  }

  void add_update(const std::string &pk_before, const std::string &pk_after) {
    auto change_delete = std::unique_ptr<Change>(new Change(DELETE, pk_before));
    auto change_insert = std::unique_ptr<Change>(new Change(INSERT, pk_after));

    current_pks.emplace_back(current_pos, std::move(change_delete));
    current_pks.emplace_back(current_pos, std::move(change_insert));
    update_count++;
  }
};

}  // namespace im

namespace std {
template <>
struct hash<im::DBTableName> {
  std::size_t operator()(const im::DBTableName &k) const {
    using std::hash;
    using std::string;

    // Compute individual hash values for first,
    // second and third and combine them using XOR
    // and bit shifting:

    return ((hash<string>()(k.db_name) ^ (hash<string>()(k.tb_name) << 1)) >>
            1);
  }
};
}  // namespace std

#endif  // MYSQL_CHANGESET_COMMON_H
