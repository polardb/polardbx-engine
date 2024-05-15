/* Copyright (c) 2016, 2018, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PLUGIN_KEYRING_RDS_KEY_ID_H_INCLUDED
#define PLUGIN_KEYRING_RDS_KEY_ID_H_INCLUDED

#include <memory>
#include "map_helpers.h"
#include "my_inttypes.h"
#include "mysql/psi/mysql_rwlock.h"

#include "plugin/keyring_rds/keyring_rds_file_io.h"
#include "plugin/keyring_rds/keyring_rds_memory.h"

namespace keyring_rds {

class Key_container;

/* The read-only global variable of master key id file path */
extern char *keyring_rds_dir;

/* Master key id cache, initialized when plugin loading */
extern Key_container *key_container;

/* Initialize master key id manager module */
extern bool init_key_id_mgr();

/* De-initialize master key id manager module */
extern void deinit_key_id_mgr();

/**
  We store all the master key id once used into a local file,
  hence verification can be done in the following two scenarios:
    1) If an id extracted fom IBD header not exist in this container,
       maybe some bugs in Keyring service, or in this manager
    2) If an id exsit in this container but not in KMS/Agent,
       maybe some key id lost in KMS/Agent
*/
class Key_container : public Keyring_obj_alloc {
  /* <Key_id, cached_times> */
  typedef malloc_unordered_map<Key_string, int> Key_id_hash;
  /* <Key_id, Key> */
  typedef malloc_unordered_map<Key_string, Key_string> Key_map;

 public:
  Key_container()
      : m_key_id_hash(key_memory_KEYRING_rds),
        m_file_io(new File_io()),
        m_content_size(0),
        m_key_map(key_memory_KEYRING_rds) {}

  ~Key_container() { delete m_file_io; }

  /**
    Load all the master key id.

    @param [in]  file     Path of the key-id file

    @retval      true     Error occurs
    @retval      false    Successfully
  */
  bool init(const char *file);

  /**
    If a key id exist in id_cache.

    @retval      true     Exist
    @retval      false    Not exist
  */
  bool exist_in_file(const Key_string &key_id);

  /**
    Add a new key id.
    First insert it to cache, then append to local file.

    @retval      true     Error occurs
    @retval      false    Successfully
  */
  bool store_key_id(const Key_string &key_id);

  /**
    Get the newest master key id.
    The implement of routine keyring_fetch.
  */
  const Key_string &get_current() const { return m_current_id; }

  /**
    Try to get key from cached key_map.
    @param[in]   key_id   key id
    @param[out]  key      key from cache
    @retval      false    Key id exist
    @retval      true     Key id not exist
  */
  bool get_key_in_cache(const Key_string &key_id, Key_string *key);

  /**
    Cache key_id and key into key_map.
    @retval      true     Error occurs
    @retval      false    Successfully
  */
  bool cache_key(const Key_string &key_id, const Key_string &key);

 private:
  /**
    Internal auxiliary functions.
  */
  bool append_file(const Key_string &key);  // Append a id to file
  bool cache_key_id(const Key_string &key_id);  // Insert a id to cache
  bool load_key_id();  // Initialize key id cache from the local file

 private:
  Key_id_hash m_key_id_hash; /* Key id cache */
  File_io *m_file_io;      /* Accessor to key id file */
  Key_string m_current_id; /* The newest master key id */
  uint32_t m_content_size; /* Length of valid content of key id file */
  Key_map m_key_map;       /* Cached map of key id to key */

  static const size_t HEADER_LENGTH;
  static const uint32_t MAX_CONTENT_LEN;

  friend class Keys_iterator;
};

/**
  Interface to get all key-ids.

  Considering that the number of IDs is not very large,
  here we copy all keys from the global container to a temporary vector,
  and iteration operation is on this vector.
  The goal is to reduce synchronization complexity of the global container.
*/
class Keys_iterator : public Keyring_obj_alloc {
  typedef Keyring_vector<Key_string> Key_vector;
  typedef Key_vector::iterator Key_vector_iterator;

 public:
  Keys_iterator() : m_keys(), m_it(m_keys.begin()) {}

  /**
    Copy keys from container, initialize iterator.
  */
  void init(Key_container *container);

  /**
    Release resources.
  */
  void deinit();

  /**
    Get details of key, and advance to next position.
  */
  bool get_key(char *key_id, char *user_id);

 private:
  Key_vector m_keys;         // temporary vector
  Key_vector_iterator m_it;  // iterator
};

}  // namespace keyring_rds

#endif
