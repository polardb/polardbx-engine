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

#include <mysql/psi/mysql_file.h>
#include "my_dbug.h"
#include "mysys_err.h"
#include "sql/mysqld.h"

#include "keyring_rds_key_id.h"
#include "keyring_rds_logger.h"

namespace keyring_rds {

// The global read-only variable of kering_rds dir in DATA_HOME
char *keyring_rds_dir = NULL;
static PSI_file_key master_key_id_file_key;

/**
  Global instance of Key_container caching master key id.
*/
Key_container *key_container = NULL;

#ifdef HAVE_PSI_INTERFACE
static void keyring_rds_init_psi_file_keys(void) {
  static PSI_file_info all_keyring_rds_files[] = {
      {&master_key_id_file_key, "keyring_rds_key_file", 0, 0, PSI_DOCUMENT_ME}};

  const char *category = "keyring_rds_file";
  int count = static_cast<int>(array_elements(all_keyring_rds_files));
  mysql_file_register(category, all_keyring_rds_files, count);
}
#endif

/**
  Called on the plugin initializing, load master key id from local file
*/
bool init_key_id_mgr() {
#ifdef HAVE_PSI_INTERFACE
  keyring_rds_init_psi_file_keys();
#endif

  char keyring_file[FN_REFLEN] = {0};
  const char *dir = keyring_rds_dir;

  if (dir == NULL || dir[0] == '\0') dir = mysql_real_data_home_ptr;

  snprintf(keyring_file, sizeof(keyring_file) - 1, "%s%cmaster_key_id", dir,
           FN_LIBCHAR);

  key_container = new Key_container();
  if (key_container == NULL) {
    Logger::log(ERROR_LEVEL, ER_SERVER_OUTOFMEMORY, sizeof(Key_container));
    return true;
  }

  if (key_container->init(keyring_file)) {
    delete key_container;
    key_container = NULL;
    return true;
  }

  return false;
}

/**
  Called on the plugin uninstall, free objects and close local file
*/
void deinit_key_id_mgr() {
  if (key_container != NULL) {
    delete key_container;
    key_container = NULL;
  }
}

static bool create_file_dir_if_not_exist(const char *file_path) {
  char keyring_dir[FN_REFLEN];
  size_t keyring_dir_length;

  dirname_part(keyring_dir, file_path, &keyring_dir_length);
  while (keyring_dir_length > 1 &&
      (keyring_dir[keyring_dir_length - 1] == FN_LIBCHAR)) {
    keyring_dir[keyring_dir_length - 1] = '\0';
    --keyring_dir_length;
  }

  assert(strlen(keyring_dir) > 0);

  int flags =
#ifdef _WIN32
      0
#else
      S_IRWXU | S_IRGRP | S_IXGRP
#endif
      ;

  /*
    If keyring_dir not exist then create it.
  */
  MY_STAT stat_info;
  for (size_t i = 1; i <= keyring_dir_length; i++) {
    if (keyring_dir[i] == FN_LIBCHAR || i == keyring_dir_length) {
      keyring_dir[i] = 0;

      if (!my_stat(keyring_dir, &stat_info, MYF(0))) {
        if (my_mkdir(keyring_dir, flags, MYF(0)) < 0) {
          char errbuf[MYSYS_STRERROR_SIZE];
          Logger::log(ERROR_LEVEL, EE_CANT_MKDIR, my_errno(),
                      my_strerror(errbuf, sizeof(errbuf), my_errno()));
          return true;
        }
      }

      keyring_dir[i] = FN_LIBCHAR;
    }
  }

  return false;
}

const size_t Key_container::HEADER_LENGTH = sizeof(uint32_t);
const uint32_t Key_container::MAX_CONTENT_LEN = 0xffffffff;

/**
  Load all the master key id.

  @param [in]  file     Path of the key-id file
  @retval      true     Error occurs
  @retval      false    Successfully
*/
bool Key_container::init(const char *file) {
  /* Create key id file dir when first loading this plugin */
  if (create_file_dir_if_not_exist(file)) return true;

  /* Check if the file exists, create it if not exist */
  int open_mode = O_RDWR;
  bool file_exist = !my_access(file, F_OK);
  if (!file_exist) open_mode |= O_CREAT;

  if (m_file_io->open(master_key_id_file_key, file, open_mode)) return true;

  /* Read the valid content length */
  size_t read_bytes =
      m_file_io->read((uchar *)&m_content_size, HEADER_LENGTH, MYF(0));
  if (read_bytes == (size_t)-1) {
    Logger::log(ERROR_LEVEL, "read file header error: %d", my_errno());
    return true;
  }

  if (read_bytes == 0) {
    /* Newly created file */
    m_content_size = HEADER_LENGTH;
    if (m_file_io->write((uchar *)&m_content_size, HEADER_LENGTH) !=
        HEADER_LENGTH)
      return true;
  } else if (read_bytes < HEADER_LENGTH) {
    /* Has been damaged */
    Logger::log(ERROR_LEVEL, "file damaged, read bytes: %u",
                (uint32_t)read_bytes);
    return true;
  } else {
    my_off_t file_size;
    if (m_file_io->seek(0, SEEK_END) == MY_FILEPOS_ERROR) return true;
    if ((file_size = m_file_io->tell()) == MY_FILEPOS_ERROR) return true;

    /* Has been damaged */
    if ((my_off_t)m_content_size > file_size) {
      Logger::log(ERROR_LEVEL, "file damaged, content size %u > file size %u",
                  m_content_size, (uint32_t)file_size);
      return true;
    }

    /* Startup after crashed, truncate invalid content */
    if ((my_off_t)m_content_size < file_size) {
      Logger::log(INFORMATION_LEVEL,
                  "truncate file, content size %u < file size %u",
                  m_content_size, (uint32_t)file_size);
      if (m_file_io->truncate((size_t)m_content_size)) return true;
    }

    /* All OK, restore the position */
    if (m_file_io->seek(HEADER_LENGTH, SEEK_SET) != (my_off_t)HEADER_LENGTH)
      return true;
  }

  /* Load data */
  return load_key_id();
}

/**
  If a key id exist in cache.

  @retval      true     Exist
  @retval      false    Not exist
*/
bool Key_container::exist_in_file(const Key_string &key_id) {
  return m_key_id_hash.find(key_id) != m_key_id_hash.end();
}

/**
  Add a new key id.
  First insert it to cache, then append to local file.

  @retval      true     Error occurs
  @retval      false    Successfully
*/
bool Key_container::store_key_id(const Key_string &key_id) {
  if (key_id.length() >= MAX_KEY_ID_LENGTH) {
    Logger::log(ERROR_LEVEL, "store too long key id: %s", key_id.c_str());
    return true;
  }

  // First append to local file, then insert into cache
  return append_file(key_id) || cache_key_id(key_id);
}

/**
  Read the local key id file, and cache all the id.
  The format of record is:
  ---------------------------------------------
  | id_len (1 byte)  |  id_content (xx bytes)  |
  ---------------------------------------------
*/
bool Key_container::load_key_id() {
  uchar id_len;
  uchar buf[MAX_KEY_ID_LENGTH];
  size_t read_bytes;

  do {
    // Read key id length
    read_bytes = m_file_io->read(&id_len, 1, MYF(0));

    if (read_bytes == (size_t)-1) {
      Logger::log(ERROR_LEVEL, "read file error");
      return true;  // Error
    }

    if (read_bytes == 0) break;  // EOF

    // Exception, file may be damaged
    if (id_len >= MAX_KEY_ID_LENGTH) {
      Logger::log(ERROR_LEVEL, "read too long key id: %u", id_len);
      return true;
    }

    my_off_t pos = m_file_io->tell();
    read_bytes = m_file_io->read(buf, id_len);
    if (read_bytes != id_len) {
      Logger::log(ERROR_LEVEL, "read length %u < expected length %u, at %u",
                  (uint)read_bytes, id_len, (uint)pos);
      return true;
    }

    cache_key_id(Key_string((const char *)buf, id_len));
  } while (true);

  my_off_t pos = m_file_io->tell();
  if (pos != (my_off_t)m_content_size) {
    Logger::log(ERROR_LEVEL, "actual read lenght %u != content length %u",
                (uint)pos, m_content_size);
    return true;
  }

  return false;
}

/**
  Write 1 byte for length + xx bytes for content
*/
bool Key_container::append_file(const Key_string &key) {
  bool has_error = false;
  uint32_t to_be_appended;

  /* Check total length */
  to_be_appended = 1 + (uint32_t)key.length();
  if (m_content_size > (MAX_CONTENT_LEN - to_be_appended)) {
    /* Too large, here we allow the process to continue */
    Logger::log(WARNING_LEVEL, "too large content length %u + %u",
                m_content_size, to_be_appended);
    return false;
  }

  /* Seek to the end of file to append */
  if (m_file_io->seek((my_off_t)m_content_size, MY_SEEK_SET) ==
      MY_FILEPOS_ERROR)
    return true;

  do {
    uchar len = static_cast<uchar>(key.length());
    size_t write_bytes = m_file_io->write(&len, 1);
    if (write_bytes != 1) {
      has_error = true;
      break;
    }

    write_bytes = m_file_io->write((const uchar *)key.c_str(), key.length());
    if (write_bytes != key.length()) {
      has_error = true;
      break;
    }
  } while (false);

  if (!has_error) {
    uint32_t size = m_content_size + to_be_appended;
    if (m_file_io->seek(0, MY_SEEK_SET) == MY_FILEPOS_ERROR ||
        m_file_io->write((uchar *)&size, HEADER_LENGTH) != HEADER_LENGTH ||
        m_file_io->sync())
      return true;

    m_content_size += to_be_appended;
  }

  return has_error;
}

/**
  Insert into cache, and update m_current_id
*/
bool Key_container::cache_key_id(const Key_string &key_id) {
  auto it = m_key_id_hash.find(key_id);

  if (it != m_key_id_hash.end()) {
    it->second++;
    Logger::log(WARNING_LEVEL, "duplicated key id: %s", key_id.c_str());
  } else {
    m_key_id_hash.emplace(key_id, 1);
  }

  /* Update the current key id */
  m_current_id = key_id;
  return false;
}

/**
  Try to get key from cached key_map.
  @param[in]   key_id   key id
  @param[out]  key      key from cache
  @retval      false    Key id exist
  @retval      true     Key id not exist
*/
bool Key_container::get_key_in_cache(const Key_string &key_id,
                                     Key_string *key) {
  if (m_key_map.find(key_id) == m_key_map.end()) return true;
  *key = m_key_map[key_id];
  return false;
}

/**
  Cache key_id and key into key_map.
  @retval      true     Error occurs
  @retval      false    Successfully
*/
bool Key_container::cache_key(const Key_string &key_id, const Key_string &key) {
  if (key_id.length() >= MAX_KEY_ID_LENGTH) {
    Logger::log(ERROR_LEVEL, "store too long key id: %s", key_id.c_str());
    return true;
  }
  if (key.length() != AES_KEY_LENGTH) {
    Logger::log(ERROR_LEVEL, "store wrong length key: %s", key.c_str());
    return true;
  }
  if (m_key_map.find(key_id) != m_key_map.end()) {
    const Key_string &cache_key = m_key_map[key_id];
    if (cache_key != key) {
      Logger::log(ERROR_LEVEL,
                  "the cached key is not equal to the key read from kms, "
                  "cached key is %s, kms key is %s",
                  cache_key.c_str(), key.c_str());
      return true;
    }
    return false;
  }
  m_key_map.emplace(key_id, key);
  return false;
}

/**
  Copy keys from container, initialize iterator.
*/
void Keys_iterator::init(Key_container *container) {
  m_keys.reserve(container->m_key_id_hash.size());

  auto hash_it = container->m_key_id_hash.begin();
  while (hash_it != container->m_key_id_hash.end()) {
    m_keys.push_back(hash_it->first);
    ++hash_it;
  }

  m_it = m_keys.begin();
}

/**
  Release resources.
*/
void Keys_iterator::deinit() {
  m_keys.clear();
  m_it = m_keys.end();
}

/**
  Get details of key, and advance to next position.
*/
bool Keys_iterator::get_key(char *key_id, char *user_id) {
  user_id[0] = 0;

  if (m_it == m_keys.end()) {
    key_id[0] = 0;
    return true;
  }

  strcpy(key_id, m_it->c_str());
  ++m_it;
  return false;
}

}  // namespace keyring_rds
