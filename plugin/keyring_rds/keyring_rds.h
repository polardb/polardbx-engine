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

#ifndef PLUGIN_KEYRING_RDS_KEYRING_RDS_H_INCUDED
#define PLUGIN_KEYRING_RDS_KEYRING_RDS_H_INCUDED

#include <mysql/psi/mysql_rwlock.h>

namespace keyring_rds {

#define MAX_KEY_ID_LENGTH 128
#define AES_KEY_LENGTH 32

/**
  Internal interface of the keyring service.
*/
class Key_interface {
 public:
  virtual ~Key_interface() {}

  /**
    Corresponds to st_mysql_keyring::mysql_key_fetch
  */
  virtual bool fetch(const char *key_id, char **key_type, const char *user_id,
                     void **key, size_t *key_len) = 0;
  /**
    Corresponds to st_mysql_keyring::mysql_key_store
  */
  virtual bool store(const char *key_id, const char *key_type,
                     const char *user_id, const void *key, size_t key_len) = 0;
  /**
    Corresponds to st_mysql_keyring::mysql_key_remove
  */
  virtual bool remove(const char *key_id, const char *user_id) = 0;
  /**
    Corresponds to st_mysql_keyring::mysql_key_generate
  */
  virtual bool generate(const char *key_id, const char *key_type,
                        const char *user_id, size_t key_len) = 0;

  /**
    Corresponds to st_mysql_keyring::mysql_key_iterator_init
  */
  virtual void iterator_init(void **key_iterator) = 0;
  /**
    Corresponds to st_mysql_keyring::mysql_key_iterator_deinit
  */
  virtual void iterator_deinit(void *key_iterator) = 0;
  /**
    Corresponds to st_mysql_keyring::mysql_key_iterator_get_key
  */
  virtual bool iterator_get_key(void *key_iterator, char *key_id,
                                char *user_id) = 0;
};

/**
  Initialization of the plugin
*/
extern bool keyring_rds_init();
/**
  De-initialization of the plugin
*/
extern void keyring_rds_deinit();

/**
  The lock that protecting plugin (de-)initializing, master key id accessing
*/
extern mysql_rwlock_t LOCK_keyring_rds;

/**
  PSI memory key id traced in performance_schema
*/
extern PSI_memory_key key_memory_KEYRING_rds;

/**
  The global instance of master key id accessor
*/
extern Key_interface *key_imp;

}  // namespace keyring_rds

#endif
