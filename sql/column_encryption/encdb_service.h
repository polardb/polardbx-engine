/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file sql/column_encryption/encdb_service.h

  Definition of Encdb_Delegate and Encdb_Observer

  Created 2025-07-31 by Zefeng.Liu
 *******************************************************/

#ifndef SQL_COLUMN_ENCRYPTION_ENCDB_SERVICE_H
#define SQL_COLUMN_ENCRYPTION_ENCDB_SERVICE_H

#include "sql/rpl_handler.h"

class Encdb_delegate;
struct Encdb_observer;

extern Encdb_delegate *encdb_delegate;

int register_encdb_observer(Encdb_observer *observer, void *plugin_var);

int unregister_encdb_observer(Encdb_observer *observer);

struct st_org_field {
  st_org_field() : db(nullptr), table(nullptr), field(nullptr), user(nullptr) {}

  st_org_field(const char *db, const char *table, const char *field,
               const char *user)
      : db(db), table(table), field(field), user(user) {}

  const char *db;
  const char *table;
  const char *field;
  const char *user;
};

struct Encdb_observer {
  uint32 len;
  /**
     Determine whether the current field belongs to a sensitive column.

     @param[in]      field            Definition of field and user
     @param[out]     encrypt          Whether to encrypt current column
  */
  int (*encdb_rule_func)(const st_org_field *field, bool *encrypt);

  /**
     Encdb rule Spread Function for Insert/Create Select query.

     @param[in]      source_db            Source database name
     @param[in]      source_table         Source table name
     @param[in]      source_field         Source field name
     @param[in]      target_db            Target database name
     @param[in]      target_table         Target table name
     @param[in]      target_field         Target field name
     @param[in]      target_tmp_table     Whether target table is temporary
     table
  */
  int (*rule_spread_insert_or_create_select)(
      const char *source_db, const char *source_table, const char *source_field,
      const char *target_db, const char *target_table, const char *target_field,
      bool target_tmp_table);

  /**
     Encdb rule Spread Function for table rename query.

      @param[in]      source_db            Source database name
      @param[in]      source_table         Source table name
      @param[in]      target_db            Target database name
      @param[in]      target_table         Target table name
      @param[in]      target_tmp_table     Whether target table is temporary
      table
  */
  int (*rule_spread_rename_table)(const char *source_db,
                                  const char *source_table,
                                  const char *target_db,
                                  const char *target_table,
                                  bool target_tmp_table);

  /**
     Encdb Rule Spread Function for column rename query.

     @param[in]      source_db            Source database name
     @param[in]      source_table         Source table name
     @param[in]      source_field         Source field name
     @param[in]      target_field         Target field name
     @param[in]      target_tmp_table     Whether target table is temporary
     table
  */
  int (*rule_spread_rename_column)(const char *source_db,
                                   const char *source_table,
                                   const char *source_field,
                                   const char *target_field,
                                   bool target_tmp_table);

  /**
     Reload encryption rules into memory
  */
  int (*reload_encdb_rule)();

  /**
     @param[in]      dst                  Output ciphertext
     @param[in]      dst_len              Ciphertext length
     @param[in]      src                  Plaintext to be encrypted
     @param[in]      arc_len              Plaintext length
     @param[in]      param                Encryption key parameters
     @param[in]      type                 type of data (enum_field_types)
   */
  int (*encdb_encrypt)(uint8_t *dst, uint32_t *dst_len, const uint8_t *src,
                       uint32_t src_len, const char *param, int type);

  /**
     Obtain ciphertext length.

     @param[in]      src_len              Plaintext length
     @param[in]      ciphertext_len       Ciphertext length
  */
  int (*encdb_expected_length)(uint32_t src_len, uint32_t *ciphertext_length);
};

#ifdef HAVE_PSI_RWLOCK_INTERFACE
extern PSI_rwlock_key key_rwlock_Encdb_delegate_lock;
#endif

class Encdb_delegate : public Delegate {
 public:
  Encdb_delegate()
      : Delegate(
#ifdef HAVE_PSI_RWLOCK_INTERFACE
            key_rwlock_Encdb_delegate_lock
#endif
        ) {
  }

  typedef Encdb_observer Observer;

  int encdb_rule_func(const st_org_field *org_field, bool *encrypt);

  int rule_spread_insert_or_create_select(
      const char *source_db, const char *source_table, const char *source_field,
      const char *target_db, const char *target_table, const char *target_field,
      bool target_tmp_table);

  int rule_spread_rename_table(const char *source_db, const char *source_table,
                               const char *target_db, const char *target_table,
                               bool target_tmp_table);

  int rule_spread_rename_column(const char *source_db, const char *source_table,
                                const char *source_field,
                                const char *target_field,
                                bool target_tmp_table);

  int reload_encdb_rule();

  int encdb_encrypt(uint8_t *dst, uint32_t *dst_len, const uint8_t *src,
                    uint32_t src_len, const char *param, int type);

  int encdb_expected_length(uint32_t src_len, uint32_t *ciphertext_length);
};
#endif