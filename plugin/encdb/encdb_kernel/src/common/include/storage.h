/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once
#include <string>
#include <vector>
#include "encdb_error.h"

namespace encdb { struct CipherContext; }

namespace encdb {
/** Snapshot of one row from mysql.encdb_kms_meta. */
struct KmsMetaRecord {
  bool valid = false;
  int version = -1;
  std::string enc_mek;
  std::string region;
  std::string key_id;
  std::string kms_salt_mac;
};
}  // namespace encdb

// Session CC access (implemented in plugin layer)
const encdb::CipherContext *encdb_plugin_get_session_cc();
void encdb_plugin_set_session_cc(const encdb::CipherContext *cc);

int encdb_plugin_exec_sql(const std::string &, bool);
int encdb_plugin_exec_sql(const std::string &,
                          std::vector<std::vector<std::string>> &);
void *encdb_plugin_malloc(size_t size);
void encdb_plugin_free(void *ptr);
int encdb_log_message(enum LogLevel level, const char *format, ...)
    __attribute__((format(printf, 2, 3)));

// KMS meta table handler-API hooks (implemented in plugin/80 layer)
int encdb_plugin_kms_meta_read_latest(encdb::KmsMetaRecord *rec);
int encdb_plugin_kms_meta_insert(const std::string *enc_mek,
                                 const std::string *region,
                                 const std::string *key_id,
                                 const std::string *kms_salt_mac);
int encdb_plugin_kms_meta_delete_all();
uint encdb_plugin_kms_meta_get_max_entries();

// When true, CC_MISS on encrypt path throws an error; when false (default),
// falls back to ephemeral-key encryption (unreadable ciphertext, no error).
bool encdb_plugin_cc_miss_error();
