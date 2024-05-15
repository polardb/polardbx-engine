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

#include "base64.h"
#include "sql/mysqld.h"

#include "keyring_rds.h"
#include "keyring_rds_kms_agent.h"
#include "keyring_rds_logger.h"
#include "keyring_rds_memory.h"
#include "my_popen.h"

/**
  !! Important Note (to be optimized) !!

  Currently, the key management interface provided by the KMS/Agent
  is the local system command, we can only interact with it by
  invoking system() or popen().
  So, the robustness of the program as a whole depends on the
  correctness of the implementation of Interface CMD.

  1. The output of the CMD must be implemented strictly accroding
     to the convertion. That is, there must be only one line output,
     and starting with "ERROR" if an error occurs.

  2. CMD execution must be returned within a certain time, in any case

  3. Performance guarantee. "Fetch Key" is a high frequency calling
     interface when startup, especially when there are many encrypt
     tables.
*/

namespace keyring_rds {

/** KMS agent local command */
char *kms_agent_cmd = NULL;

/** Local command execution timeout */
int cmd_timeout_sec = 10;

bool validate_cached_key = false;

KMS_Agent global_inst;
KMS_Agent *kms_agent = &global_inst;  // The global instance

static const size_t MAX_CMD_LEN = 256;
static const size_t MAX_OUTPUT_LEN = 512;

/**
  Helper routine to execute command.

  @param [in]  cmd      Command line
  @param [out] ret_buf  Output buffer
  @param [in]  buf_len  Output buffer length
  @retval = 0  Error occurs
  @retval > 0  The length of output conetnt
*/
static size_t execute_cmd(const char *cmd, char *ret_buf, size_t buf_len) {
  int fd;
  pid_t pid;

  if (keyring_popen(cmd, &fd, &pid)) return 0;

  size_t ret_size = keyring_read(fd, pid, ret_buf, buf_len, cmd_timeout_sec);

  keyring_pclose(fd, pid);

  /**
    KMS/Agent output ERROR:xxx if any error occurs.
    Here rely on the correctness of the KMS/Agent command.
  */
  if (ret_size >= 5 && memcmp(ret_buf, "ERROR", 5) == 0) {
    Logger::log(ERROR_LEVEL, "error pipe output [%s]", ret_buf);
    return 0;
  }

  /* Trim right */
  while (ret_size > 0) {
    char *tail = ret_buf + ret_size - 1;
    if (*tail != '\n' && *tail != ' ' && *tail != '\t' && *tail != '\r') break;
    *tail = 0;
    ret_size--;
  }

  if (ret_size == 0) Logger::log(ERROR_LEVEL, "pipe output length = 0");

  return ret_size;
}

/**
  Transforms an arbitrary long key into a fixed length AES key.

  @param [in]  k     Original key content
  @param [in]  k_len Original key length
  @param [out] rkey  32 bytes length AES key
*/
static void standardized_aes_key(const uchar *k, size_t k_len, uchar *rkey) {
  uchar *rkey_end;      /* Real key boundary */
  uchar *ptr;           /* Start of the real key*/
  const uchar *sptr;    /* Start of the working key */
  const uchar *key_end; /* Working key boundary*/

  key_end = k + k_len;
  rkey_end = rkey + AES_KEY_LENGTH;

  memset(rkey, 0, AES_KEY_LENGTH); /* Set initial key */

  for (ptr = rkey, sptr = k; sptr < key_end; ptr++, sptr++) {
    if (ptr == rkey_end) /*  Just loop over tmp_key until we used all key */
      ptr = rkey;
    *ptr ^= *sptr;
  }
}

#ifndef NDEBUG
/* For autotest */
static char *saved_cmd = NULL;

static void set_kms_agent_cmd_mock() {
  static char progdir[FN_REFLEN] = {0};

  if (!progdir[0]) {
    size_t dlen = 0;
    dirname_part(progdir, my_progname, &dlen);
    strcat(progdir, "kms_agent_mock");

    saved_cmd = kms_agent_cmd;
    kms_agent_cmd = progdir;
  }
}

static void set_kms_agent_cmd_unmock() {
  if (saved_cmd != NULL) {
    kms_agent_cmd = saved_cmd;
    saved_cmd = NULL;
  }
}
#endif

/**
  Request KMS to generate a new master key.

  @para[out] id_buff      Buffer storing the generated master key id
  @para[in]  id_buff_len  Buffer Length
  @return <=0   Error occurs
  @return  >0   The actual length of the generated master key id
*/
size_t KMS_Agent::generate(char *id_buff, size_t id_buff_len) {
  // For autotest
  DBUG_EXECUTE_IF("keyring_rds_kms_simu", { set_kms_agent_cmd_mock(); });
  DBUG_EXECUTE_IF("keyring_rds_kms_unsimu", { set_kms_agent_cmd_unmock(); });

  char cmd[MAX_CMD_LEN];
  char out[MAX_OUTPUT_LEN];

  snprintf(cmd, sizeof(cmd), "%s %d generateKey", kms_agent_cmd, mysqld_port);

  size_t ret_len = execute_cmd(cmd, out, sizeof(out));

  if (ret_len == 0) return 0;  // Err logged in execute_cmd

  if (ret_len >= id_buff_len) {
    Logger::log(ERROR_LEVEL, "key id too long [%u, %s]", (uint)ret_len, out);
    return 0;
  }

  strcpy(id_buff, out);  // master key id is of type string
  return ret_len;
}

/**
  Fetch the master key specified by id from KMS .

  @para[in]  id            Specified master key id
  @para[out] key_buff      Buffer storing the master key content
  @para[in]  key_buff_len  Buffer Length
  @return <=0   Error occurs
  @return  >0   The actual length of the master key
*/
size_t KMS_Agent::fetch(const char *id, char *key_buff,
                        size_t key_buff_len MY_ATTRIBUTE((unused))) {
  // For autotest
  DBUG_EXECUTE_IF("keyring_rds_kms_simu", { set_kms_agent_cmd_mock(); });

  char cmd[MAX_CMD_LEN];
  char out[MAX_OUTPUT_LEN];

  snprintf(cmd, sizeof(cmd), "%s %d fetchKey %s", kms_agent_cmd, mysqld_port,
           id);

  size_t ret_len = execute_cmd(cmd, out, sizeof(out));

  if (ret_len == 0) return 0;  // Err logged in execute_cmd

  assert(key_buff_len == AES_KEY_LENGTH);

  // Key content is encoded using base64.
  uint64 decoded_len = base64_needed_decoded_length(ret_len);
  char *tmp = keyring_rds_malloc<char *>(decoded_len);
  if (tmp == NULL) {
    Logger::log(ERROR_LEVEL, ER_SERVER_OUTOFMEMORY, decoded_len);
    return 0;
  }

  int64 len = base64_decode(out, ret_len, tmp, NULL, 0);
  if (len <= 0) {
    my_free(tmp);
    Logger::log(ERROR_LEVEL, "base64 decode failed [%s]", out);
    return 0;
  }

  // Make key to 32 bytes
  standardized_aes_key((const uchar *)tmp, (size_t)len, (uchar *)key_buff);
  my_free(tmp);

  return AES_KEY_LENGTH;
}

}  // namespace keyring_rds
