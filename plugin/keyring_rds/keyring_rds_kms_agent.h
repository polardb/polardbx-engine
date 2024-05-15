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

#ifndef PLUGIN_KEYRING_RDS_KEYRING_RDS_KMS_H_
#define PLUGIN_KEYRING_RDS_KEYRING_RDS_KMS_H_

namespace keyring_rds {

/** KMS agent local command */
extern char *kms_agent_cmd;

/** Local command execution timeout */
extern int cmd_timeout_sec;

extern bool validate_cached_key;

/**
  Accessor to KMS/Agent.
*/
class KMS_Agent {
 public:
  /**
    Request KMS to generate a new master key.

    @para[out] id_buff      Buffer storing the generated master key id
    @para[in]  id_buff_len  Buffer Length
    @return <=0   Error occurs
    @return  >0   The actual length of the generated master key id
  */
  size_t generate(char *id_buff, size_t id_buff_len);

  /**
    Fetch the master key specified by id from KMS .

    @para[in]  id            Specified master key id
    @para[out] key_buff      Buffer storing the master key content
    @para[in]  key_buff_len  Buffer Length
    @return <=0   Error occurs
    @return  >0   The actual length of the master key
  */
  size_t fetch(const char *id, char *key_buff, size_t key_buff_len);
};

extern KMS_Agent *kms_agent;  // The single global instance

}  // namespace keyring_rds

#endif
