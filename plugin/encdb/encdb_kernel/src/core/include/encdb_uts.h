/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include "EncDBConfig.h"
#include "encdb_error.h"
/**
 * @brief process request message from client.
 *
 * @param msgOut        output message.
 * @param pMsgOutLen    specify length of output memory \c msgOut
 * @param msgIn         input message, json-format string
 * @param msgInLen      input message length
 * @return EncdbStatus
 */
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
EncdbStatus encdb_process_msg_internal(std::string &msgOut,
                                       const std::string &msgIn,
                                       const std::string &username);
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
EncdbStatus encdb_process_msg_internal(std::string &msgOut,
                                       const std::string &msgIn,
                                       const std::string &username,
                                       const std::string &old_mac,
                                       std::string &new_mac);
#endif
