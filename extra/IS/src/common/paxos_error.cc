/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_error.cc,v 1.0 May 8, 2019 10:29:12 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_error.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date May 8, 2019 10:29:12 AM
 * @version 1.0
 * @brief
 *
 **/

#include "paxos_error.h"

namespace alisql {

static const char *paxos_error_msg[PE_TOTAL] = {
    "Success.",                                              // PE_NONE
    "Some error happens, please check the error log.",       // PE_DEFAULT
    "Current node is not a leader.",                         // PE_NOTLEADER
    "Target node not exists.",                               // PE_NOTFOUND
    "Target node already exists.",                           // PE_EXISTS
    "A concurrent command is running, please retry later.",  // PE_CONFLICTS
    "This node delays too much.",                            // PE_DELAY
    "Invalid argument, please check the error log.",  // PE_INVALIDARGUMENT
    "Timeout.",                                       // PE_TIMEOUT
    "Replicate log fail.",                            // PE_REPLICATEFAIL
    "This node is already a learner.",                // PE_DOWNGRADLEARNER
    "Downgrade a leader is not allowed.",             // PE_DOWNGRADELEADER
    "Configure forcesync or weight to a learner is not allowed.",  // PE_WEIGHTLEARNER
    "Leader transfer to a learner is not allowed.",  // PE_NOTFOLLOWER
    "For test."                                      // PE_TEST
};

const char *pxserror(int error_code) {
  // TODO: refactor all return code to PaxosErrorCode
  if (error_code < 0) return paxos_error_msg[PE_DEFAULT];
  // Invalid PaxosErrorCode case
  if (error_code >= PE_TOTAL) return "Unknown error.";
  const char *s = paxos_error_msg[error_code];
  // nullptr case should return default errmsg
  return s ? s : paxos_error_msg[PE_DEFAULT];
}

const char *pxserror() { return pxserror(PE_DEFAULT); }

}  // namespace alisql
