/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_error.h,v 1.0 May 8, 2019 10:09:27 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_error.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date May 8, 2019 10:09:27 AM
 * @version 1.0
 * @brief
 *
 **/
#ifndef CONSENSUS_INCLUDE_PAXOS_ERROR_H_
#define CONSENSUS_INCLUDE_PAXOS_ERROR_H_

namespace alisql {

enum PaxosErrorCode : int {
  PE_NONE,
  PE_DEFAULT,
  PE_NOTLEADR,
  PE_NOTFOUND,
  PE_EXISTS,
  PE_CONFLICTS,
  PE_DELAY,
  PE_INVALIDARGUMENT,
  PE_TIMEOUT,
  PE_REPLICATEFAIL,
  PE_DOWNGRADLEARNER,
  PE_DOWNGRADELEADER,
  PE_WEIGHTLEARNER,
  PE_NOTFOLLOWER,
  PE_TEST,
  PE_TOTAL
};

const char *pxserror(int error_code);
const char *pxserror();

}  // namespace alisql

#endif /* CONSENSUS_INCLUDE_PAXOS_ERROR_H_ */
