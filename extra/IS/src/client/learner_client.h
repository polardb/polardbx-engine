/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  learner_client.h,v 1.0 12/27/2016 10:33:49 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file learner_client.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/27/2016 10:33:49 AM
 * @version 1.0
 * @brief
 *
 **/

#ifndef CONSENSUS_CLIENT_LEARNER_CLIENT_H_
#define CONSENSUS_CLIENT_LEARNER_CLIENT_H_

#include <string>
#include "paxos.pb.h"
// #include "witness.h"

using std::string;

namespace alisql {
class Witness;

class RemoteLeader {
 public:
  string ip;
  uint64_t port;
  string user;
  string passwd;
  RemoteLeader(string ip, uint64_t port, string user, string passwd)
      : ip(ip), port(port), user(user), passwd(passwd){};
};

/* just give another name */
typedef RemoteLeader ServerMeta;

/*
 * copy from consensus_log_manager.h, flag is set in LogEntry.info()
 * flag is used for blob
 */
enum Logentry_info_flag {
  FLAG_GU1 = 1,
  FLAG_GU2 = 1 << 1, /* FLAG_GU2 = 3 - FLAG_GU1 */
  FLAG_LARGE_TRX = 1 << 2,
  FLAG_LARGE_TRX_END = 1 << 3,
  FLAG_CONFIG_CHANGE = 1 << 4,
  FLAG_BLOB = 1 << 5,
  FLAG_BLOB_END = 1 << 6,
  FLAG_BLOB_START = 1 << 7 /* we should mark the start for SDK */
};

class LearnerClient {
 public:
  typedef enum ReadErrorCode {
    RET_SUCCESS = 0,
    RET_TIMEOUT,
    RET_ERROR,
    RET_WAIT_BLOB,
  } ReadError;

  LearnerClient();
  virtual ~LearnerClient();

  /**
   * Register and Deregister function for an AliSQL Learner
   * @leader remote leader AliSQL server
   * @addr learner listen address
   */
  int registerLearner(const RemoteLeader &leader, const string &addr);
  int deregisterLearner(const RemoteLeader &leader, const string &addr);
  /**
   * Open a learner client and receive log from a given logindex
   * @addr learner listen address
   * @clusterId cluster id of leader
   * @lastLogIndex client will receive log after lastLogIndex
   * @readTimeout ms, timeout of read, 0 means never timeout
   * @cacheSize cache size for log, if cache full, no more log can be appended
   **/
  int open(const string &addr, uint64_t clusterId, uint64_t lastLogIndex = 0,
           uint64_t readTimeout = 0, uint64_t cacheSize = 1000);
  /*
   * Open a learner client and receive log from a given timestamp
   */
  int openWithTimestamp(const string &addr, uint64_t clusterId,
                        const RemoteLeader &leader, uint64_t lastTimestamp,
                        uint64_t readTimeout = 0, uint64_t cacheSize = 1000);

  /*
   * Get the log index just same or after the given timestamp.
   */
  uint64_t getLogIndexByTimestamp(const RemoteLeader &leader,
                                  uint64_t lastTimestamp);
  /**
   * do not rely on an index because the index may not successive after a
   *restart read just return the next log in the cache For blob, le.value() is
   *empty until we receive the complete data
   **/
  int read(LogEntry &le);
  int close();
  /**
   * get real current leader
   * @meta learner source
   */
  ServerMeta getRealCurrentLeader(const ServerMeta &meta);

 protected:
  Witness *node_;
  uint64_t lastLogIndex_; /* the last log index has been popped */
  uint64_t readTimeout_;
  std::string blobCache_;
};

}  // namespace alisql

#endif /* CONSENSUS_CLIENT_LEARNER_CLIENT_H_ */
