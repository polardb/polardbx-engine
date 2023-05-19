/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  learner_client.cc,v 1.0 12/27/2016 10:42:18 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file learner_client.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/27/2016 10:42:18 AM
 * @version 1.0
 * @brief
 *
 **/

#include <cstdlib>
#include <memory>
// #include <my_global.h>
#include <mysql.h>
#include "learner_client.h"
#include "mem_paxos_log.h"
#include "paxos_log.h"
#include "witness.h"

namespace alisql {

LearnerClient::LearnerClient()
    : node_(nullptr), lastLogIndex_(0), readTimeout_(0) {}

LearnerClient::~LearnerClient() { close(); }

int LearnerClient::registerLearner(const RemoteLeader &leader,
                                   const string &addr) {
  MYSQL *conn = mysql_init(NULL);
  if (!mysql_real_connect(conn, leader.ip.c_str(), leader.user.c_str(),
                          leader.passwd.c_str(), NULL, leader.port, NULL, 0)) {
    fprintf(stderr, "Failed to connect to database: Error: %s\n",
            mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  char query[100];
  snprintf(query, 100, "add consensus_learner \"%s\"", addr.c_str());
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "Failed to execute query %s : Error: %s\n", query,
            mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  mysql_close(conn);
  return 0;
}

int LearnerClient::deregisterLearner(const RemoteLeader &leader,
                                     const string &addr) {
  MYSQL *conn = mysql_init(NULL);
  if (!mysql_real_connect(conn, leader.ip.c_str(), leader.user.c_str(),
                          leader.passwd.c_str(), NULL, leader.port, NULL, 0)) {
    fprintf(stderr, "Failed to connect to database: Error: %s\n",
            mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  char query[100];
  snprintf(query, 100, "drop consensus_learner \"%s\"", addr.c_str());
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "Failed to execute query %s : Error: %s\n", query,
            mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  mysql_close(conn);
  return 0;
}

int LearnerClient::open(const string &addr, uint64_t clusterId,
                        uint64_t lastLogIndex, uint64_t readTimeout,
                        uint64_t cacheSize) {
  /* It will always use MemPaxosLog */
  std::shared_ptr<MemPaxosLog> memlog =
      std::make_shared<MemPaxosLog>(lastLogIndex, readTimeout, cacheSize);
  node_ = new Witness(addr, memlog, clusterId);
  lastLogIndex_ = lastLogIndex;
  readTimeout_ = readTimeout;
  return 0;
}

int LearnerClient::openWithTimestamp(const string &addr, uint64_t clusterId,
                                     const RemoteLeader &leader,
                                     uint64_t lastTimestamp,
                                     uint64_t readTimeout, uint64_t cacheSize) {
  /* TODO not implemented */
  /* step 1: Parse timestamp to logindex */
  uint64_t lastLogIndex = getLogIndexByTimestamp(leader, lastTimestamp);
  if (lastLogIndex == 0) {
    fprintf(stderr, "Get log index from timestamp fail.\n");
    return -1;
  }

  /* step 2: Open with lastLogIndex */
  open(addr, clusterId, lastLogIndex - 1, readTimeout, cacheSize);
  return 0;
}

uint64_t LearnerClient::getLogIndexByTimestamp(const RemoteLeader &leader,
                                               uint64_t lastTimestamp) {
  MYSQL *conn = mysql_init(NULL);
  if (!mysql_real_connect(conn, leader.ip.c_str(), leader.user.c_str(),
                          leader.passwd.c_str(), NULL, leader.port, NULL, 0)) {
    fprintf(stderr, "Failed to connect to database: Error: %s\n",
            mysql_error(conn));
    mysql_close(conn);
    return 0;
  }
  char query[100];
  snprintf(query, 100, "show consensus_index %lu", lastTimestamp);
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "Failed to execute query %s : Error: %s\n", query,
            mysql_error(conn));
    mysql_close(conn);
    return 0;
  }
  MYSQL_RES *myRes = mysql_store_result(conn);
  if (myRes == NULL) {
    fprintf(stderr, "Failed to fetch result: Error: %s\n", mysql_error(conn));
    mysql_close(conn);
    return 0;
  }
  MYSQL_ROW myRow = mysql_fetch_row(myRes);
  if (myRow == NULL || mysql_num_fields(myRes) == 0) {
    fprintf(stderr, "Failed to fetch logindex in the result\n");
    mysql_free_result(myRes);
    mysql_close(conn);
    return 0;
  }
  /* Should support ulonglong. */
  uint64_t lastLogIndex = strtoull(myRow[0], NULL, 10);
  mysql_free_result(myRes);
  mysql_close(conn);

  return lastLogIndex;
}

int LearnerClient::read(LogEntry &le) {
  if (node_ == nullptr) return RET_ERROR;
  auto log = node_->getLog();
  int ret = log->getEntry(le);
  if (ret != 0) {
    return RET_TIMEOUT;
  } else {
    lastLogIndex_ = le.index();
    /* updateAppliedIndex after read */
    if (node_) node_->updateAppliedIndex(lastLogIndex_);
    /* extra check, avoid reading half blob */
    if ((le.info() &
         (Logentry_info_flag::FLAG_BLOB | Logentry_info_flag::FLAG_BLOB_END)) &&
        blobCache_.length() == 0 &&
        !(le.info() & Logentry_info_flag::FLAG_BLOB_START)) {
      fprintf(
          stderr,
          "Current index may cause a broken blob data (index: %lu, flag: %lu). "
          "Try an early lastLogIndex to init LearnerClient lastLogIndex.\n",
          le.index(), le.info());
      node_->resetLastLogIndex(le.index() - 2);
      return RET_WAIT_BLOB;
    }
    if (le.info() & Logentry_info_flag::FLAG_BLOB_END) {
      blobCache_.append(le.value());
      le.set_value(blobCache_);
      blobCache_.clear();
    } else if (le.info() & Logentry_info_flag::FLAG_BLOB) {
      blobCache_.append(le.value());
      le.set_value("");
    }
    return RET_SUCCESS;
  }
}

int LearnerClient::close() {
  if (node_ != nullptr) {
    delete node_;
    node_ = nullptr;
  }
  return 0;
}

ServerMeta LearnerClient::getRealCurrentLeader(const ServerMeta &meta) {
  ServerMeta current = meta;
  while (1) {
    MYSQL *conn = mysql_init(NULL);
    if (!mysql_real_connect(conn, current.ip.c_str(), current.user.c_str(),
                            current.passwd.c_str(), NULL, current.port, NULL,
                            0)) {
      fprintf(stderr, "Failed to connect to database: Error: %s\n",
              mysql_error(conn));
      mysql_close(conn);
      return current;
    }
    char query[100];
    snprintf(query, 100,
             "select current_leader from "
             "information_schema.alisql_cluster_local limit 1");
    if (mysql_query(conn, query) != 0) {
      fprintf(stderr, "Failed to execute query %s : Error: %s\n", query,
              mysql_error(conn));
      mysql_close(conn);
      return current;
    }
    MYSQL_RES *myRes = mysql_store_result(conn);
    if (myRes == NULL) {
      fprintf(stderr, "Failed to fetch result: Error: %s\n", mysql_error(conn));
      mysql_close(conn);
      return current;
    }
    MYSQL_ROW myRow = mysql_fetch_row(myRes);
    if (myRow == NULL || mysql_num_fields(myRes) == 0) {
      fprintf(stderr, "Failed to fetch current leader in the result\n");
      mysql_free_result(myRes);
      mysql_close(conn);
      return current;
    }
    /* Should support ulonglong. */
    string sqlres = myRow[0];
    auto pos = sqlres.find_last_of(':');
    if (pos == std::string::npos) {
      fprintf(stderr, "Failed to resolve ip:port\n");
      mysql_free_result(myRes);
      mysql_close(conn);
      return current;
    }
    string newIp = sqlres.substr(0, pos);
    /* alisql port = paxos port - 8000 */
    uint64_t newPort =
        strtoull(sqlres.substr(pos + 1).c_str(), NULL, 10) - 8000;
    fprintf(stderr, "Update Current ip(%s -> %s) port(%lu -> %lu)\n",
            current.ip.c_str(), newIp.c_str(), current.port, newPort);
    if (newIp == current.ip && newPort == current.port) {
      mysql_free_result(myRes);
      mysql_close(conn);
      break;
    }
    current.ip = newIp;
    current.port = newPort;
    mysql_free_result(myRes);
    mysql_close(conn);
  }
  return current;
}

}  // namespace alisql
