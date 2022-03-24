/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  cli_sdk.h,v 1.0 08/26/2016 02:46:50 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file cli_sdk.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/26/2016 02:46:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef  cluster_cli_sdk_INC
#define  cluster_cli_sdk_INC

#include <vector>
#include <string>
#include <libmemcached/memcached.h>
#include "../service/state_machine_service.h"
#include "raft.h"

namespace alisql {

/**
 * @class cli_sdk
 *
 * @brief class for cli_sdk
 *
 **/
enum SDKOpr {
    Get= 0,
    Put= 1,
    Del= 2,
    Gets= 3,
    Cas= 4
};

// Not thread safe
class CliSDK
{
public:
  static int parseArgs(int argc, char* argv[],
                       std::vector<std::string> &members);
  CliSDK(const std::vector<std::string> &members);
  virtual ~CliSDK();

  int put(const std::string &key, std::string &value);
  int get(const std::string &key, std::string &value);
  int gets(const std::string &key, std::string &value);
  int cas(const std::string &key, std::string &value);
  int del(const std::string &key);
  int showStats();
  int process(SDKOpr opr, const std::string &key,
              std::string &value);
  int processInternal(SDKOpr opr, const std::string &key,
                      std::string &value);
  void init(const std::vector<std::string> &members);
  int memConnect(const char *hostname, int port);
  int parseIpPort(std::string &ipPortStr, std::string *ip, int *port);
  uint64_t getCasUnique() {return casUnique_;}
  void setCasUnique(uint64_t cas) {casUnique_ = cas;}
  memcached_return getMemcachedRc() {return rc_;}

private:
  void sortMemberList(std::vector<std::string> &memberList);
  std::string leaderId_;
  std::vector<std::string> members_;
  bool stop_;
  memcached_st *memc_;
  uint64_t casUnique_;
  memcached_return rc_;
};

};/* end of namespace alisql */

#endif     //#ifndef cluster_cli_sdk_INC
