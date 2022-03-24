/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  dst_cli_sdk.h,v 1.0 08/26/2016 02:46:50 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file dst_cli_sdk.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/26/2016 02:46:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef  cluster_dst_cli_sdk_INC
#define  cluster_dst_cli_sdk_INC

#include "cli_sdk.h"

namespace alisql {

/**
 * @class dst_cli_sdk
 *
 * @brief class for dst_cli_sdk
 *
 **/
class RaftGroup
{
  public:
    RaftGroup(const std::string &groupName, 
              const std::vector<std::string> &members);
    virtual ~RaftGroup();
    CliSDK* getCliSdk() {return cliSdk_;}
    std::string getGroupName() {return groupName_;}
  private:
    std::string groupName_;
    CliSDK *cliSdk_;
};

// Not thread safe
class DstCliSDK
{
public:
  DstCliSDK(const std::string &raftGroupInfo);
  virtual ~DstCliSDK();

  int put(const std::string &key, std::string &value);
  int get(const std::string &key, std::string &value);
  int gets(const std::string &key, std::string &value);
  int cas(const std::string &key, std::string &value);
  int del(const std::string &key);
  int showStats();
  int process(SDKOpr opr, const std::string &key,
              std::string &value);
  int init();
  int parseArgs();
  memcached_return getMemcachedRc() {return rc_;}
  uint64_t getCasUnique() {return casUnique_;}
  void setCasUnique(uint64_t cas) {casUnique_ = cas;}

private:
  memcached_st *memc_;
  memcached_return rc_;
  uint64_t casUnique_;
  std::vector<RaftGroup*> raftGroups_;
  std::string raftGroupInfo_;
};

};/* end of namespace alisql */

#endif     //#ifndef cluster_dst_cli_sdk_INC
