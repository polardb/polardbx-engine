/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  dst_cli_sdk.cc,v 1.0 08/06/2016 04:53:21 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file dst_cli_sdk.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/25/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "dst_cli_sdk.h"
#include <gflags/gflags.h>
#include <algorithm>
#include <boost/lexical_cast.hpp>  
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>

DECLARE_string(server_members);

namespace alisql {

RaftGroup::RaftGroup(const std::string &groupName, 
                     const std::vector<std::string> &members)
                     : groupName_(groupName)
{
  cliSdk_= new CliSDK(members);
}

RaftGroup::~RaftGroup()
{
  if (cliSdk_ != NULL)
  {
    delete cliSdk_;
    cliSdk_= NULL;
  }
}

DstCliSDK::DstCliSDK(const std::string &raftGroupInfo) 
          :casUnique_(0), memc_(NULL), rc_(MEMCACHED_SUCCESS), raftGroupInfo_(raftGroupInfo)
{
}

DstCliSDK::~DstCliSDK()
{
  if (memc_ != NULL)
  {
    memcached_free(memc_);
    memc_= NULL;  
  }

  for (int i=0; i < raftGroups_.size(); i++)
  {
    RaftGroup* raftGroup= raftGroups_[i];
    if (raftGroup != NULL)
    {
      delete raftGroup;
    }
  }
}

int DstCliSDK::parseArgs()
{
  std::vector<std::string> raftGroups;
  boost::split(raftGroups, raftGroupInfo_,
               boost::is_any_of(";"), boost::token_compress_on);
  for (int i= 0; i < raftGroups.size(); i++)
  {
    std::string raftGroup= raftGroups[i];
    std::vector<std::string> members;
    boost::split(members, raftGroup,
                 boost::is_any_of(","), boost::token_compress_on);
    if (members.size() < 2)
    {
      LOG_INFO("invalid raft group and member name, please check your configuration\n");
      return -1;
    }
    std::string groupName= members[0];
    members.erase(members.begin());
    RaftGroup* raftGroupPtr= new RaftGroup(groupName, members);
    raftGroups_.push_back(raftGroupPtr);
  }
  return 0;
}

int DstCliSDK::init()
{
  int rc= parseArgs();
  if (rc != 0)
  {
    LOG_INFO("DstCliSDK init failed\n");
    return rc;
  }

  memc_= memcached_create(NULL);
  
  rc_= memcached_behavior_set_distribution(memc_, 
                MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA);  
  if (!memcached_success(rc_)) 
  {
    LOG_INFO("memcached_behavior_set_distribution failed\n");
    return -1;
  }
  
  std::string raftGroupNames;

  for (int i= 0; i < raftGroups_.size(); i++)
  {
    if (i != 0)
    {
      raftGroupNames.append(",");
    }
    raftGroupNames.append(raftGroups_[i]->getGroupName());
  }

  memcached_server_st *server_pool;
  server_pool = memcached_servers_parse(raftGroupNames.c_str());
  
  rc_= memcached_server_push(memc_, server_pool);
  if (!memcached_success(rc_)) 
  {
    LOG_INFO("memcached_server_push failed\n");
    return -1;
  }
  memcached_server_list_free(server_pool);
}

int DstCliSDK::put(const std::string &key, std::string &value)
{
  return process(Put, key, value);    
}

int DstCliSDK::get(const std::string &key, std::string &value)
{
  return process(Get, key, value);    
}

int DstCliSDK::del(const std::string &key)
{
  std::string value;
  return process(Del, key, value);
}

int DstCliSDK::gets(const std::string &key, std::string &value)
{
  return process(Gets, key, value);  
}

int DstCliSDK::cas(const std::string &key, std::string &value)
{
  return process(Cas, key, value);  
}

int DstCliSDK::showStats()
{ 
  for (int i= 0; i < raftGroups_.size(); i++)
  {
    RaftGroup* raftGroup= raftGroups_[i];
    printf("Raft group: %s\n", raftGroup->getGroupName().c_str());
    raftGroup->getCliSdk()->showStats();
  }

  return 0;
}

int DstCliSDK::process(SDKOpr opr, const std::string &key, 
                       std::string &value)
{
  uint32_t groupKey= memcached_generate_hash(memc_, key.c_str(), key.length());
  
  if (groupKey > raftGroups_.size())
  {
    LOG_INFO("group key is larger than the size of raft group! \n");
    return -1;
  }
  RaftGroup* raftGroup= raftGroups_[groupKey];
  
  if (opr == Cas)
  {
    raftGroup->getCliSdk()->setCasUnique(casUnique_);
  }
  int ret= raftGroup->getCliSdk()->process(opr, key, value);  
  if (opr == Gets)
  {
    casUnique_= raftGroup->getCliSdk()->getCasUnique();
  }
  
  rc_= raftGroup->getCliSdk()->getMemcachedRc();
  return ret;
}
} //namespace alisql