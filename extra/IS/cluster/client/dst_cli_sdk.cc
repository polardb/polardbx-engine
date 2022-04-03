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

RaftGroup::PaxosGroup(const std::string &groupName, 
                     const std::vector<std::string> &members)
                     : groupName_(groupName)
{
  cliSdk_= new CliSDK(members);
}

RaftGroup::~PaxosGroup()
{
  if (cliSdk_ != NULL)
  {
    delete cliSdk_;
    cliSdk_= NULL;
  }
}

DstCliSDK::DstCliSDK(const std::string &paxosGroupInfo) 
          :casUnique_(0), memc_(NULL), rc_(MEMCACHED_SUCCESS), paxosGroupInfo_(paxosGroupInfo)
{
}

DstCliSDK::~DstCliSDK()
{
  if (memc_ != NULL)
  {
    memcached_free(memc_);
    memc_= NULL;  
  }

  for (int i=0; i < paxosGroups_.size(); i++)
  {
    PaxosGroup* paxosGroup= paxosGroups_[i];
    if (paxosGroup != NULL)
    {
      delete paxosGroup;
    }
  }
}

int DstCliSDK::parseArgs()
{
  std::vector<std::string> paxosGroups;
  boost::split(paxosGroups, paxosGroupInfo_,
               boost::is_any_of(";"), boost::token_compress_on);
  for (int i= 0; i < paxosGroups.size(); i++)
  {
    std::string paxosGroup= paxosGroups[i];
    std::vector<std::string> members;
    boost::split(members, paxosGroup,
                 boost::is_any_of(","), boost::token_compress_on);
    if (members.size() < 2)
    {
      LOG_INFO("invalid paxos group and member name, please check your configuration\n");
      return -1;
    }
    std::string groupName= members[0];
    members.erase(members.begin());
    PaxosGroup* paxosGroupPtr= new PaxosGroup(groupName, members);
    paxosGroups_.push_back(paxosGroupPtr);
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
  
  std::string paxosGroupNames;

  for (int i= 0; i < paxosGroups_.size(); i++)
  {
    if (i != 0)
    {
      paxosGroupNames.append(",");
    }
    paxosGroupNames.append(paxosGroups_[i]->getGroupName());
  }

  memcached_server_st *server_pool;
  server_pool = memcached_servers_parse(paxosGroupNames.c_str());
  
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
  for (int i= 0; i < paxosGroups_.size(); i++)
  {
    PaxosGroup* paxosGroup= paxosGroups_[i];
    printf("Paxos group: %s\n", paxosGroup->getGroupName().c_str());
    paxosGroup->getCliSdk()->showStats();
  }

  return 0;
}

int DstCliSDK::process(SDKOpr opr, const std::string &key, 
                       std::string &value)
{
  uint32_t groupKey= memcached_generate_hash(memc_, key.c_str(), key.length());
  
  if (groupKey > paxosGroups_.size())
  {
    LOG_INFO("group key is larger than the size of paxos group! \n");
    return -1;
  }
  PaxosGroup* paxosGroup= paxosGroups_[groupKey];
  
  if (opr == Cas)
  {
    paxosGroup->getCliSdk()->setCasUnique(casUnique_);
  }
  int ret= paxosGroup->getCliSdk()->process(opr, key, value);  
  if (opr == Gets)
  {
    casUnique_= paxosGroup->getCliSdk()->getCasUnique();
  }
  
  rc_= paxosGroup->getCliSdk()->getMemcachedRc();
  return ret;
}
} //namespace alisql
