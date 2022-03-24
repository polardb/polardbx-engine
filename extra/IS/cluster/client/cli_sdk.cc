/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  cli_sdk.cc,v 1.0 08/06/2016 04:53:21 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file cli_sdk.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/25/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "cli_sdk.h"
#include <gflags/gflags.h>
#include <algorithm>
#include <boost/lexical_cast.hpp>  
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>

DECLARE_string(server_members);

namespace alisql {
static memcached_return_t stat_printer(const memcached_instance_st * instance,
                                       const char *key, size_t key_length,
                                       const char *value, size_t value_length,
                                       void *context)
{
  static const memcached_instance_st * last= NULL;
  bool *isLeader = (bool*)context;

  printf("\t %.*s: %.*s\n", (int)key_length, key, (int)value_length, value);
  if (strcmp(key, "role") == 0 && strcmp(value, "LEADER") == 0 && isLeader != NULL)
  {
    *isLeader= true;
  }

  return MEMCACHED_SUCCESS;
}


int CliSDK::parseArgs(int argc, char* argv[], 
                      std::vector<std::string> &members)
{
  google::ParseCommandLineFlags(&argc, &argv, true);
  boost::split(members, FLAGS_server_members,
               boost::is_any_of(","), boost::token_compress_on);
  if (members.size() == 0) 
  {
    LOG_INFO("server members is empty , please check your configuration\n");
    return -1;
  }
  return 0;
}

CliSDK::CliSDK(const std::vector<std::string> &members) :
  stop_(false), memc_(NULL), casUnique_(0), rc_(MEMCACHED_SUCCESS)
{
  init(members);
}

CliSDK::~CliSDK()
{
  if (memc_ != NULL)
  {
    memcached_free(memc_);  
    memc_= NULL;
  }
}

void CliSDK::init(const std::vector<std::string> &members)
{
  std::copy(members.begin(), members.end(), std::back_inserter(members_));
}

void CliSDK::sortMemberList(std::vector<std::string>& memberList)
{
  if (!leaderId_.empty())
  {
    memberList.push_back(leaderId_);
  }
  std::copy(members_.begin(), members_.end(),
            std::back_inserter(memberList));            
}

int CliSDK::memConnect(const char *hostname, int port)
{
  if (memc_ != NULL)
  {
    //LOG_INFO("memc_ is not NULL\n");
    return -1;
  }
  memcached_return rc;  
  memcached_server_st *server;
  memc_= memcached_create(NULL);  
  server= memcached_server_list_append(NULL, hostname, port, &rc);
  if (rc != MEMCACHED_SUCCESS)
  {
    //LOG_INFO("memcached_server_list_append fail\n");
    return -1;
  }
  rc= memcached_server_push(memc_, server);
  if (rc != MEMCACHED_SUCCESS)
  {
    //LOG_INFO("memcached_server_push fail\n");    
    return -1;
  }
  memcached_server_list_free(server);
}

int CliSDK::processInternal(SDKOpr opr, const std::string &key, 
                            std::string &value)
{
  std::string oprStr;
  if (opr == Put)
  {
    rc_= memcached_set(memc_, key.c_str(), key.length(),
                     value.c_str(), value.length(), 0, 0);  
  }
  else if (opr == Del)
  {
    rc_= memcached_delete(memc_, key.c_str(), key.length(), 0);
  }
  else if (opr == Get)
  {
    uint32_t flags;
    size_t value_length= 0;
    memcached_behavior_set(memc_, MEMCACHED_BEHAVIOR_SUPPORT_CAS, false);
    char* result= memcached_get(memc_, key.c_str(),
                                key.length(), &value_length,
                                &flags, &rc_);
    if (result != NULL)
    {
      value= result;
    }
  }
  else if (opr == Gets)
  {
    size_t keyLength= key.length();
    const char *p= reinterpret_cast<const char*>(&key[0]);
    memcached_behavior_set(memc_, MEMCACHED_BEHAVIOR_SUPPORT_CAS, true);
    rc_= memcached_mget(memc_, &p, &keyLength, 1);
    
    memcached_result_st results_obj;
    memcached_result_st *results= memcached_result_create(memc_, &results_obj);

    if (results == NULL)
    {
      //LOG_INFO("Fail to create result\n"); 
      return -1; 
    }

    results= memcached_fetch_result(memc_, &results_obj, &rc_);
    if (results == NULL || rc_ != MEMCACHED_SUCCESS)
    {
      //LOG_INFO("Fail to get result\n"); 
      return -1; 
    }
    value= memcached_result_value(results);
    casUnique_= memcached_result_cas(results);

    memcached_result_free(&results_obj);
  }
  else if (opr == Cas)
  {
    rc_= memcached_cas(memc_, key.c_str(), key.length(),
                       value.c_str(), value.length(), 0, 0, casUnique_);
  }
  
  if (memcached_success(rc_)) 
  {
    return 0;
  }
  // Route the request to another server if this member is not leader now
  // or it is offline
  else
  {
    return -1;
  }
}

int CliSDK::put(const std::string &key, std::string &value)
{
  return process(Put, key, value);    
}

int CliSDK::get(const std::string &key, std::string &value)
{
  return process(Get, key, value);    
}

int CliSDK::del(const std::string &key)
{
  std::string value;
  return process(Del, key, value);
}

int CliSDK::gets(const std::string &key, std::string &value)
{
  return process(Gets, key, value);  
}

int CliSDK::cas(const std::string &key, std::string &value)
{
  return process(Cas, key, value);  
}

int CliSDK::showStats()
{ 
  for (int i= 0; i < members_.size(); i++)
  {
    std::string memberId= members_[i];
    memcached_server_st* servers= memcached_servers_parse(memberId.c_str());
    if (servers == NULL || memcached_server_list_count(servers) == 0)
    {
      LOG_INFO("Invalid server provided: %s \n", memberId.c_str());  
      return -1;
    }

    memcached_st *memc= memcached_create(NULL);

    memcached_return_t rc= memcached_server_push(memc, servers);
    memcached_server_list_free(servers);

    if (rc != MEMCACHED_SUCCESS && rc != MEMCACHED_SOME_ERRORS)
    { 
      return -1;
    }
    printf("-----------------------------------------\n");
    printf("Server: %s\n", memberId.c_str());
    printf("Local stats info:\n");
    bool isLeader= false;
    rc= memcached_stat_execute(memc, "local", stat_printer, &isLeader);
    if (memcached_fatal(rc))
    {
      printf("\t is offline \n");
    }
    
    if (isLeader)
    {
      printf("Cluster stats info:\n");
      rc= memcached_stat_execute(memc, "cluster", stat_printer, NULL);
    }
    memcached_free(memc);
  }

  return 0;
}

int CliSDK::parseIpPort(std::string &ipPortStr, std::string *ip, int *port)
{
  try {
    std::vector<std::string> ipPort;
    boost::split(ipPort, ipPortStr,
                 boost::is_any_of(":"), boost::token_compress_on);
    if (ipPort.size() < 2) 
    {
      LOG_INFO("Member address %s format error, please check your configuration!\n", ipPortStr.c_str());  
      return -1;
    }

    *ip= ipPort[0];
    std::string portStr= ipPort[1];
    *port= boost::lexical_cast<int>(portStr);
  }
  catch(std::exception& e)
  {
    printf("Parse address failed \n");
    return -1;
  }
  return 0;
}

int CliSDK::process(SDKOpr opr, const std::string &key, 
                    std::string &value)
{
  int ret= 0;

  if (!leaderId_.empty() && memc_ != NULL)
  {
    ret= processInternal(opr, key, value);
    if (ret == 0)
    {
      return 0;
    }

    if (opr == Cas && rc_ == MEMCACHED_DATA_EXISTS)
    {
      return -1;
    }
  }

  for (int i= 0; i < members_.size(); i++)
  {
    std::string memberId= members_[i];
    std::string ip;
    int port;
    ret= parseIpPort(memberId, &ip, &port);
    if (ret != 0)
    { 
      LOG_INFO("Invalid address format for %s!\n", memberId.c_str());   
      continue;
    }

    if (memc_ != NULL)
    {
      memcached_free(memc_); 
      memc_= NULL;
    }

    ret= memConnect(ip.c_str(), port);
    if (memc_ == NULL) 
    {
      continue;
    }

    ret= processInternal(opr, key, value);
    if (ret == 0)
    {
      leaderId_= memberId;
      return ret;
    }

    if (opr == Cas && rc_ == MEMCACHED_DATA_EXISTS)
    {
      return -1;
    }
  }

  return ret;
}
} //namespace alisql