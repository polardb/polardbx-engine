/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  service.cc,v 1.0 08/01/2016 10:59:50 AM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file state_machine_service.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/06/2016 11:59:50 AM
 * @version 1.0
 * @brief implement of StateMachineService
 *
 **/

#include <stdlib.h>
#include "state_machine_service.h"
#include <boost/lexical_cast.hpp>  
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <gflags/gflags.h>

DECLARE_uint64(work_thread_num);

namespace alisql {

StateMachineService::StateMachineService(): Service()
{
}

StateMachineService::~StateMachineService()
{
}

int StateMachineService::shutdown()
{
  st_->shutdown();
  net_->shutdown();
  return 0;
}

int StateMachineService::init(std::string &serverId,
                              const std::vector<std::string> &serverMembers,
                              const std::vector<std::string> &rpcMembers)
{
  int ret= 0;

  ret= initNet(serverId);
  if (ret == -1) 
  {
    LOG_INFO("Init net failed!\n");
    return ret;
  }

  ret= initStateMachine(serverId, serverMembers, rpcMembers);
  if (ret == -1) 
  {
    LOG_INFO("Init state machine failed!\n");
    return ret;
  }
  
  return ret;
}

int StateMachineService::initNet(std::string &serverId) 
{
  net_= std::shared_ptr<MemcachedEasyNet>(new MemcachedEasyNet());

  std::vector<std::string> selfIpPort;
  boost::split(selfIpPort, serverId, boost::is_any_of(":"), boost::token_compress_on);
  if (selfIpPort.size() < 2)
  {
    LOG_INFO("incorrect ip port format!\n");
    return -1;
  }
  int port= boost::lexical_cast<int>(selfIpPort[1]);

  workPool_= easy_thread_pool_create(net_->getEio(), FLAGS_work_thread_num, 
                                     StateMachineService::process, NULL);
  firstThread_= getFirstThread(workPool_);
  net_->setWorkPool(workPool_);
  net_->init(this);
  net_->start(port);

  LOG_INFO("Init net successfully! Port: %d\n", port);
  return 0;
}

int StateMachineService::initStateMachine(std::string &serverId,
                              const std::vector<std::string> &serverMembers,
                              const std::vector<std::string> &rpcMembers)
{
  st_= std::shared_ptr<StateMachine>(new StateMachine(serverId, serverMembers, rpcMembers));
  return st_->init(this);
}

int StateMachineService::wait()
{
  return net_->waitEio();
}

int StateMachineService::handleGetRequest(easy_request_t *r, TextRequest *cmd)
{
  return st_->get(r, cmd);
}

int StateMachineService::handleStatsRequest(easy_request_t *r, TextRequest *cmd)
{
  return st_->showStats(r, cmd);
}

int StateMachineService::handleVersionRequest(easy_request_t *r, TextRequest *cmd)
{
  return st_->showVersion(r, cmd);
}

int StateMachineService::handleStorageRequest(easy_request_t *r,
                                              TextRequest *cmd,
                                              LogOperation op)
{
  return st_->storage(r, cmd, op);
}

int StateMachineService::handleDeleteRequest(easy_request_t *r,
                                             TextRequest *cmd,
                                             LogOperation op)
{
  return st_->del(r, cmd, op);
}

int StateMachineService::process(easy_request_t *r, void *args)
{
  TextRequest *cmd= reinterpret_cast<TextRequest*>(r->ipacket);
  StateMachineService *service= (StateMachineService *)(r->ms->c->handler->user_data2);

  if (cmd == NULL || !cmd->getValid())
  {
    return EASY_OK;
  }

  if (cmd->getCommand() == TextCommand::SET)
  {
    return service->handleStorageRequest(r, cmd, kPut);
  }
  else if (cmd->getCommand() == TextCommand::DELETE)
  {
    return service->handleDeleteRequest(r, cmd, kDel);
  }
  else if (cmd->getCommand() == TextCommand::GET ||
           cmd->getCommand() == TextCommand::GETS)
  {
    return service->handleGetRequest(r, cmd);  
  }
  else if (cmd->getCommand() == TextCommand::CAS)
  {
    return service->handleStorageRequest(r, cmd, kCas);
  }
  else if (cmd->getCommand() == TextCommand::STATS)
  {
    return service->handleStatsRequest(r, cmd);
  }
  else if (cmd->getCommand() == TextCommand::VERSION)
  {
    return service->handleVersionRequest(r, cmd);
  }
  else if (cmd->getCommand() == TextCommand::TAIR_SET)
  {
    return service->handleStorageRequest(r, cmd, kTairSet);
  }
  else
  {
    LOG_INFO("Unknow operation!\n");
    return EASY_OK;
  }
}

int StateMachineService::sendResponse(easy_request_t *r, 
                                      TextResponse *response,  
                                      bool needWakeUp)
{
  NetPacket *packet;
  uint64_t len= response->getResponseLen();
  uint64_t extraLen= sizeof(NetPacket);
  
  if ((packet= (NetPacket *) easy_pool_alloc(r->ms->pool, extraLen + len)) == NULL)
  {
    r->opacket= NULL;
    return EASY_OK;
  }

  packet->len= len;
  packet->data= &packet->buffer[0];
  response->serializeToArray(packet->data);

  r->opacket= (void *)packet;

  if (needWakeUp)
  {
    easy_request_wakeup(r);
  }
  
  if (response != NULL)
  {
    delete response;
  }
  return EASY_OK;
}

};/* end of namespace alisql */
