/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  state_machine_server.cc,v 1.0 07/31/2016 04:53:21 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/


#include "state_machine_server.h"

namespace alisql {

StateMachineServer::StateMachineServer()
{}

int StateMachineServer::init(std::string& serverId,
                             const std::vector<std::string>& serverMembers,
                             const std::vector<std::string>& rpcMembers)
{
  service_= std::shared_ptr<StateMachineService>(
            new StateMachineService());
  service_->init(serverId, serverMembers, rpcMembers);
  return 0;
}

int StateMachineServer::wait() {
  return service_->wait();
}

int StateMachineServer::shutdown()
{
  service_->shutdown();
  return 0;
}

} //namespace alisql
