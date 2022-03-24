/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  Service.h,v 1.0 07/15/2016 02:46:50 PM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file Service.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/15/2016 02:46:50 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_statemachine_server_INC
#define  cluster_statemachine_server_INC

#include <string>
#include <vector>
#include "ev.h"
#include "service.h"
#include "state_machine_service.h"

namespace alisql {

/**
 * @class StateMachineServer
 *
 * @brief class for StateMachineServer
 *
 **/
class StateMachineServer {
  public:
    StateMachineServer();
    virtual ~StateMachineServer() {};

    virtual int init(std::string& serverId,
                     const std::vector<std::string>& serverMembers,
                     const std::vector<std::string>& rpcMembers);
    virtual int start() {return 0;};
    virtual int shutdown();

    virtual int wait();

  protected:
    std::shared_ptr<StateMachineService> service_;

};/* end of class StateMachineServer */

};/* end of namespace alisql */

#endif     //#ifndef cluster_statemachine_server_INC 
