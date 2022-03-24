/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  state_machine_service.h,v 1.0 07/15/2016 02:46:50 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file state_machine_service.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 07/15/2016 02:46:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef  cluster_statemachine_service_INC
#define  cluster_statemachine_service_INC

#include <string>
#include <vector>
#include "service.h"
#include "raft.h"
#include "state_machine.h"
#include "../memcached/memcached_easyNet.h"
#include "../memcached/text_request.h"
#include "../memcached/text_response.h"
#include "../storage/data_storage.h"
#include "rd_raft_log.h"
#include <boost/unordered_map.hpp>

namespace alisql {

/**
 * @class StateMachineService
 *
 * @brief class for StateMachineService
 *
 **/
class StateMachineService : public Service {
  public:
    StateMachineService();
    virtual ~StateMachineService();

    virtual int init(std::string &serverId,
                     const std::vector<std::string> &serverMembers,
                     const std::vector<std::string> &rpcMembers);

    virtual int initNet(std::string &serverId);

    virtual int initStateMachine(std::string &serverId,
                                 const std::vector<std::string> &serverMembers,
                                 const std::vector<std::string> &rpcMembers);

    virtual int wait();

    virtual int shutdown();
    static int process(easy_request_t *r, void *args);

    int sendResponse(easy_request_t *r,
                     TextResponse *response,
                     bool needWakeUp=false);

    int handleStorageRequest(easy_request_t *r, TextRequest *cmd, LogOperation op);

    int handleGetRequest(easy_request_t *r, TextRequest *cmd);

    int handleDeleteRequest(easy_request_t *r, TextRequest *cmd, LogOperation op);

    int handleStatsRequest(easy_request_t *r, TextRequest *cmd);

    int handleVersionRequest(easy_request_t *r, TextRequest *cmd);

  protected:
    std::shared_ptr<MemcachedEasyNet> net_;
    std::shared_ptr<StateMachine> st_;
};/* end of class StateMachineService */

};/* end of namespace alisql */

#endif     //#ifndef cluster_statemachine_service_INC
