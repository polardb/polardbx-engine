/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  state_machine.h,v 1.0 07/15/2016 02:46:50 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file state_machine.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 07/15/2016 02:46:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef  cluster_statemachine_INC
#define  cluster_statemachine_INC

#include <string>
#include <vector>
#include "state_machine_service.h"
#include "raft.h"
#include "../memcached/memcached_easyNet.h"
#include "../memcached/text_request.h"
#include "../memcached/text_response.h"
#include "../storage/data_storage.h"
#include "rd_raft_log.h"
#include <boost/unordered_map.hpp>
#include <thread>
#include <mutex>
#include <atomic>
#include "rocksdb/options.h"

namespace alisql {

struct TextResponse;
class StateMachineService;

struct ClientRsp
{
  TextResponse *response;
  easy_request_t *easyReq;
  ClientRsp() : response(NULL), easyReq(NULL)
  {
  }
};

/**
 * @class StateMachine
 *
 * @brief class for StateMachine
 *
 **/
class StateMachine {
  public:
    StateMachine(std::string &serverId,
                 const std::vector<std::string> &serverMembers,
                 const std::vector<std::string> &rpcMembers);
    virtual ~StateMachine();

    typedef struct MemStats {
      std::atomic<uint64_t> cmd_get;
      MemStats()
        :cmd_get(0)
      {}
    } MemStatsType;

    virtual int init(StateMachineService *srv);

    virtual int initRaft();

    virtual int initDataStorage(std::string &subDir);

    virtual int initRaftLog(std::string &subDir);

    virtual int initLastAppliedIndex();

    virtual int shutdown();

    void initApplyThread();

    void applyLogThread();

    void set(std::string &key, std::string &value, uint64_t index);

    int storage(easy_request_t *r, TextRequest *cmd, LogOperation op);

    int get(easy_request_t *r, TextRequest *cmd);

    int cas(easy_request_t *r, TextRequest *cmd);

    int del(easy_request_t *r, TextRequest *cmd, LogOperation op);

    int showStats(easy_request_t *r, TextRequest *cmd);

    int showVersion(easy_request_t *r, TextRequest *cmd);

    int replicateLog(easy_request_t *r, LogEntry &logEntry,
                     TextResponse *response);

    uint64_t getLastAppliedIndex();

    enum Raft::State getState() {return state_;}

    void stateChangeCb(enum Raft::State raftState);

    int getRocksDBOptions(rocksdb::Options &options);

    const MemStatsType &getMemStats() {return stats_;}

  private:
    std::mutex lock_;
    bool stop_;
    std::atomic<enum Raft::State> state_;
    // Before statemachine transfer to leader, we must make sure all
    // logs have been applied. leaderTerm_ is set when raft layer
    // transfer to leader in stateChangeCb, after all logs have been,
    // applied we verify that the term has not changed in the meantime,
    // then transfer to leader state.
    std::atomic<uint64_t> leaderTerm_;
    std::string selfId_;
    std::vector<std::string> serverMembers_;
    std::vector<std::string> rpcMembers_;
    std::shared_ptr<DataStorage> dataStore_;
    std::shared_ptr<RDRaftLog> raftLog_;
    std::shared_ptr<Raft> raft_;
    StateMachineService *srv_;
    boost::unordered_map<uint64_t, ClientRsp> clientRsps_;
    std::thread applyLogThread_;
    uint64_t lastAppliedIndex_;
    MemStatsType stats_;
};/* end of class StateMachine */

};/* end of namespace alisql */

#endif     //#ifndef cluster_statemachine_INC
