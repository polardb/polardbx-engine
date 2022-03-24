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
 * @brief implement of StateMachine
 *
 **/

#include <stdlib.h>
#include "state_machine_service.h"
#include <google/protobuf/text_format.h>
#include <boost/lexical_cast.hpp>  
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <gflags/gflags.h>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <functional>
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "rocksdb/cache.h"

DECLARE_string(data_dir);
DECLARE_string(raftlog_dir);
DECLARE_bool(data_compress);
DECLARE_bool(raftlog_sync);
DECLARE_uint64(data_write_buffer_size);
DECLARE_int32(server_id);
DECLARE_bool(data_compress);
DECLARE_uint64(data_write_buffer_size);
DECLARE_bool(raftlog_sync);
DECLARE_uint64(max_write_buffer_number);
DECLARE_uint64(min_write_buffer_number_to_merge);
DECLARE_uint64(max_background_compactions);
DECLARE_uint64(max_bytes_for_level_base);
DECLARE_uint64(target_file_size_base);
DECLARE_uint64(level0_slowdown_writes_trigger);
DECLARE_uint64(level0_stop_writes_trigger);
DECLARE_bool(enable_statistics);
DECLARE_uint64(stats_dump_period_sec);
DECLARE_uint64(block_cache_size);
DECLARE_uint64(block_size);
DECLARE_uint64(bloom_filter_bits_per_key);
DECLARE_bool(block_based_bloom_filter);

namespace alisql {

StateMachine::StateMachine(std::string &serverId,
                           const std::vector<std::string> &serverMembers,
                           const std::vector<std::string> &rpcMembers)
                           : stop_(false), 
                           selfId_(serverId), 
                           lastAppliedIndex_(0),
                           leaderTerm_(0)
{
  std::vector<std::string>::const_iterator it= serverMembers.begin();
  bool selfInCluster= false;
  for (; it != serverMembers.end(); it++) 
  {
    serverMembers_.push_back(*it);
    if (selfId_ == *it) 
    {
      LOG_INFO("server member[Self]:%s\n", it->c_str());
      selfInCluster= true;
    }
    else 
    {
      LOG_INFO("server member:%s\n", it->c_str());
    }
  }

  it= rpcMembers.begin();
  for (; it != rpcMembers.end(); it++) 
  {
    rpcMembers_.push_back(*it);
    if (selfId_ == *it) 
    {
      LOG_INFO("rpc member[Self]:%s\n", it->c_str());
    }
    else 
    {
      LOG_INFO("rpc member:%s\n", it->c_str());
    }
  }

  state_.store(Raft::FOLLOWER);
}

StateMachine::~StateMachine()
{
}

int StateMachine::shutdown()
{
  stop_= true;
  if (applyLogThread_.joinable())
    applyLogThread_.join();
  return 0;
}

void StateMachine::initApplyThread()
{
  applyLogThread_= std::thread(&StateMachine::applyLogThread, this);
}

void StateMachine::applyLogThread()
{
  while (!stop_)
  {
    while (!stop_ && raft_->getCommitIndex() <= lastAppliedIndex_)
    {
      raft_->waitCommitIndexUpdate(lastAppliedIndex_);
    }

    if (stop_)
    {
      return;
    }
    uint64_t lastAppliedIdx= lastAppliedIndex_;
    uint64_t commitIndex= raft_->getCommitIndex();
    
    for (uint64_t i= lastAppliedIdx + 1; i <= commitIndex; i++)
    {
      LogEntry logEntry;
      int ret= raftLog_->getEntry(i, logEntry);
      if (ret != 0)
      {
        LOG_INFO("Fail to get entry idx: %ld from raft log db!\n", i);
        abort();
      }

      const char *result;
      std::string entryKey= logEntry.key();
      std::string entryVal= logEntry.value();
      switch (logEntry.optype())
      {
        case kPut:
        case kTairSet:
          set(entryKey, entryVal, i);
          result= TEXT_STORED;
          break;

        case kDel:
          ret= dataStore_->del("", entryKey, i);
          if (ret != 0)
          {
            LOG_INFO("Fail to delete entry at log idx: %ld from data storage!\n", i);
            abort();
          }
          result= TEXT_DELETED;
          break;

        case kCas:
          {
            std::string value;
            // cas unique number sent within cas command
            uint64_t inSeqNum= 0;
            // current cas unique number for this record
            uint64_t curSeqNum= 0;
            MemcachedObject::deserializeSeqNum(entryVal, inSeqNum);

            ret= dataStore_->get("", entryKey, &value);
            if (ret == 0) //find record
            {
              MemcachedObject::deserializeSeqNum(value, curSeqNum);

              if (inSeqNum != curSeqNum)
              {
                LOG_INFO("Server: %s, cas miss for key %s, cur cas: %ld, sent cas: %ld\n", selfId_.c_str(), entryKey.c_str(), curSeqNum, inSeqNum);
                result= TEXT_EXISTS;
                break;
              }
            }
            // return "NOT_FOUND\r\n" to indicate that the item user
            // is trying to store with a "cas" command did not exist.
            else
            {
              result= TEXT_NOT_FOUND;
              break;
            }
            set(entryKey, entryVal, i);
            result= TEXT_STORED;
            break;
          }
        case kNop:
          {
            // Before transfering to leader, raft layer will always send
            // out a empty log, the statemachine is saft to transfer as 
            // leader if this empty log is appied, this make sure all
            // logs before the empty one has been applied
            if (raft_->getState() == Raft::State::LEADER && 
                leaderTerm_ == raft_->getTerm())
            {
              LOG_INFO("Transfer to leader");
              state_.store(Raft::State::LEADER);
            }
            break;
          }
        default:
          LOG_INFO("Unkonwn op: %ld\n", logEntry.optype());
      }
      {
        std::lock_guard<std::mutex> lg(lock_);
        if (clientRsps_.find(i) != clientRsps_.end()) 
        {
          ClientRsp& ack= clientRsps_[i];
  
          if (ack.response != NULL)
          {
            ack.response->setResult(result);
            srv_->sendResponse(ack.easyReq, ack.response, true);
          }
          clientRsps_.erase(i);
        }
      }
      lastAppliedIndex_++;
    }
  }
}

void StateMachine::set(std::string &key, std::string &value, uint64_t index)
{
  MemcachedObject::serializeSeqNum(value, index);
  int ret= dataStore_->set("", key, value, index);
  if (ret != 0)
  {
    LOG_INFO("Fail to put entry at log idx: %ld to data storage!\n", index);
    abort();
  }
}

int StateMachine::init(StateMachineService *srv)
{
  int ret= 0;
  srv_= srv;

  std::string subDir= selfId_;
  boost::replace_all(subDir, ":", "_");

  ret= initDataStorage(subDir);
  if (ret == -1) 
  {
    LOG_INFO("Init data storage failed!\n");
    return ret;
  }

  ret= initRaftLog(subDir);
  if (ret == -1) 
  {
    LOG_INFO("Init raft log failed!\n");
    return ret;
  }

  initLastAppliedIndex();

  initRaft();
  if (ret != 0) 
  {
    LOG_INFO("Init raft failed!\n");
    return ret;
  }

  initApplyThread();
  
  return ret;
}

int StateMachine::initLastAppliedIndex()
{
  std::string tag;
  int ret= dataStore_->get("", DataStorage::lastAppliedIndexTag, &tag);
  if (ret == 0)
  {
    lastAppliedIndex_= RDRaftLog::stringToInt(tag);   
  }
  LOG_INFO("Init last applied index as: %ld\n", lastAppliedIndex_);
}

void StateMachine::stateChangeCb(enum Raft::State raftState)
{
  // Step down if we are no longer leader
  if (state_.load() == Raft::LEADER)
  {
    if (raftState != Raft::LEADER)
    {
      LOG_INFO("Change from state %d to %d", state_.load(), raftState);
      state_.store(raftState);
    }
  }
  else
  {
    if (raftState == Raft::LEADER) 
    {
      // we will transfer to leader after applying all logs, this is done
      // in applythread when we apply the empty log generated by raft layer
      leaderTerm_= raft_->getTerm();
      LOG_INFO("Set leader term as: %lld\n", raft_->getTerm());
    }
    else
    {
      LOG_INFO("Change from state %d to %d", state_.load(), raftState);
      state_.store(raftState);
    }
  }
}

int StateMachine::initRaft()
{
  raft_= std::shared_ptr<Raft>(new Raft(5000, raftLog_));
  raft_->setStateChangeCb(std::bind(&StateMachine::stateChangeCb, this, std::placeholders::_1));
  return raft_->init(rpcMembers_, FLAGS_server_id, NULL);
}

int StateMachine::initDataStorage(std::string &subDir) 
{
  std::string dataStorePath= FLAGS_data_dir + "/" + subDir + "/storage" ;
  rocksdb::Options options;
  getRocksDBOptions(options);
  dataStore_= std::shared_ptr<DataStorage>(new DataStorage(dataStorePath, options));
  return 0;
}

int StateMachine::initRaftLog(std::string &subDir)
{
  std::string raftLogPath= FLAGS_raftlog_dir + "/" + subDir;
  raftLog_= std::shared_ptr<RDRaftLog>(new RDRaftLog(raftLogPath, FLAGS_data_compress,
                                       FLAGS_data_write_buffer_size * 1024 * 1024));
  return 0;
}

int StateMachine::getRocksDBOptions(rocksdb::Options &options)
{
  options.create_if_missing= true;
  if (FLAGS_data_compress)
  {
    options.compression= rocksdb::kSnappyCompression;
    LOG_INFO("enable snappy compress for data storage\n");
  }

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  options.write_buffer_size= FLAGS_data_write_buffer_size * 1024 * 1024;

  // The maximum number of write buffers that are built up in memory.
  options.max_write_buffer_number= FLAGS_max_write_buffer_number;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.
  options.min_write_buffer_number_to_merge= FLAGS_min_write_buffer_number_to_merge;

  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  options.max_background_compactions= FLAGS_max_background_compactions;

  // Control maximum total data size for base level (level 1).
  options.max_bytes_for_level_base= FLAGS_max_bytes_for_level_base * 1024 * 1024;

  // Target file size for compaction.
  options.target_file_size_base= FLAGS_target_file_size_base * 1024 * 1024;

  // Soft limit on number of level-0 files. We start slowing down writes at this point.
  options.level0_slowdown_writes_trigger= FLAGS_level0_slowdown_writes_trigger;

  // Maximum number of level-0 files. We stop writes at this point.
  options.level0_stop_writes_trigger= FLAGS_level0_stop_writes_trigger;

  // Rocksdb Statistics provides cumulative stats over time.
  // Set it ture only when debugging performance, because it will introduce overhead.
  if (FLAGS_enable_statistics)
  {
    //options.statistics= rocksdb::CreateDBStatistics();
  }

  // Dump statistics periodically in information logs.
  // Same as rocksdb's default value (10 min).
  options.stats_dump_period_sec= FLAGS_stats_dump_period_sec;

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache= rocksdb::NewLRUCache(FLAGS_block_cache_size * 1024 * 1024 * 1024);
  table_options.block_size= FLAGS_block_size * 1024;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bloom_filter_bits_per_key,
                                    FLAGS_block_based_bloom_filter));
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  return 0;
}

uint64_t StateMachine::getLastAppliedIndex()
{
  return lastAppliedIndex_;
}

int StateMachine::get(easy_request_t *r, TextRequest *cmd)
{
  TextResponse *response = new TextResponse();
  if (response == NULL)
  {
    LOG_INFO("Fail to allocate TextResponse!\n");
    abort();
  }
  
  ++stats_.cmd_get;
  
  if (state_.load() != Raft::LEADER)
  {
    response->setResult(TEXT_ERROR);
    return srv_->sendResponse(r, response);
  }
  const char* key;
  std::size_t keyLen;
  std::tie(key, keyLen)= cmd->getKey();
  std::string keyStr(key, keyLen);
  std::string value;
  bool cas= false;
  uint64_t termBeforeGets;
  if (cmd->getCommand() == TextCommand::GETS && 
      raft_->getState() != Raft::State::LEADER)
  {
    response->setResult(TEXT_ERROR);
    return srv_->sendResponse(r, response);
  }
  
  termBeforeGets= raft_->getTerm();
  int ret= dataStore_->get("", keyStr, &value);
  if (ret == -1)
  {
    LOG_INFO("Get value for key: %s failed!\n", key);
    response->setResult(TEXT_END);
    return srv_->sendResponse(r, response);
  }

  if (cmd->getCommand() == TextCommand::GETS)
  {
    cas= true;
    // we must make sure we are leader to get correct cas unique
    if (raft_->getState() != Raft::State::LEADER ||
        raft_->getTerm() != termBeforeGets)
    {
      response->setResult(TEXT_ERROR);
      return srv_->sendResponse(r, response);
    }
  }
  response->setValueResult(keyStr, value, cas);
  return srv_->sendResponse(r, response);
}
           
int StateMachine::showStats(easy_request_t *r, TextRequest *cmd)
{
  TextResponse *response = new TextResponse();
  if (response == NULL)
  {
    LOG_INFO("Fail to allocate TextResponse!\n");
    abort();
  }

  if (cmd->getStats() == stats_t::CLUSTER)
  {
    if (state_.load() == Raft::LEADER)
    {
      uint64_t clusterSize= rpcMembers_.size();
      Raft::ClusterInfoType cis[clusterSize];
      raft_->getClusterInfo(cis, clusterSize);
      response->setClusterStatsResult(cis, clusterSize);
    }
  }
  else
  {
    Raft::MemberInfoType mi;
    raft_->getMemberInfo(&mi);
    response->setLocalStatsResult(mi, getLastAppliedIndex(), stats_.cmd_get);
  }

  return srv_->sendResponse(r, response);
}

int StateMachine::showVersion(easy_request_t *r, TextRequest *cmd)
{
  TextResponse *response= new TextResponse();
  response->setVersionResult();
  return srv_->sendResponse(r, response);
}

int StateMachine::storage(easy_request_t *r, TextRequest *cmd, LogOperation op)
{
  TextResponse *response= new TextResponse();
  if (response == NULL)
  {
    LOG_INFO("Fail to allocate TextResponse!\n");
    abort();
  }
  if (state_.load() != Raft::LEADER)
  {
    response->setResult(TEXT_ERROR);
    return srv_->sendResponse(r, response);
  }
  const char* key;
  std::size_t keyLen;
  const char* value;
  std::size_t valueLen;
  std::tie(key, keyLen)= cmd->getKey();
  std::tie(value, valueLen)= cmd->getData();
  std::string keyStr(key, keyLen);
  std::string valueStr(value, valueLen);
  uint32_t flags= cmd->getFlags();
  time_t exptime= cmd->getExptime();
  MemcachedObject object(static_cast<uint8_t>(op), flags, exptime, valueStr);
  std::string buffer;
  object.dumpObject(&buffer);
  
  if (op == kCas)
  {
    std::string curValue;
    int ret= dataStore_->get("", keyStr, &curValue);
    uint64_t curSeqNum= 0;
    if (ret == 0) //find record
    {
      MemcachedObject::deserializeSeqNum(curValue, curSeqNum);
      
      // If the cas unique sent from client is smaller than current cas unique,
      // return EXIST since replicate the cas command is unnecessary and expensive
      if (cmd->getCasUnique() < curSeqNum)
      {
        LOG_INFO("Server: %s, avoid to replicate cas for key %s, cur cas: %ld, sent cas: %ld\n", selfId_.c_str(), keyStr.c_str(), curSeqNum, cmd->getCasUnique());
        response->setResult(TEXT_EXISTS);
        return srv_->sendResponse(r, response);
      }
    }
    MemcachedObject::serializeSeqNum(buffer, cmd->getCasUnique());
  }
  LogEntry logEntry;
  logEntry.set_key(keyStr);
  logEntry.set_value(buffer);
  logEntry.set_optype(op);
  return replicateLog(r, logEntry, response);
}

int StateMachine::del(easy_request_t *r, TextRequest *cmd, LogOperation op)
{
  TextResponse *response= new TextResponse();
  if (response == NULL)
  {
    LOG_INFO("Fail to allocate TextResponse!\n");
    abort();
  }
  if (state_.load() != Raft::LEADER)
  {
    response->setResult(TEXT_ERROR);
    return srv_->sendResponse(r, response);
  }
  const char* key;
  std::size_t keyLen;
  std::tie(key, keyLen)= cmd->getKey();
  std::string keyStr(key, keyLen);
  
  LogEntry logEntry;
  logEntry.set_key(keyStr);
  logEntry.set_optype(op);
  return replicateLog(r, logEntry, response);
}

int StateMachine::replicateLog(easy_request_t *r, LogEntry &logEntry,
                               TextResponse *response)
{
  uint64_t curIndex= raft_->replicateLog(logEntry);
  if (curIndex == -1)
  {
    response->setResult(TEXT_ERROR);
    return srv_->sendResponse(r, response);
  }

  {
    std::lock_guard<std::mutex> lg(lock_);
    ClientRsp& ack= clientRsps_[curIndex];
    ack.response= response;
    ack.easyReq= r;
  }
  return EASY_ABORT;
}

};/* end of namespace alisql */
