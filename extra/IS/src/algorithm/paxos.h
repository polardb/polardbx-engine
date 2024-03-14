/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos.h,v 1.0 07/29/2016 12:37:45 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/29/2016 12:37:45 PM
 * @version 1.0
 * @brief the PAXOS algorithm
 *
 **/

#ifndef cluster_paxos_INC
#define cluster_paxos_INC

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <vector>
#include "consensus.h"
#include "msg_compress.h"
#include "paxos_configuration.h"
#include "paxos_error.h"
#include "paxos_log.h"
#include "paxos_log_cache.h"
#include "paxos_option.h"
#include "service.h"
#include "single_process_queue.h"
#include "thread_timer.h"

namespace alisql {

class ClientService;

/**
 * @class ConfigureChangeManager
 *
 * @brief Manager of Configure Change
 *
 **/
class ConfigureChangeManager {
 public:
  ConfigureChangeManager()
      : prepared(0),
        applied(0),
        aborted(0),
        needNotify(0),
        preparedIndex(0),
        autoChangeRet(0),
        waitTimeout(0) {}
  void clear() {
    prepared = applied = aborted = needNotify = preparedIndex = 0;
  }

  // mutable std::mutex lock;
  mutable std::condition_variable cond;
  mutable std::condition_variable condChangeDone;  // learner autoChange done
  uint64_t prepared : 1;
  uint64_t applied : 1;
  uint64_t aborted : 1;
  uint64_t needNotify : 1;
  /* TODO: In follower, more than one preparedIndex will  */
  uint64_t preparedIndex;
  std::string autoChangeAddr;
  int autoChangeRet;
  std::atomic<uint64_t> waitTimeout; /* 0 means no timeout (sync) */
};

/**
 * @class CommitDepRecoveryManager
 *
 * @brief leader change may cause only receive partial commit dependency log.
 *        The recovery will reset these logs.
 *        This class keeps the expected lastLogindex for heartbeat
 *        and stop replicate log until the reset process succeeds
 **/
class CommitDepRecoveryManager {
 public:
  CommitDepRecoveryManager()
      : inRecovery(false), lastLogIndex(0), lastNonCommitDepIndex(0) {}
  void clear() {
    inRecovery = false;
    lastLogIndex = 0;
    lastNonCommitDepIndex = 0;
  }
  void setLastNonCommitDepIndex(uint64_t index) {
    /* In X-Cluster, set directly is also ok, no need to cas */
    for (;;) {
      uint64_t old = lastNonCommitDepIndex.load();
      if (old >= index ||
          (old < index &&
           lastNonCommitDepIndex.compare_exchange_weak(old, index)))
        break;
    }
  }

  std::atomic<bool> inRecovery;
  std::atomic<uint64_t> lastLogIndex;
  std::atomic<uint64_t> lastNonCommitDepIndex;
};

/**
 * @class Paxos
 *
 * @brief a paxos implement of Consensus
 *
 **/
class Paxos : public Consensus {
 public:
  Paxos(uint64_t electionTimeout = 5000, std::shared_ptr<PaxosLog> log = NULL,
        uint64_t purgeLogTimeout = 10000);
  ~Paxos() override;

  typedef enum LcType {
    LeaderTransfer,
    PurgeLog,
  } LcTypeT;

  /* Number of timer type can't exceed Service::MaxTimerCnt */
  enum TimerType {
    ElectionTimer = 0,
    HeartbeatTimer = 1,
  };

  typedef enum LogFillMode {
    NormalMode = 0,
    LargeBatchMode = 1,
    EmptyMode = 2,
  } LogFillModeT;

  typedef enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER,
    LEARNER,
    NOROLE,
  } StateType;
  const char stateString[5][5] = {"FOLL", "CAND", "LEDR", "LENR", "NORO"};

  typedef enum SubState {
    SubNone = 0,
    SubLeaderTransfer = 1,
  } SubStateType;

  typedef enum FlowControlMode {
    Slow = -1,
    Normal = 0,
  } FlowControlModeType;

  typedef enum AlertLogLevel {
    LOG_ERROR = EASY_LOG_ERROR,
    LOG_WARN = EASY_LOG_WARN,
    LOG_INFO = EASY_LOG_INFO,
    LOG_DEBUG = EASY_LOG_DEBUG,
    LOG_TRACE = EASY_LOG_TRACE,
  } AlertLogLevelType;

  typedef struct Stats {
    std::atomic<uint64_t> countMsgAppendLog;
    std::atomic<uint64_t> countMsgRequestVote;
    std::atomic<uint64_t> countHeartbeat;
    std::atomic<uint64_t> countLeaderCommand;
    std::atomic<uint64_t> countOnMsgAppendLog;
    std::atomic<uint64_t> countOnMsgRequestVote;
    std::atomic<uint64_t> countOnHeartbeat;
    std::atomic<uint64_t> countOnLeaderCommand;
    std::atomic<uint64_t> countReplicateLog;
    std::atomic<uint64_t> countLeaderTransfer;
    std::atomic<uint64_t> countTruncateBackward;
    Stats()
        : countMsgAppendLog(0),
          countMsgRequestVote(0),
          countHeartbeat(0),
          countLeaderCommand(0),
          countOnMsgAppendLog(0),
          countOnMsgRequestVote(0),
          countOnHeartbeat(0),
          countOnLeaderCommand(0),
          countReplicateLog(0),
          countLeaderTransfer(0),
          countTruncateBackward(0) {}
    void reset() {
      countMsgAppendLog = 0;
      countMsgRequestVote = 0;
      countHeartbeat = 0;
      countLeaderCommand = 0;
      countOnMsgAppendLog = 0;
      countOnMsgRequestVote = 0;
      countOnHeartbeat = 0;
      countOnLeaderCommand = 0;
      countReplicateLog = 0;
      countLeaderTransfer = 0;
      countTruncateBackward = 0;
    }
  } StatsType;

  typedef struct MembershipChange {
    std::string time;
    CCOpTypeT cctype;
    CCOpTypeT optype;
    std::string address;  // "ip:port;ip:port..."
    // for optype == CCConfigureNode
    bool forceSync;
    uint64_t electionWeight;
    bool sendByAppliedIndex;
    std::string learnerSource;
    MembershipChange() {
      auto tt = std::chrono::system_clock::to_time_t(
          std::chrono::system_clock::now());
      struct tm *timeinfo = std::localtime(&tt);
      char buffer[100];
      memset(buffer, 0, 100);
      strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);
      time = std::string(buffer);
    }
  } MembershipChangeType;

  typedef struct ClusterInfo {
    uint64_t serverId;
    std::string ipPort;
    uint64_t matchIndex;
    uint64_t nextIndex;
    StateType role;
    uint64_t hasVoted;
    bool forceSync;
    uint electionWeight;
    uint64_t learnerSource;
    uint64_t appliedIndex;
    bool pipelining;
    bool useApplied;
  } ClusterInfoType;

  typedef struct MemberInfo {
    uint64_t serverId;
    uint64_t currentTerm;
    uint64_t currentLeader;
    uint64_t commitIndex;
    uint64_t lastLogTerm;
    uint64_t lastLogIndex;
    StateType role;
    uint64_t votedFor;
    uint64_t lastAppliedIndex;
    std::string currentLeaderAddr;
  } MemberInfoType;

  typedef struct HealthInfo {
    uint64_t serverId;
    std::string addr;
    StateType role;
    bool connected;
    uint64_t
        logDelayNum;  // how many logs follower or learner are behind leader
    uint64_t applyDelayNum;  // how many applied logs follower or learner are
                             // behind leader
  } HealthInfoType;

  typedef class ChangeStateArg {
   public:
    ChangeStateArg(StateType r, uint64_t t, uint64_t i, Paxos *ra)
        : role(r), term(t), index(i), paxos(ra) {}
    ChangeStateArg() {}
    StateType role;
    uint64_t term;
    uint64_t index;
    Paxos *paxos;
  } ChangeStateArgType;

  typedef class purgeLogArg {
   public:
    purgeLogArg(uint64_t i, Paxos *ra) : index(i), paxos(ra) {}
    uint64_t index;
    Paxos *paxos;
  } purgeLogArgType;

  typedef class commitDepArg {
   public:
    commitDepArg(uint64_t i, uint64_t t, Paxos *p)
        : lastLogIndex(i), term(t), paxos(p) {}
    uint64_t lastLogIndex;
    uint64_t term;
    Paxos *paxos;
  } commitDepArgType;

  /* FOLLOWER */
  int onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) override;
  int onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) override;
  /* CANDIDATE */
  int requestVote(bool force = true) override;
  int onRequestVoteResponce(PaxosMsg *msg) override;
  /* LEADER */
  virtual int leaderTransfer(uint64_t targetId);
  virtual int leaderTransfer(
      const std::string &addr); /* support ip:port argument */
  virtual int changeLearners(CCOpTypeT type,
                             std::vector<std::string> &strConfig);
  virtual int changeMember(CCOpTypeT type, std::string &strConfig);
  int autoChangeLearnerAction();
  virtual int configureLearner(uint64_t serverId, uint64_t source,
                               bool applyMode = false);
  virtual int configureLearner(
      const std::string &addr, const std::string &sourceAddr,
      bool applyMode = false); /* support ip:port argument */
  virtual int configureMember(uint64_t serverId, bool forceSync,
                              uint electionWeight);
  virtual int configureMember(
      const std::string &addr, bool forceSync,
      uint electionWeight); /* support ip:port argument */
  virtual int downgradeMember(uint64_t serverId);
  virtual int downgradeMember(
      const std::string &addr); /* support ip:port argument */
  void forceFixMatchIndex(uint64_t targetId, uint64_t newIndex);
  void forceFixMatchIndex(const std::string &addr,
                          uint64_t newIndex); /* support ip:port argument */

  uint64_t replicateLog(LogEntry &entry) override {
    return replicateLog_(entry, true);
  }
  int appendLog(const bool needLock) override;
  virtual int appendLogToLearner(
      std::shared_ptr<RemoteServer> wserver = nullptr, bool needLock = false);
  int onAppendLogResponce(PaxosMsg *msg) override;
  virtual int leaderCommand(LcTypeT type,
                            std::shared_ptr<RemoteServer> server = nullptr);
  int onLeaderCommandResponce(PaxosMsg *msg) override;
  int onClusterIdNotMatch(PaxosMsg *msg) override;
  int onMsgPreCheck(PaxosMsg *msg, PaxosMsg *rsp) override;
  int onMsgPreCheckFailed(PaxosMsg *msg) override;
  virtual int forceSingleLeader();
  virtual int forceSingleLearner();
  virtual int forcePromote();
  int appendLogToServerByPtr(std::shared_ptr<RemoteServer> server,
                             bool needLock = true, bool force = false);
  int appendLogToServer(std::weak_ptr<RemoteServer> wserver,
                        bool needLock = true, bool force = false);

  /* ALL */
  int onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) override;
  virtual uint64_t waitCommitIndexUpdate(uint64_t baseIndex, uint64_t term = 0);
  virtual uint64_t checkCommitIndex(
      uint64_t baseIndex,
      uint64_t term = 0); /* A lock-free interface for follower */
  uint64_t getClusterId() override { return clusterId_.load(); }
  int setClusterId(uint64_t ci) override;
  uint64_t getMyServerId() { return myServerId_.load(); }
  int setMyServerId(uint64_t ci) { myServerId_.store(ci); return 0; }

  int checkLeaderTransfer(uint64_t targetId, uint64_t term, uint64_t &logIndex,
                          uint64_t leftCnt);
  int getClusterInfo(std::vector<ClusterInfoType> &cis);
  static void printClusterInfo(const std::vector<ClusterInfoType> &cis);
  int getClusterHealthInfo(std::vector<HealthInfoType> &healthInfo);
  void getMemberInfo(MemberInfoType *mi);
  uint64_t getServerIdFromAddr(const std::string &strAddr);
  int setMsgCompressOption(int type, size_t threshold, bool checksum,
                           const std::string &strAddr = "");
  int resetMsgCompressOption();
  void setOptimisticHeartbeat(bool optimistic) {
    optimisticHeartbeat_.store(optimistic);
  }
  bool getOptimisticHeartbeat() { return optimisticHeartbeat_.load(); }
  void shutdown();
  void stop();

  int onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId = nullptr) override;
  void setStateChangeCb(
      std::function<void(enum State, uint64_t, uint64_t)> cb) {
    stateChangeCb_ = cb;
  }
  void setStateChangeCb(
      std::function<void(Paxos *, enum State, uint64_t, uint64_t)> cb) {
    stateChangeCb_ = std::bind(cb, this, std::placeholders::_1,
                               std::placeholders::_2, std::placeholders::_3);
  }
  std::function<void(enum State, uint64_t, uint64_t)> getStateChangeCb() {
    return stateChangeCb_;
  }
  static void execStateChangeCb(ChangeStateArgType *arg) {
    arg->paxos->getStateChangeCb()(arg->role, arg->term, arg->index);
  }
  void setChecksumCb(
      std::function<uint32_t(uint32_t, const unsigned char *, size_t)> cb) {
    checksumCb_ = cb;
  }
  std::function<uint32_t(uint32_t, const unsigned char *, size_t)>
  getChecksumCb() {
    return checksumCb_;
  }
  void setChecksumMode(bool mode) { checksum_mode_ = mode; }
  static void appendLogCb(Paxos **paxos) { (*paxos)->appendLog(true); }
  // int onAsyncEvent(AsyncEventType type, void *arg, void *arg1);
  int tryUpdateCommitIndex();

  uint64_t appendLogFillForEachAsync(PaxosMsg *msg, RemoteServer *server,
                                     LogFillModeT mode = NormalMode);
  uint64_t appendLogFillForEach(PaxosMsg *msg, RemoteServer *server,
                                LogFillModeT mode = NormalMode);
  int init(const std::vector<std::string> &strConfig, uint64_t current,
           uint64_t myServerId, ClientService *cs = NULL,
           uint64_t ioThreadCnt = 4, uint64_t workThreadCnt = 4,
           std::shared_ptr<LocalServer> localServer = nullptr,
           bool memory_usage_count = false, uint64_t heartbeatThreadCnt = 0);
  int initAsLearner(std::string &strConfig, uint64_t myServerId,
                    ClientService *cs = NULL,
                    uint64_t ioThreadCnt = 4, uint64_t workThreadCnt = 4,
                    std::shared_ptr<LocalServer> localServer = nullptr,
                    bool memory_usage_count = false,
                    uint64_t heartbeatThreadCnt = 0);
  int initAutoPurgeLog(
      bool autoPurge = true, bool useAppliedIndex = true,
      std::function<bool(const LogEntry &le)> handler = nullptr);
  static void doPurgeLog(purgeLogArgType *arg);
  void updateAppliedIndex(uint64_t index);
  int forcePurgeLog(bool local, uint64_t forceIndex = UINT64_MAX);
  uint64_t getAppliedIndex() { return appliedIndex_.load(); }
  void electionWeightAction(uint64_t term, uint64_t baseEpoch);

  uint64_t getTerm() { return currentTerm_; }
  void setService(std::shared_ptr<Service> srvArg) { srv_ = srvArg; }
  std::shared_ptr<Service> getService() { return srv_; }
  std::shared_ptr<Configuration> &getConfig() { return config_; }
  std::shared_ptr<PaxosLog> getLog() { return log_; }
  std::shared_ptr<LocalServer> getLocalServer() { return localServer_; }
  void setLocalServer(std::shared_ptr<LocalServer> ls) {
    localServer_ = ls;
    electionTimer_->setRandWeight(ls->electionWeight);
  }
  enum State getState() { return state_.load(); }
  enum SubState getSubState() { return subState_.load(); }
  const uint64_t &getElectionTimeout() { return electionTimeout_; }
  const uint64_t &getHeartbeatTimeout() { return heartbeatTimeout_; }
  uint64_t getCommitIndex() {
    return (state_.load() == LEADER && consensusAsync_.load())
               ? localServer_->lastSyncedIndex.load()
               : commitIndex_.load();
  }
  const StatsType &getStats() { return stats_; }
  std::vector<MembershipChangeType> getMembershipChangeHistory() {
    std::lock_guard<std::mutex> lg(lock_);
    std::vector<MembershipChangeType> ret = membershipChangeHistory_;
    return ret;
  }
  const PaxosLog::StatsType &getLogStats() { return log_->getStats(); }
  void clearStats() { stats_.reset(); }
  uint64_t getLastLogIndex() { return log_->getLastLogIndex(); }
  uint64_t getLastCachedLogIndex() { return log_->getLastCachedLogIndex(); }
  uint64_t getCurrentLeader() { return leaderId_.load(); }
  // std::shared_ptr<ThreadTimer> getElectionTimer() {return electionTimer_;}
  uint64_t getCurrentEpoch() { return currentEpoch_.load(); }
  void setForceSyncEpochDiff(uint64_t arg) { forceSyncEpochDiff_ = arg; }
  uint64_t getForceSyncEpochDiff() { return forceSyncEpochDiff_; }
  uint64_t getMaxPacketSize() {
    return maxPacketSize_;
  }  // 1000000 BW * RTT/2/pipelines
  void setMaxPacketSize(uint64_t size) { maxPacketSize_ = size; }
  uint64_t getMaxDelayIndex() { return maxDelayIndex_; }
  uint64_t getMinDelayIndex() { return minDelayIndex_; }
  void setLargeBatchRatio(uint64_t v) { largeBatchRatio_ = v; }
  uint64_t getLargeBatchRatio() { return largeBatchRatio_; }
  bool getForceSyncStatus() { return leaderForceSyncStatus_.load(); }
  uint64_t getPipeliningTimeout() { return pipeliningTimeout_; }
  void setPipeliningTimeout(uint64_t t) { pipeliningTimeout_ = t; }
  void setMaxDelayIndex(uint64_t size) { maxDelayIndex_ = size; }
  void setMinDelayIndex(uint64_t size) { minDelayIndex_ = size; }
  bool isShutdown() override { return shutdown_.load(); }
  int getLogCacheByteSize() { return logRecvCache_.getByteSize(); }
  void updateFollowerMetaNo() { followerMetaNo_.fetch_add(1); }
  void setSyncFollowerMetaInterval(uint64_t val) { syncMetaInterval_ = val; }
  void setMaxDelayIndex4NewMember(uint64_t val) {
    maxDelayIndex4NewMember_ = val;
  }
  uint64_t getMaxMergeReportTimeout() { return maxMergeReportTimeout_; }
  void setMaxMergeReportTimeout(uint64_t val) { maxMergeReportTimeout_ = val; }
  void setCompactOldMode(bool val) { compactOldMode_ = val; }
  void setConsensusAsync(bool val) {
    consensusAsync_.store(val);
    cond_.notify_all();
  }
  bool getConsensusAsync() { return consensusAsync_.load(); }
  void setReplicateWithCacheLog(bool val) { replicateWithCacheLog_ = val; }
  bool getReplicateWithCacheLog() { return replicateWithCacheLog_.load(); }
  void setConfigureChangeTimeout(uint64_t t);
  void setAsLogType(bool val) { localServer_->logType = val; }
  void setLearnerConnTimeout(uint64_t t);
  void setSendPacketTimeout(uint64_t t);
  int log_checksum_test(const LogEntry &le);  // return 0 for success
  void setEnableDynamicEasyIndex(bool flag) { enableDynamicEasyIndex_ = flag; }
  bool getEnableDynamicEasyIndex() { return enableDynamicEasyIndex_; }
  void setEnableLearnerPipelining(bool flag) {
    enableLearnerPipelining_ = flag;
  }
  void setEnableAutoResetMatchIndex(bool flag) {
    enableAutoResetMatchIndex_ = flag;
  }
  void setEnableLearnerAutoResetMatchIndex(bool flag) {
    enableLearnerAutoResetMatchIndex_ = flag;
  }
  bool getEnableLearnerPipelining() { return enableLearnerPipelining_; }
  bool getEnableAutoResetMatchIndex() { return enableAutoResetMatchIndex_; }
  bool getEnableLearnerAutoResetMatchIndex() {
    return enableLearnerAutoResetMatchIndex_;
  }
  uint64_t getClusterSize() {
    return config_->getServerNum() + config_->getLearnerNum();
  }
  std::string getHost() { return host_; }
  uint getPort() { return port_; }

  /*
    PaxosOptions get/set functions
    TODO: move all options into class PaxosOption to have good encapsulation
  */
  void setEnableLearnerHeartbeat(bool arg) {
    option.enableLearnerHeartbeat_.store(arg);
  }
  bool getEnableLearnerHeartbeat() const {
    return option.enableLearnerHeartbeat_.load();
  }
  void setEnableAutoLeaderTransfer(bool arg) {
    option.enableAutoLeaderTransfer_.store(arg);
  }
  bool getEnableAutoLeaderTransfer() {
    return option.enableAutoLeaderTransfer_.load();
  }
  void setAutoLeaderTransferCheckSeconds(uint64_t arg) {
    option.autoLeaderTransferCheckSeconds_.store(arg);
  }
  void setMaxPipeliningEntrySize(uint64_t arg) {
    option.maxPipeliningEntrySize_.store(arg);
  }
  uint64_t getMaxPipeliningEntrySize() {
    return option.maxPipeliningEntrySize_.load();
  }
  std::shared_ptr<ExtraStore> getExtraStore() { return option.extraStore; }
  void setExtraStore(std::shared_ptr<ExtraStore> arg) {
    option.extraStore = std::move(arg);
  }

  void setThreadHook(const std::function<void()> &start,
                     const std::function<void()> &end) {
    delete threadHook;
    threadHook = new ThreadHook(start, end);
  }

  static void msleep(uint64_t t);
  void startElectionCallback();
  static void heartbeatCallback(std::weak_ptr<RemoteServer> wserver);
  void epochTimerCallback();
  void purgeLogCallback();
  static void commitDepResetLog(commitDepArgType *arg);
  void resetNextIndexForServer(std::shared_ptr<RemoteServer> server);
  bool tryFillFollowerMeta_(
      ::google::protobuf::RepeatedPtrField<::alisql::ClusterInfoEntry>
          *ciEntries);
  static void setAlertLogLevel(AlertLogLevelType level = LOG_WARN);

  /* append log flow control */
  void reset_flow_control();
  void set_flow_control(uint64_t serverId, int64_t fc);

  // FOR TEST ONLY
  void fakeRequestVote() { currentTerm_++; }
  void unfakeRequestVote() { currentTerm_--; }

  const static std::string keyCurrentTerm;
  const static std::string keyVoteFor;
  const static std::string keyLastLeaderTerm;
  const static std::string keyLastLeaderLogIndex;
  const static std::string keyMemberConfigure;
  const static std::string keyLearnerConfigure;
  const static std::string keyScanIndex;
  const static std::string keyClusterId;

  static bool debugDisableElection;
  static bool debugDisableStepDown;
  uint64_t debugMaxSendLogIndex;
  static bool debugWitnessTest; /* if true, send RequestVote & LeaderCommand to
                                   learner for unittest */
  static bool debugResetLogSlow;
  static bool debugSkipUpdateCommitIndex;

 protected:
  void becameLeader_();
  uint64_t replicateLog_(LogEntry &entry, const bool needLock = true);
  void changeState_(enum State newState);
  // void stateChangeCbInternal_();
  int applyConfigureChange_(uint64_t logIndex) {
    std::lock_guard<std::mutex> lg(lock_);
    return applyConfigureChangeNoLock_(logIndex);
  }
  int applyConfigureChangeNoLock_(uint64_t logIndex);
  int checkConfigure_(CCOpTypeT cctype, CCOpTypeT type,
                      std::vector<std::string> &strConfig,
                      const std::vector<Configuration::ServerRef> &servers);
  inline void prepareConfigureChangeEntry_(const LogEntry &entry, PaxosMsg *msg,
                                           bool fromCache = false);
  bool cdrIsValid(commitDepArgType *arg);
  void cdrClear(commitDepArgType *arg);
  uint64_t collectMinMatchIndex(std::vector<ClusterInfoType> &cis, bool local,
                                uint64_t forceIndex);

  /* common part of the corresponding public functions */
  int leaderTransfer_(uint64_t targetId);
  int leaderTransferSend_(uint64_t targetId, uint64_t term, uint64_t logIndex,
                          uint64_t leftCnt);
  int configureLearner_(uint64_t serverId, uint64_t source, bool applyMode,
                        std::unique_lock<std::mutex> &ul);
  int configureMember_(uint64_t serverId, bool forceSync, uint electionWeight,
                       std::unique_lock<std::mutex> &ul);
  int downgradeMember_(uint64_t serverId, std::unique_lock<std::mutex> &ul);
  int configureChange_(CCOpTypeT cctype, CCOpTypeT optype,
                       std::vector<std::string> &strConfig,
                       const std::vector<Configuration::ServerRef> &servers);
  int sendConfigureAndWait_(const ConfigureChangeValue &value,
                            std::unique_lock<std::mutex> &ul);
  void membershipChangeHistoryUpdate_(const MembershipChangeType &mc);
  uint64_t leaderTransferIfNecessary_(uint64_t epoch);
  uint64_t getLeaderTransferInterval_();
  uint64_t getNextEpochCheckStatemachine_(uint64_t epoch);
  void truncateBackward_(uint64_t firstIndex /* include */);

  bool onHeartbeatOptimistically_(PaxosMsg *msg, PaxosMsg *rsp);

  std::shared_ptr<Configuration> config_;
  std::shared_ptr<PaxosLog> log_;
  std::shared_ptr<Service> srv_;
  std::shared_ptr<LocalServer> localServer_;

  std::atomic<uint64_t> clusterId_;
  std::atomic<uint64_t> myServerId_;
  std::atomic<bool> shutdown_;
  uint64_t maxPacketSize_;
  const static uint64_t maxSystemPacketSize_;
  uint64_t maxDelayIndex_;
  uint64_t minDelayIndex_;
  uint64_t largeBatchRatio_;
  uint64_t pipeliningTimeout_;
  /* timeout unit is ms. */
  const uint64_t electionTimeout_;
  const uint64_t heartbeatTimeout_;
  const uint64_t purgeLogTimeout_;
  std::atomic<uint64_t> currentTerm_;
  std::atomic<bool> leaderStepDowning_;
  std::atomic<uint64_t> commitIndex_;
  std::atomic<uint64_t> leaderId_;
  std::string leaderAddr_;
  uint64_t votedFor_;
  bool forceRequestMode_;
  std::atomic<uint64_t> currentEpoch_;
  uint64_t forceSyncEpochDiff_;
  std::atomic<StateType> state_;
  std::atomic<SubStateType> subState_;
  std::atomic<bool> weightElecting_;
  std::atomic<bool> leaderForceSyncStatus_;
  std::atomic<bool> consensusAsync_;
  std::atomic<bool> replicateWithCacheLog_;
  std::atomic<bool> optimisticHeartbeat_;
  /* TODO need optimize lock granularity. */
  mutable std::mutex lock_;
  mutable std::condition_variable cond_;

  ConfigureChangeManager ccMgr_;
  PaxosLogCache logRecvCache_;
  CommitDepRecoveryManager cdrMgr_;

  std::shared_ptr<ThreadTimer> electionTimer_;
  std::shared_ptr<ThreadTimer> epochTimer_;
  std::shared_ptr<ThreadTimer> purgeLogTimer_;
  bool autoPurge_;        // switch for auto purge log (default true)
  bool useAppliedIndex_;  // will call updateAppliedIndex (default true), for
                          // purgelog
  uint64_t minMatchIndex_;
  std::atomic<uint64_t> appliedIndex_;
  /* For follower sync learner source. */
  std::atomic<uint64_t> followerMetaNo_;
  uint64_t lastSyncMetaNo_;
  uint64_t syncMetaInterval_;
  uint64_t maxDelayIndex4NewMember_;
  uint64_t maxMergeReportTimeout_;
  uint64_t nextEpochCheckStatemachine_;
  bool compactOldMode_;
  bool enableLogCache_;
  bool enableDynamicEasyIndex_;
  bool enableLearnerPipelining_;
  bool enableAutoResetMatchIndex_;
  bool enableLearnerAutoResetMatchIndex_;

  StatsType stats_;
  std::vector<MembershipChangeType> membershipChangeHistory_;

  SingleProcessQueue<purgeLogArgType> purgeLogQueue_;
  SingleProcessQueue<ChangeStateArgType> changeStateQueue_;
  SingleProcessQueue<Paxos *> appendLogQueue_;
  SingleProcessQueue<commitDepArgType> commitDepQueue_;

  /*
  std::queue<ChangeStateArgType *> changeStateMsgList_;
  std::mutex changeStateMsgListLock_;
  uint64_t changeStateWorkers_;
  */
  std::function<void(enum State, uint64_t, uint64_t)> stateChangeCb_;
  /*
   * CRC32 checksum for LogEntry
   * uint32_t checksum_crc32(uint32_t crc, const unsigned char *pos, size_t
   * length) set to nullptr if checksum is disabled
   */
  std::function<uint32_t(uint32_t, const unsigned char *, size_t)> checksumCb_;
  bool checksum_mode_;

  void newTerm(uint64_t newTerm);
  int tryUpdateCommitIndex_();

  PaxosOption option;
  std::string host_; /* paxos connect host */
  uint port_;        /* paxos listen port */

  ThreadHook *threadHook = nullptr;

 private:
  Paxos(const Paxos &other);                   // copy constructor
  const Paxos &operator=(const Paxos &other);  // assignment operator

  friend class Server;
  friend class LocalServer;
  friend class AliSQLServer;
  friend class RemoteServer;
  friend class StableConfiguration;

}; /* end of class Paxos */

}  // namespace alisql

#endif  // #ifndef cluster_paxos_INC
