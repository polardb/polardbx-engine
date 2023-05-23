/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_option.h,v 1.0 12/19/2019 10:04:06 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_option.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/19/2019 10:04:06 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef paxos_option_INC
#define paxos_option_INC

#include <atomic>
#include <memory>
#include <string>

namespace alisql {

class Paxos;
class RemoteServer;

/* store extra information from upper module */
class ExtraStore {
 public:
  ExtraStore() {}
  virtual ~ExtraStore() {}
  /* remote info received from leader */
  virtual std::string getRemote() = 0;
  virtual void setRemote(const std::string &) = 0;
  /* local info to send to others */
  virtual std::string getLocal() = 0;
};

class DefaultExtraStore : public ExtraStore {
  std::string getRemote() override { return ""; }
  void setRemote(const std::string &) override {}
  /* local info to send to others */
  std::string getLocal() override { return ""; }
};

class PaxosOption {
  friend class Paxos;
  friend class RemoteServer;

 public:
  PaxosOption()
      : enableLearnerHeartbeat_(false),
        enableAutoLeaderTransfer_(false),
        autoLeaderTransferCheckSeconds_(60),
        maxPipeliningEntrySize_(2ULL * 1024 * 1024 /* default 2M */) {
    extraStore = std::make_shared<DefaultExtraStore>();
  }
  ~PaxosOption() {}

 private:
  std::atomic<bool> enableLearnerHeartbeat_;
  std::atomic<bool> enableAutoLeaderTransfer_;
  std::atomic<uint64_t> autoLeaderTransferCheckSeconds_;
  std::atomic<uint64_t> maxPipeliningEntrySize_;
  std::shared_ptr<ExtraStore> extraStore;
};

}  // namespace alisql

#endif  // #ifndef paxos_option_INC
