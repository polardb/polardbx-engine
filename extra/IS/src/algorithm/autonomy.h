#ifndef AUTONOMY_H
#define AUTONOMY_H
#include "protocol.h"

namespace alisql {

class Autonomy : public Protocol {
 public:
  Autonomy() = default;
  virtual ~Autonomy() = default;

  int onRequestVote(PaxosMsg *, PaxosMsg *) override { assert(0); return 0; }
  int onAppendLog(PaxosMsg *, PaxosMsg *) override { assert(0); return 0; }
  int onRequestVoteResponce(PaxosMsg *) override { assert(0); return 0; }
  int onAppendLogSendFail(PaxosMsg *, uint64_t *) override { assert(0); return 0; }
  int onAppendLogResponce(PaxosMsg *) override { assert(0); return 0; }
  int requestVote(bool ) override { assert(0); return 0; }
  uint64_t replicateLog(LogEntry &) override { assert(0); return 0; }
  uint64_t replicateEmptyLog() override { assert(0); return 0; }
  int appendLog(const bool ) override { assert(0); return 0; }
  int onLeaderCommand(PaxosMsg *, PaxosMsg *) override { assert(0); return 0; }
  int onLeaderCommandResponce(PaxosMsg *) override { assert(0); return 0; }
  int onClusterIdNotMatch(PaxosMsg *) override { assert(0); return 0; }
  int onMsgPreCheck(PaxosMsg *, PaxosMsg *) override { assert(0); return 0; }
  int onMsgPreCheckFailed(PaxosMsg *) override { assert(0); return 0; }
  uint64_t getClusterId() override { assert(0); return 0; }
  int setClusterId(uint64_t ) override { assert(0); return 0; }
  bool isShutdown() override { assert(0); return false; }
};

}

#endif