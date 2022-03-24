/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  echo_learner.cc,v 1.0 10/25/2017 3:08:17 PM jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file echo_learner.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 10/25/2017 3:08:17 PM
 * @version 1.0
 * @brief
 *
 **/

#include <cstdlib>
#include <iostream>
#include <thread>
#include <unistd.h>
#include "paxos.h"
#include "rd_paxos_log.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "paxos.pb.h"

using namespace alisql;

bool bshutdown;

/*
 * Apply thread: once a log entry is committed, the apply thread will echo the value of the entry.
 * it also can be set to state machine or ack to the client in KV server.
 */
void applyThread(Paxos *paxos)
{
  uint64_t appliedIndex= 0;
  std::shared_ptr<PaxosLog> log= paxos->getLog();

  while (1)
  {
    /*uint64_t commitIndex= */paxos->waitCommitIndexUpdate(appliedIndex);
    if (bshutdown)
      break;
    uint64_t i= 0;
    for (i= appliedIndex + 1; i <= paxos->getCommitIndex(); ++i)
    {
      LogEntry entry;
      log->getEntry(i, entry);
      if (entry.optype() > 10)
        continue;
      //std::cout<< "LogIndex "<<i <<": key:"<< entry.ikey()<<", value:"<< entry.value()<< std::endl<< std::flush;
      std::cout<< "====> CommittedMsg:"<< entry.value() <<", LogIndex:"<< i<< std::endl<< std::flush;
    }
    appliedIndex= i - 1;
  }

  std::cout<< "====> ApplyThread: exit."<<std::endl<< std::flush;
}

int main(int argc, char *argv[])
{
  if (argc != 3)
  {
    std::cerr<< "Usage: ./echo_learner addr index" <<std::endl;
    std::cerr<< "Example: ./echo_learner 127.0.0.1:10004 1" <<std::endl;
    return 1;
  }
  std::string addr= argv[1];
  uint64_t index = atol(argv[2]);
  std::cout<< "Current Instance IP:PORT " << addr << " Index:"<< index << std::endl;

  setenv("easy_log_level", "3", 1);
  extern easy_log_level_t easy_log_level;
  easy_log_level= easy_log_level;
  //easy_log_level= EASY_LOG_ERROR;

  /* You can use the RDPaxosLog (based on RocksDB) by default, you can also implement a new log based on the interface PaxosLog by yourself. */
  auto rlog= std::make_shared<RDPaxosLog>(std::string("paxosLogTestDir")+addr, true, 4 * 1024 * 1024);
  rlog->setMetaData(Paxos::keyClusterId, 1);
  LogEntry le;
  le.set_term(0);
  le.set_index(0);
  le.set_optype(kMock);
  le.set_value("");
  rlog->debugSetLastLogIndex(index-1);
  rlog->append(le);
  /* Init paxos consensus here. */
  Paxos *paxos1= new Paxos(5000, rlog);
  paxos1->initAsLearner(addr);

  /* Start the apply thread. */
  std::thread th1(applyThread, paxos1);


  while (true)
  {
    sleep(1);
  }
  bshutdown= true;

  th1.join();
  sleep(2);
  delete paxos1;

  return 0;
}         //function main
