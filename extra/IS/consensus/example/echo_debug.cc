/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  main.cc,v 1.0 08/08/2016 05:24:03 PM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file main.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/08/2016 05:24:03 PM
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
  int num= atol(argv[1]);
  if (argc <= (2+num))
  {
    std::cerr<< "Usage: ./echo_debug <num> <ip1:port1> <ip2:port2> <ip3:port3> <currentIndex> [<witness_ip:witness_port>]" <<std::endl;
    std::cerr<< "Example: ./echo_debug 3 127.0.0.1:10001 127.0.0.1:10002 127.0.0.1:10003 1 127.0.0.1:10004" <<std::endl;
    return 1;
  }
  int index= atol(argv[2+num]);
  std::cout<< "Current Instance IP:PORT " << argv[index+1] << " Index:"<< index<< std::endl;

  setenv("easy_log_level", "3", 1);
  extern easy_log_level_t easy_log_level;
  easy_log_level= easy_log_level;
  //easy_log_level= EASY_LOG_ERROR;

  /* Server list. */
  std::vector<std::string> strConfig;

  for (int i= 2; i < 2+num; ++i)
    strConfig.emplace_back(argv[i]);

  /* You can use the RDPaxosLog (based on RocksDB) by default, you can also implement a new log based on the interface PaxosLog by yourself. */
  std::shared_ptr<PaxosLog> rlog= std::make_shared<RDPaxosLog>(std::string("paxosLogTestDir")+strConfig[index-1], true, 4 * 1024 * 1024);
  rlog->setMetaData(Paxos::keyClusterId, 1);
  /* Init paxos consensus here. */
  Paxos *paxos1= new Paxos(5000, rlog);
  /* For Debug. */
  if (index == 1)
    paxos1->debugDisableStepDown= true;
  else
    paxos1->debugDisableElection= true;
  paxos1->init(strConfig, index, new ClientService());

  /* init witness node */
  std::vector<std::string> witnesslist;
  for (int i = (3+num); i < argc; ++i)
  {
    witnesslist.emplace_back(argv[i]);
  }
  /* add learner after we have a leader */
  while (1) {
      sleep(1);
      if (paxos1->getState() == paxos1->LEADER) {
          paxos1->changeLearners(Paxos::CCAddNode, witnesslist);
      }
      if (paxos1->getCurrentLeader() != 0) {
          /* have leader */
          break;
      }
  }

  /* Start the apply thread. */
  std::thread th1(applyThread, paxos1);

  paxos1->requestVote();

  while (true)
  {
    std::string key= "keykey", value, line;
    std::cin>> value;
    if (paxos1->getState() != Paxos::LEADER)
    {
      std::cerr<< "====> Error: I'm not leader!! Leader is server:"<<paxos1->getCurrentLeader() << std::endl;
      continue;
    }

    //std::cin>> "set">> key>> "=">> value>> std::endl;
    std::cout<< "====> Input value:"<< value<< " ."<< std::endl;

    if (value == "close")
      break;

    LogEntry le;
    le.set_index(0);
    le.set_optype(1);
    //le.set_ikey(key);
    le.set_value(value);

    /* Send the log entry by Paxos consensus. */
    paxos1->replicateLog(le);
  }
  bshutdown= true;

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  //le.set_ikey(key);
  le.set_value("close");
  paxos1->replicateLog(le);


  th1.join();
  //paxos1->stop();
  //sleep(2);
  //paxos1->shutdown();

  sleep(2);
  delete paxos1;

  return 0;
}				  //function main 
