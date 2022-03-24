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
#include <vector>
#include "paxos.h"
#include "rd_paxos_log.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "paxos.pb.h"

using namespace alisql;

/*
 * Apply thread: once a log entry is committed, the apply thread will echo the value of the entry.
 * it also can be set to state machine or ack to the client in KV server.
 */
void applyThread(Paxos *paxos)
{
  uint64_t applyedIndex= 0;
  std::shared_ptr<PaxosLog> log= paxos->getLog();

  while (1)
  {
    uint64_t commitIndex= paxos->waitCommitIndexUpdate(applyedIndex);
    uint64_t i= 0;
    for (i= applyedIndex + 1; i <= paxos->getCommitIndex(); ++i)
    {
      LogEntry entry;
      log->getEntry(i, entry);
      if (entry.optype() > 10)
        continue;
      std::cout<< "====> CommittedMsg:"<< entry.value() <<", LogIndex:"<< i<< std::endl<< std::flush;
    }
    applyedIndex= i - 1;
  }

  std::cout<< "====> ApplyThread: exit."<<std::endl<< std::flush;
}

void benchThread(Paxos *paxos, uint64_t threadId, uint64_t num)
{
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("aaaaaaa");
  //le.set_value("0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 0123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 123456789 ");

  std::cout<< "====> BenchThread "<< threadId<< " Start!"<< std::endl<< std::flush;

  for (int i= 1; i<=num; ++i)
    paxos->replicateLog(le);

  std::cout<< "====> BenchThread "<< threadId<< " Stop!"<< std::endl<< std::flush;
}

void printPaxosStats(Paxos *paxos)
{
  const Paxos::StatsType &stats= paxos->getStats();

  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog<< " countMsgRequestVote:"<<stats.countMsgRequestVote<< " countOnMsgAppendLog:"<< stats.countOnMsgAppendLog<< " countHeartbeat:"<< stats.countHeartbeat << " countOnMsgRequestVote:"<<stats.countOnMsgRequestVote<< " countOnHeartbeat:"<<stats.countOnHeartbeat<< " countReplicateLog:"<<stats.countReplicateLog<< std::endl<< std::flush;
}

int main(int argc, char *argv[])
{
  bool isSync= false;
  uint64_t num= 1000;
  uint64_t conc= 1;

  if (argc < 2)
  {
    std::cerr<< "Usage: ./benchmark <client threads> <is sync> <num per thread>" <<std::endl;
    std::cerr<< "Example: ./benchmark 2 1 1000" <<std::endl;
    return 1;
  }
  if (argc >= 2)
  {
    conc= atol(argv[1]);
  }
  if (argc >= 3)
  {
    if (atol(argv[2]) > 0)
      isSync= true;
  }
  if (argc >= 4)
  {
    num= atol(argv[3]);
  }
  //int index= atol(argv[4]);

  /* Control the log level, we use easy log here. */
  extern easy_log_level_t easy_log_level;
  //easy_log_level= EASY_LOG_ERROR;

  /* Server list. */
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:12001");
  strConfig.emplace_back("127.0.0.1:12002");
  strConfig.emplace_back("127.0.0.1:12003");

  Paxos *paxosList[4];
  paxosList[0]= NULL;

  uint64_t electionTimeout= 2000;
  /* You can use the RDPaxosLog (based on RocksDB) by default, you can also implement a new log based on the interface PaxosLog by yourself. */
  std::shared_ptr<PaxosLog> rlog1= std::make_shared<RDPaxosLog>(std::string("paxosLogTestDir")+strConfig[1-1], true, 4 * 1024 * 1024, isSync);
  paxosList[1]= new Paxos(electionTimeout, rlog1);
  paxosList[1]->init(strConfig, 1, NULL, 8, 8);

  std::shared_ptr<PaxosLog> rlog2= std::make_shared<RDPaxosLog>(std::string("paxosLogTestDir")+strConfig[2-1], true, 4 * 1024 * 1024, isSync);
  paxosList[2]= new Paxos(electionTimeout, rlog2);
  paxosList[2]->init(strConfig, 2, NULL, 8, 8);

  std::shared_ptr<PaxosLog> rlog3= std::make_shared<RDPaxosLog>(std::string("paxosLogTestDir")+strConfig[3-1], true, 4 * 1024 * 1024, isSync);
  paxosList[3]= new Paxos(electionTimeout, rlog3);
  paxosList[3]->init(strConfig, 3, NULL, 8, 8);

  Paxos *leader= NULL;
  uint64_t i= 0;
  while (leader == NULL)
  {
    sleep(4);
    for (i= 1; i<=3; ++i)
    {
      if (paxosList[i]->getState() == Paxos::LEADER)
      {
        leader= paxosList[i];
        break;
      }
    }

    if (leader == NULL)
      std::cout<< "====> Election Fail! " <<std::endl;
  }

  std::cout<< "====> Election Success! Leader is: "<< i <<std::endl;


  std::thread th1(applyThread, leader);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("AAAAAAAAAAAAAA");

  leader->replicateLog(le);

  struct timeval tv;
  uint64_t start,stop;
  gettimeofday(&tv, NULL);
  start= tv.tv_sec*1000000 + tv.tv_usec;

  uint64_t totalQueries= num;
  std::vector<std::thread *> ths;
  for (uint64_t i= 0; i < conc; ++i)
  {
    ths.push_back(new std::thread(benchThread, leader, i, totalQueries));
  }
  //std::thread bt1(benchThread, leader, 1, totalQueries);
  //std::thread bt2(benchThread, leader, 2, totalQueries);

  leader->waitCommitIndexUpdate(totalQueries*conc+1);
  gettimeofday(&tv, NULL);
  stop= tv.tv_sec*1000000 + tv.tv_usec;

  sleep(1);

  std::cout<< "Total cost:"<< stop-start<< "us." <<std::endl;
  std::cout<< "Rps:"<< totalQueries*conc*1000/((stop-start)/1000)<< " ." <<std::endl;
  printPaxosStats(leader);

  for (auto th : ths)
    th->join();
  //bt1.join();

  for (i= 1; i<=3; ++i)
    delete paxosList[i];

  return 0;
}				  //function main 
