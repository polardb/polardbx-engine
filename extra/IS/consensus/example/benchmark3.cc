/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  benchmark3.cc,v 1.0 08/08/2016 05:24:03 PM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file benchmark3.cc
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
#include "file_paxos_log.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "paxos.pb.h"

using namespace alisql;

/*
 * Apply thread: once a log entry is committed, the apply thread will echo the value of the entry.
 * it also can be set to state machine or ack to the client in KV server.
 */
bool gStop= false;
void applyThread(Paxos *paxos)
{
  uint64_t applyedIndex= 0;
  std::shared_ptr<PaxosLog> log= paxos->getLog();

  while (! gStop)
  {
    uint64_t commitIndex= paxos->waitCommitIndexUpdate(applyedIndex);
    uint64_t i= 0;
    for (i= applyedIndex + 1; i <= paxos->getCommitIndex(); ++i)
    {
      const LogEntry *le;
      if (NULL == (le= log->getEntry(i)))
      {
        std::cerr<< "getEntry error!!! logIndex:"<< i<< std::endl;
        break;
      }
      const LogEntry& entry= *le;
      if (entry.optype() > 10)
        continue;

      if (entry.value().length() < 100)
        std::cout<< "====> CommittedMsg:"<< entry.value() <<", LogIndex:"<< i<< std::endl;
      else
        std::cout<< "====> CommittedMsg:"<< "Too long not display" <<", LogIndex:"<< i<< std::endl;
    }
    applyedIndex= i - 1;
  }

  std::cout<< "====> ApplyThread: exit."<<std::endl<< std::flush;
}

void my_usleep(uint64_t t)
{
  struct timeval sleeptime;
  if (t == 0)
    return;
  sleeptime.tv_sec= t / 1000000;
  sleeptime.tv_usec= (t - (sleeptime.tv_sec * 1000000));
  select(0, 0, 0, 0, &sleeptime);
}
uint64_t sleepTime= 1;
void benchThread(Paxos *paxos, uint64_t threadId, uint64_t num, uint64_t valueSize)
{
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);

  char *mem= (char *)malloc(valueSize+1);
  memset(mem, 'a', valueSize);
  mem[valueSize]= '\0';
  le.set_value(mem);
  std::shared_ptr<alisql::PaxosLog> log= paxos->getLog();

  std::cout<< "====> BenchThread "<< threadId<< " Start!"<< std::endl<< std::flush;

  for (int i= 1; i<num; ++i)
  {
    //if (i % 100 == 0 || i < 10)
      paxos->replicateLog(le);
    //else
      //log->append(le);
    if (sleepTime != 0)
      my_usleep(sleepTime);
  }
  paxos->replicateLog(le);

  log.reset();

  std::cout<< "====> BenchThread "<< threadId<< " Stop!"<< std::endl<< std::flush;

  free(mem);
}

void printPaxosStats(Paxos *paxos)
{
  const Paxos::StatsType &stats= paxos->getStats();

  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog<< " countMsgRequestVote:"<<stats.countMsgRequestVote<< " countOnMsgAppendLog:"<< stats.countOnMsgAppendLog<< " countHeartbeat:"<< stats.countHeartbeat << " countOnMsgRequestVote:"<<stats.countOnMsgRequestVote<< " countOnHeartbeat:"<<stats.countOnHeartbeat<< " countReplicateLog:"<<stats.countReplicateLog<< std::endl<< std::flush;
}

int main(int argc, char *argv[])
{
  uint32_t logType= 0;
  uint64_t num= 1000;
  uint64_t conc= 1;
  uint64_t valueSize= 20;
  uint64_t maxPacketSize= 0;

  if (argc < 5)
  {
    std::cerr<< "Usage: ./benchmark3 <ip1:port1> <ip2:port2> <ip3:port3> <currentIndex> <client threads> <logtype(012)> <num per thread> [value size] [net packet size] [sleeptime]" <<std::endl;
    std::cerr<< "Example: ./benchmark3 127.0.0.1:10001 127.0.0.1:10002 127.0.0.1:10003 1 2 1 1000" <<std::endl;
    return 1;
  }
  int index= atol(argv[4]);

  if (argc >= 6)
  {
    conc= atol(argv[5]);
  }
  if (argc >= 7)
  {
    if (atol(argv[6]) > 0)
      logType= atol(argv[6]);
  }
  if (argc >= 8)
  {
    num= atol(argv[7]);
  }
  if (argc >= 9)
  {
    valueSize= atol(argv[8]);
  }
  if (argc >= 10)
  {
    maxPacketSize= atol(argv[9]);
  }
  if (argc >= 11)
  {
    sleepTime= atol(argv[10]);
  }

  /* Control the log level, we use easy log here. */
  extern easy_log_level_t easy_log_level;
  easy_log_level= EASY_LOG_ERROR;

  /* Server list. */
  std::vector<std::string> strConfig;
  strConfig.emplace_back(argv[1]);
  strConfig.emplace_back(argv[2]);
  strConfig.emplace_back(argv[3]);

  /* You can use the RDPaxosLog (based on RocksDB) by default, you can also implement a new log based on the interface PaxosLog by yourself. */
  std::shared_ptr<PaxosLog> rlog= std::make_shared<FilePaxosLog>(std::string("paxosLogTestDir")+strConfig[index-1], (FilePaxosLog::LogTypeT)logType);

  uint64_t electionTimeout= 100000;
  /* You can use the RDPaxosLog (based on RocksDB) by default, you can also implement a new log based on the interface PaxosLog by yourself. */
  auto paxos1= new Paxos(electionTimeout, rlog);
  if (index == 1)
    paxos1->debugDisableStepDown= true;
  else
    paxos1->debugDisableElection= true;

  if (maxPacketSize != 0)
    paxos1->setMaxPacketSize(maxPacketSize);

  paxos1->init(strConfig, index, NULL, 8, 8);
  sleep(5);
  paxos1->requestVote();

  sleep(4);
  while (paxos1->getState() != Paxos::LEADER)
  {
    sleep(1);
  }

  std::cout<< "====> Election Success! I'm the leader !!"<< std::endl;

  auto leader= paxos1;

  uint64_t totalQueries= num;
  std::thread th1(applyThread, leader);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("AAAAAAAAAAAAAA");


  std::cout<< "====> Start warm up"<< std::endl;
  {
    uint64_t valueSize= 5000000;
    char *mem= (char *)malloc(valueSize+1);
    memset(mem, 'a', valueSize);
    mem[valueSize]= '\0';
    le.set_value(mem);

    for (uint i= 0; i<=100; ++i)
    {
      leader->replicateLog(le);
      my_usleep(50000);
    }
    sleep(1);
  }
  std::cout<< "====> Stop warm up"<< std::endl;

  struct timeval tv;
  uint64_t start,stop;
  gettimeofday(&tv, NULL);
  start= tv.tv_sec*1000000 + tv.tv_usec;

  std::vector<std::thread *> ths;
  for (uint64_t i= 0; i < conc; ++i)
  {
    ths.push_back(new std::thread(benchThread, leader, i, totalQueries, valueSize));
  }

  leader->waitCommitIndexUpdate(totalQueries*conc+1);
  gettimeofday(&tv, NULL);
  stop= tv.tv_sec*1000000 + tv.tv_usec;

  sleep(1);

  std::cout<< "Total cost:"<< stop-start<< "us." <<std::endl;
  std::cout<< "Rps:"<< totalQueries*conc*1000/((stop-start)/1000)<< " ." <<std::endl;
  printPaxosStats(leader);


  for (auto th : ths)
    th->join();

  gStop= true;

  delete paxos1;

  th1.join();

  return 0;
}				  //function main 
