/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  benchmark_compression.cc,v 1.0 01/28/2019 03:08:00 PM aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file benchmark_compression.cc
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 01/28/2019 03:08:00 PM
 * @version 1.0
 * @brief
 *
 **/

#include <sys/prctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <atomic>
#include "paxos.h"
#include "file_paxos_log.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "paxos.pb.h"
#include "msg_compress.h"

using namespace alisql;
std::atomic<bool> run(true);
int valueSize = 64;

void my_usleep(uint64_t t)
{
  struct timeval sleeptime;
  if (t == 0)
    return;
  sleeptime.tv_sec= t / 1000000;
  sleeptime.tv_usec= (t - (sleeptime.tv_sec * 1000000));
  select(0, 0, 0, 0, &sleeptime);
}

/*
 * Apply thread: once a log entry is committed, the apply thread will echo the value of the entry.
 * it also can be set to state machine or ack to the client in KV server.
 */
void applyThread(Paxos *paxos, Paxos *list[4], const char *file)
{
  uint64_t applyedIndex= 0;
  std::shared_ptr<PaxosLog> log= paxos->getLog();

  char tname[16];
  memcpy(tname, "apply thread\0", 13);
  prctl(PR_SET_NAME, tname);
  std::string compare(4096, 'a');
  std::ifstream is;
  if (file) {
    is.open(file, std::ios::in | std::ios::binary);
    assert(is.is_open());
  }
  char buffer[1024 * 1024];
  int ptr = 1024 * 1024;
  while (run)
  {
    uint64_t commitIndex= paxos->waitCommitIndexUpdate(applyedIndex);
    uint64_t i= 0;
    for (i= applyedIndex + 1; i <= paxos->getCommitIndex(); ++i)
    {
      LogEntry entry;
      log->getEntry(i, entry);
      if (entry.optype() > 10)
        continue;
      if (entry.value().size() == 0)
        continue;
      if (file) {
        LogEntry le;
        if (ptr == 1024 * 1024) {
          ptr = 0;
          is.read(buffer, 1024 * 1024);
          assert(is.good() && is.eof() == false);
        }
        std::string s(buffer + ptr, valueSize);
        ptr += valueSize;
        assert(s == entry.value());
      } else {
        assert(entry.value() == compare);
      }
    }
    applyedIndex= i - 1;
  }

  std::cout<< "====> ApplyThread: exit."<<std::endl<< std::flush;
}

void benchThread(Paxos *paxos, uint64_t threadId, uint64_t num, const char *file)
{
  char tname[16];
  memcpy(tname, "bench thread\0", 13);
  prctl(PR_SET_NAME, tname);
  std::cout<< "====> BenchThread "<< threadId<< " Start!"<< std::endl<< std::flush;
  std::cout<< valueSize << std::endl;
  if (file) {
    std::ifstream is(file, std::ios::in | std::ios::binary);
    assert(is.is_open());
    char buffer[8*1024*1024];
    int i = 0, ptr = 1024 * 1024;
    //while (is.eof() == false) {
    while (1) {
      if (++i > num)
        break;
      std::string s(valueSize, 0);
      LogEntry le;
      if (ptr == 1024 * 1024) {
        ptr = 0;
        is.read(buffer, 1024 * 1024);
        assert(is.good() && is.eof() == false);
      }
      le.set_optype(1);
      memcpy((char *)s.data(), buffer + ptr, valueSize);
      ptr += valueSize;
      le.set_value(s);
      le.set_index(0);
      paxos->replicateLog(le);
      //my_usleep(1);
    }
  } else {
    LogEntry le;
    le.set_optype(1);
    le.set_value(std::string(4096, 'a'));

    for (int i= 1; i<=num; ++i) {
      le.set_index(0);
      le.set_info(0);
      paxos->replicateLog(le);
    }
  }

  std::cout<< "====> BenchThread "<< threadId<< " Stop!"<< std::endl<< std::flush;
}

void printPaxosStats(Paxos *paxos)
{
  const Paxos::StatsType &stats= paxos->getStats();

  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog<< " countMsgRequestVote:"<<stats.countMsgRequestVote<< " countOnMsgAppendLog:"<< stats.countOnMsgAppendLog<< " countHeartbeat:"<< stats.countHeartbeat << " countOnMsgRequestVote:"<<stats.countOnMsgRequestVote<< " countOnHeartbeat:"<<stats.countOnHeartbeat<< " countReplicateLog:"<<stats.countReplicateLog
  <<std::endl;
}

int main(int argc, char *argv[])
{
  bool isSync= false;
  uint64_t num= 1000;
  uint64_t conc= 1;
  int compressionType = 0;
  bool checksum = false;
  char *file = 0;
  int ioThread = 1;
  int workThread = 1;

  if (argc < 2)
  {
    std::cerr<< "Usage: ./benchmark <client threads> <is sync> <num per thread> <value size> <compression type> <checksum> <io thread> <work Thread> <file name>" <<std::endl;
    std::cerr<< "Example: ./benchmark 2 1 1000 0 0" <<std::endl;
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
  if (argc >= 5) {
    valueSize = atoi(argv[4]);
  }
  if (argc >= 6) {
    compressionType = atoi(argv[5]);
  }
  if (argc >= 7) {
    checksum = atoi(argv[6]);
  }
  if (argc >= 8) {
    ioThread = atoi(argv[7]);
  }
  if (argc >= 9) {
    workThread = atoi(argv[8]);
  }
  if (argc >= 10) {
    file = argv[9];
  }
  std::cout << compressionType << std::endl;
  std::cout << checksum << std::endl;

  char tname[16];
  memcpy(tname, "main thread\0", 12);
  //prctl(PR_SET_NAME, tname);
  /* Control the log level, we use easy log here. */
  extern easy_log_level_t easy_log_level;
  easy_log_level= EASY_LOG_ERROR;

  /* Server list. */
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:12001");
  strConfig.emplace_back("127.0.0.1:12002");
  strConfig.emplace_back("127.0.0.1:12003");

  Paxos *paxosList[4];
  paxosList[0]= NULL;

  uint64_t electionTimeout= 5000;
  std::shared_ptr<PaxosLog> rlog1= std::make_shared<FilePaxosLog>(std::string("paxosLogTestDir")+strConfig[1-1], (FilePaxosLog::LogTypeT)0);
  paxosList[1]= new Paxos(electionTimeout, rlog1, 10000);
  paxosList[1]->init(strConfig, 1, NULL, ioThread, workThread);

  std::shared_ptr<PaxosLog> rlog2= std::make_shared<FilePaxosLog>(std::string("paxosLogTestDir")+strConfig[2-1], (FilePaxosLog::LogTypeT)0);
  paxosList[2]= new Paxos(electionTimeout, rlog2, 10000);
  paxosList[2]->init(strConfig, 2, NULL, ioThread, workThread);

  std::shared_ptr<PaxosLog> rlog3= std::make_shared<FilePaxosLog>(std::string("paxosLogTestDir")+strConfig[3-1], (FilePaxosLog::LogTypeT)0);
  paxosList[3]= new Paxos(electionTimeout, rlog3, 10000);
  paxosList[3]->init(strConfig, 3, NULL, ioThread, workThread);

  paxosList[1]->requestVote(true);

  Paxos *leader= NULL;
  uint64_t i= 0;
  while (leader == NULL)
  {
    sleep(3);
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

  for (int i = 1; i <= 3; i++)
    paxosList[i]->setMaxPacketSize(128 * 1024);

  leader->setMsgCompressOption((MsgCompressionType)compressionType, 4096, checksum);

  std::cout<< "====> Election Success! Leader is: "<< i <<std::endl;

  //std::thread th1(applyThread, leader, paxosList, file);

  struct timeval tv;
  uint64_t start,stop;
  gettimeofday(&tv, NULL);
  start= tv.tv_sec*1000000 + tv.tv_usec;

  uint64_t totalQueries= num;
  std::vector<std::thread *> ths;
  for (uint64_t i= 0; i < conc; ++i)
  {
    ths.push_back(new std::thread(benchThread, leader, i, totalQueries, file));
  }

  leader->waitCommitIndexUpdate(totalQueries*conc);
  gettimeofday(&tv, NULL);
  stop= tv.tv_sec*1000000 + tv.tv_usec;

  sleep(1);

  std::cout<< "Total cost:"<< stop-start<< "us." <<std::endl;
  std::cout<< "Rps:"<< (float)totalQueries*conc*1000/((float(stop-start))/1000)<< " ." <<std::endl;
  printPaxosStats(paxosList[1]);
  printPaxosStats(paxosList[2]);
  printPaxosStats(paxosList[3]);

  run = false;

  for (auto th : ths)
    th->join();

  for (i= 1; i<=3; ++i)
    delete paxosList[i];

  //th1.join();

  return 0;
}				  //function main
