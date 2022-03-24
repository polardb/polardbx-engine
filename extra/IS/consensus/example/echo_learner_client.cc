/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  echo_learner_client.cc,v 1.0 12/20/2016 19:15:45 PM jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file echo_learner_client.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/20/2016 19:15:45 PM
 * @version 1.0
 * @brief example code of learner client
 *
 **/

#include <cstdlib>
#include <iostream>
#include <thread>
#include <string>
#include <unistd.h>
#include "learner_client.h"

using namespace alisql;

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: ./echo_learner_client <ip:port> <startLogIndex> <clusterId> <readDelay>" << std::endl;
    std::cerr << "Example: ./echo_learner_client 127.0.0.1:10004 5 1 1000000 (1s) "
        << std::endl;
    return 1;
  }
  
  uint64_t startLogIndex= atol(argv[2]);

  std::cout << "Current Instance IP:PORT " << argv[1] << std::endl;

  LearnerClient *client = new LearnerClient();

//  RemoteLeader rl("127.0.0.1", 10001, "", "");
//  client->registerLearner(rl, argv[1]);

  int ret;
  ret= client->open(argv[1], atol(argv[3]), startLogIndex, 1000, 2); /* read from index 5, read timeout 1s, cache size 2*/
  //ret= client->openWithTimestamp(argv[1], rl, 1488336538, 10000, 2); /* read from index 5, read timeout 10s, cache size 2*/

  if (ret != 0)
  {
    std::cerr<< "Open client fail!!"<< std::endl;
    return 1;
  }
  
  ServerMeta source("127.0.0.1", 16001, "root", "");
  ServerMeta meta = client->getRealCurrentLeader(source);
  std::cout << "Real Current Leader ip " << meta.ip << " port " << meta.port << std::endl;

  while (1) {
    LogEntry le;
    int ret = client->read(le);
    if (ret == 0) {
      usleep(atol(argv[4]));
      std::cout << std::endl << "term: " << le.term() << ", index: " << le.index() << std::endl;
      if (le.value().length() >= 15)
        std::cout << "value: " << le.value().substr(0, 10) << "..." << std::endl;
      else
        std::cout << "value: " << le.value() << std::endl;
    } else {
      std::cout << "fail, error code is " << ret << std::endl;
    }
  }
  client->close();
//  client->deregisterLearner(rl, argv[1]);
  delete client;
  return 0;
}

