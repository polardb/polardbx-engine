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
#include "easyNet.h"
#include "service.h"
#include "client_service.h"
#include "paxos.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "paxos_configuration.h"
#include "paxos.pb.h"

#include "rd_paxos_log.h"

using namespace alisql;

int main(int argc, char *argv[])
{
  uint64_t index;

  if (argc < 2)
  {
    std::cerr<< "Need an Argument." <<std::endl;
    return 1;
  }
  index= atol(argv[1]);
  std::cout<< "Index: " << index << " ." <<std::endl;
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:10001");
  strConfig.emplace_back("127.0.0.1:10002");
  strConfig.emplace_back("127.0.0.1:10003");

  std::shared_ptr<PaxosLog> rlog= std::make_shared<RDPaxosLog>(std::string("paxosLogTestDir")+strConfig[index-1], true, 4 * 1024 * 1024);
  Paxos *paxos1= new Paxos(5000, rlog);
  paxos1->init(strConfig, index, new ClientService());



  sleep(1000);
  return 0;
}				  //function main 
