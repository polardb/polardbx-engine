/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  cli.cc,v 1.0 08/25/2016 04:53:21 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file cli.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/25/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <gflags/gflags.h>
#include <iostream>
#include "raft.h"
#include "cli_sdk.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>

DECLARE_string(cli_cmd);
DECLARE_string(cli_key);
DECLARE_string(cli_value);
DECLARE_uint64(cli_cas);

using namespace alisql;

bool is_quit= false;

void resetFlags()
{
  printf("AliRocks cluster> ");
  std::vector<std::string> commands;

  std::string buf;
  getline(std::cin, buf, '\n');
  boost::split(commands, buf, boost::is_any_of(" \t\n"), boost::token_compress_on);
  for (int i= commands.size(); i < 4; ++i)
  {
    commands.push_back("");
  }

  FLAGS_cli_cmd= commands[0];
  if (FLAGS_cli_cmd == "delete" || FLAGS_cli_cmd == "get" ||
      FLAGS_cli_cmd == "gets")
  {
    FLAGS_cli_key= commands[1];
  }
  else if (FLAGS_cli_cmd == "put")
  {
    FLAGS_cli_key= commands[1];
    FLAGS_cli_value= commands[2];
  }
  else if (FLAGS_cli_cmd == "cas")
  {
    FLAGS_cli_key= commands[1];
    FLAGS_cli_value= commands[2];
    try
    {
      FLAGS_cli_cas= boost::lexical_cast<uint64_t>(commands[3]);
    }
    catch(std::exception& e)
    {
      printf("Error, cas unique is not set as number\n");
      resetFlags();
    }
  }
  else if (FLAGS_cli_cmd == "quit")
  {
    is_quit= true;
  }
  else
  {
  }
}

void usage() {
  std::cout
    << "Usage: " << std::endl
    << "stats [ show cluster stats ]" << std::endl
    << "put (key) (value) [ set the value of key, e.g. put k1 v1 ] " << std::endl
    << "get (key) [ read the data by key, e.g. get k1 ]" << std::endl
    << "gets (key) [ read the data and cas unique by key, e.g. get k1 ]" << std::endl
    << "delete (key) [ delete the key, e.g. delete k1 ]" << std::endl
    << "cas (key) (value) (cas unique) [check and set operation, e.g. cas k1 v1 6]" << std::endl
    << "quit [ exit the cli ]" << std::endl;
 }

int main(int argc, char* argv[])
{
  std::vector<std::string> members;
  CliSDK::parseArgs(argc, argv, members);
  CliSDK cliSdk(members);
  int ret= -1;
  memcached_return_t rc;
  do {
    if (FLAGS_cli_cmd == "put")
    {
      std::string key= FLAGS_cli_key;
      std::string value= FLAGS_cli_value;
      ret= cliSdk.put(key, value);
      rc= cliSdk.getMemcachedRc();
      std::cout << memcached_strerror(NULL, rc) << std::endl;
    }
    else if (FLAGS_cli_cmd == "delete")
    {
      std::string key= FLAGS_cli_key;
      ret= cliSdk.del(key);
      rc= cliSdk.getMemcachedRc();
      std::cout << memcached_strerror(NULL, rc) << std::endl;
    }
    else if (FLAGS_cli_cmd == "get")
    {
      std::string key= FLAGS_cli_key;
      std::string value;
      ret= cliSdk.get(key, value);
      rc= cliSdk.getMemcachedRc();
      if (ret == 0)
      {
        std::cout << "value: " << value << std::endl;
      }
      else
      {
        std::cout << memcached_strerror(NULL, rc) << std::endl;
      }
    }
    else if (FLAGS_cli_cmd == "gets")
    {
      std::string key= FLAGS_cli_key;
      std::string value;
      ret= cliSdk.gets(key, value);
      uint64_t casUnique= cliSdk.getCasUnique();
      rc= cliSdk.getMemcachedRc();
      if (ret == 0)
      {
        std::cout << "value: " << value << ", cas unique: " 
                  << casUnique << std::endl;
      }
      else
      {
        std::cout << memcached_strerror(NULL, rc) << std::endl;
      }
    }
    else if (FLAGS_cli_cmd == "cas")
    {
      std::string key= FLAGS_cli_key;
      std::string value= FLAGS_cli_value;
      uint64_t casUnique= FLAGS_cli_cas;
      cliSdk.setCasUnique(casUnique);
      ret= cliSdk.cas(key, value);
      rc= cliSdk.getMemcachedRc();
      std::cout << memcached_strerror(NULL, rc) << std::endl;
    }
    else if (FLAGS_cli_cmd == "stats")
    {
      ret= cliSdk.showStats();
      if (ret != 0)
      {
        LOG_INFO("show stats failed\n");
      }
    }
    else
    {
      usage();
    }
    resetFlags();
  } while (!is_quit);

  return 0;
}
