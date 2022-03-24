#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <iostream>
#include "../client/cli_sdk.h"
#include <boost/lexical_cast.hpp>

using namespace alisql;

void usage()
{
  std::cout
    << "Usage: ./sample [get|put|delete] [num] --flagfile" << std::endl;
}

int main(int argc, char* argv[])
{
  int ret= -1;

  if (argc < 4)
  {
    fprintf(stderr, "./sample [get|put|delete] [num] --flagfile \n");
    return -1;
  }

  std::string command= argv[1];
  std::string numberStr= argv[2];
  int number= boost::lexical_cast<int>(numberStr);
  std::vector<std::string> members;
  CliSDK::parseArgs(argc, argv, members);
  CliSDK cliSdk(members);

  if (strcmp("put", command.c_str()) == 0)
  {
    fprintf(stderr, "put test\n");
    char keyBuf[2048]= {'\0'};
    char valueBuf[2048]= {'\0'};
    for (int i= 1; i <= number; i++)
    {
      snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
      snprintf(valueBuf, sizeof(valueBuf), "value_%d", i);
      std::string key= keyBuf;
      std::string value= valueBuf;
      ret= cliSdk.put(key, value);
      if (ret != 0)
      {
        i--;
        printf("try put again: %s\n", keyBuf);
        sleep(1);
        continue;
      }
      if (i % 1000 == 0)
      {
        printf("%s\n", keyBuf);
      }
    }
  }
  else if (strcmp("get", command.c_str()) == 0)
  {
    fprintf(stderr, "read test\n");
    char keyBuf[2048]= {'\0'};
    char valueBuf[2048]= {'\0'};
    for (int i= 1; i <= number; i++)
    {
      snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
      snprintf(valueBuf, sizeof(valueBuf), "value_%d", i);
      std::string key= keyBuf;
      std::string value;
      ret= cliSdk.get(key, value);
      if (ret != 0)
      {
        i--;
        printf("try get again: %s", keyBuf);
        sleep(1);
        continue;
      }
      else 
      {
        if (strcmp(value.c_str(), valueBuf) != 0)
        {
          std::cout << "No match, expected value: " << valueBuf << " return value: "
                    << value << std::endl;
        }
      }
      fflush(stdout);
    }
  }
  else if (strcmp("delete", command.c_str()) == 0)
  {
    fprintf(stderr, "delete test\n");
    char keyBuf[2048]= {'\0'};
    for (int i= 1; i <= number; i++)
    {
      snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
      std::string key= keyBuf;
      ret= cliSdk.del(key);
      if (ret != 0)
      {
        i--;
        printf("try delete again: %s", keyBuf);
        sleep(1);
        continue;
      }
      else 
      {
        if (i % 1000 == 0)
        {
          printf("delete %s\n", keyBuf);
        }
      }
    }
  }
  else
  {
    usage();
  }
  return ret;
}


