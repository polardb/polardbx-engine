#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <iostream>
#include <getopt.h>
#include <memory>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include "../client/cli_sdk.h"
#include <boost/lexical_cast.hpp>  
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <fstream>
#include <iostream>
#include <sstream> 

using namespace alisql;

#define PROGRAM_NAME "AliRocks cluster function test"
#define PROGRAM_DESCRIPTION "AliRocks cluster function test program"

/* Types */
enum TestType
{
  SET_TEST,
  GET_TEST,
  GET_ALL_TEST, //verify the data got from all servers are correct
  DELETE_TEST,
  NOCAS_TEST,
  CAS_TEST,
  LOAD_TEST,
  DUMP_TEST,
  LOAD_FROM_FILE_TEST
};

struct KeyValuePair
{
  std::string key;
  std::string value;
};

static unsigned int optExecuteNumber= 0;
static unsigned int optConcurrency= 0;
static unsigned int optSleep= 0;
static char *optServers= NULL;
static char *optLoadSource= NULL;
TestType optTest= SET_TEST;

#define DEFAULT_EXECUTE_NUMBER 10000
#define DEFAULT_CONCURRENCY 1

/* Global Thread counter */
volatile unsigned int masterWakeup;
pthread_mutex_t sleeperMutex;
pthread_cond_t sleepThreshhold;
std::vector<std::string> members;

struct ThreadContext
{
  std::vector<KeyValuePair> executePairs;
  TestType test;
  uint64_t start;
  uint64_t end;
  uint32_t threadID;

  ThreadContext(TestType testArg) :
    test(testArg),
    start(0),
    end(0),
    threadID(0)
  {
  }

  ~ThreadContext()
  {
  }
};

struct Conclusions {
  long int loadTime;
  long int readTime;
  unsigned int rowsLoaded;
  unsigned int rowsRead;

  Conclusions() :
    loadTime(0),
    readTime(0),
    rowsLoaded(0),
    rowsRead()
  { }
};

enum RDOptions {
  OPT_SERVERS= 's',
  OPT_HELP= 'h',
  OPT_EXECUTE_NUMBER,
  OPT_TEST,
  OPT_CONCURRENCY,
  OPT_SLEEP,
  OPT_LOAD_SOURCE
};

static const char *lookupHelp(RDOptions option)
{
  switch (option)
  {
    case OPT_SERVERS: 
      return("List which servers you wish to connect to.");
    case OPT_HELP: 
      return("Display this message and then exit.");
    case OPT_EXECUTE_NUMBER: 
      return("Number of times to execute the given test.");
    case OPT_TEST: 
      return("Test to run (currently \"get\", \"set\" or \"delete\").");
    case OPT_CONCURRENCY: 
      return("Number of users to simulate with load.");
    case OPT_SLEEP:
      return ("Sleep # ms before next action.");
    default:
      break;
  };

  return "unknown function";
}

static void helpCommand(const char *commandName, const char *description,
                        const struct option *longOptions)
{
  unsigned int i;

  printf("%s v%u.%u\n\n", commandName, 1U, 0U);
  printf("\t%s\n\n", description);
  printf("Current options. A '=' means the option takes a value.\n\n");

  for (i= 0; longOptions[i].name; i++)
  {
    const char *helpMessage;

    printf("\t --%s%c\n", longOptions[i].name,
           longOptions[i].has_arg ? '=' : ' ');
    if ((helpMessage= lookupHelp(RDOptions(longOptions[i].val))))
      printf("\t\t%s\n", helpMessage);
  }

  printf("\n");
  exit(0);
}

int executeNoCas(CliSDK* sdk)
{
  uint64_t i= 0;
  int success = 0;
  int rc= -1;
  std::string key= "cas";
  std::string value;
  char valueBuf[128]= {'\0'};
  while (i < optExecuteNumber)
  {
    i++;
    if (optSleep > 0)
    {
      usleep(optSleep * 1000);
    }

    rc= sdk->get(key, value);
    if (rc != 0)
    {
      printf("fail to get key: %s\n", key.c_str());
      return -1;
    }
    else
    {
      int curVal= boost::lexical_cast<int>(value);
      curVal += 1;
      snprintf(valueBuf, sizeof(valueBuf), "%d", curVal);
      value= valueBuf;
      rc= sdk->put(key, value);
      if (rc != 0)
      {
        printf("fail to set value: %s\n", value.c_str());
        return -1;
      }
    }
  }

  return rc;
}

int executeCas(CliSDK* sdk, ThreadContext *context)
{
  uint64_t i= 0;
  int success = 0;
  int rc= -1;
  std::string key= "cas";
  std::string value;
  char valueBuf[128]= {'\0'};
  while (i < optExecuteNumber)
  {
    if (optSleep > 0)
    {
      usleep(optSleep * 1000);
    }

    rc= sdk->gets(key, value);
    if (rc != 0)
    {
      printf("Thd %d, fail to get key: %s\n", context->threadID, key.c_str());
      return -1;
    }
    else
    {
      uint64_t casUnique= sdk->getCasUnique();
      int curVal= boost::lexical_cast<int>(value);
      curVal += 1;
      snprintf(valueBuf, sizeof(valueBuf), "%d", curVal);
      value= valueBuf;
      sdk->setCasUnique(casUnique);
      printf("Thd %d, cas unique: %ld, value: %s\n",context->threadID, casUnique, value.c_str());
      rc= sdk->cas(key, value);
      memcached_return_t mrc= sdk->getMemcachedRc();
      if (rc != 0)
      {
        std::cout << "Thd " << context->threadID << " " << memcached_strerror(NULL, mrc) << std::endl;
        usleep(10 * 1000); //sleep 10ms
      }
      else
      {
        std::cout << "Thd " << context->threadID << " " << memcached_strerror(NULL, mrc) << std::endl;
        i++;
      }
    }
  }

  return rc;
}

uint64_t executeSet(CliSDK* sdk, uint64_t start, uint64_t end)
{
  uint64_t i= 0;
  int rc= -1;
  int tryCount= 0;
  char keyBuf[128]= {'\0'};
  char valueBuf[128]= {'\0'};
  std::string key;
  std::string value;
  for (i= start; i <= end; ++i)
  {
    if (optSleep > 0)
    {
      usleep(optSleep * 1000);
    }
    snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
    snprintf(valueBuf, sizeof(valueBuf), "value_%d", i); 
    key= keyBuf;
    value= valueBuf;
    rc= sdk->put(key, value);
    if (rc != 0)
    {
      if (tryCount < 100)
      {
        i--;
        printf("try put again: %s\n", key.c_str());
        sleep(1);
        tryCount++;
        continue;
      }
      else
      {
        tryCount= 0;
        printf("fail to put: %s\n", key.c_str());
        break;
      }
    }

    if (i % 1000 == 0)
    {
      printf("set %s\n", key.c_str());
    }
  }

  return i;
}

uint64_t executeGet(CliSDK* sdk, uint64_t start, uint64_t end)
{
  uint64_t i= 0;
  int rc= -1;
  char keyBuf[128]= {'\0'};
  char valueBuf[128]= {'\0'};
  std::string key;
  std::string value;
  for (i= start; i <= end; ++i)
  {
    if (optSleep > 0)
    {
      usleep(optSleep * 1000);
    }
    snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
    snprintf(valueBuf, sizeof(valueBuf), "value_%d", i); 
    key= keyBuf;
    rc= sdk->get(key, value);
    if (rc != 0)
    {
      printf("fail to get key: %s\n", key.c_str());
    }
    else 
    {
      // if (strcmp(value.c_str(), valueBuf) != 0)
      // {
      //   std::cout << "No match, expected value: " << valueBuf
      //             << " return value: " << value << std::endl;
      // }
    }
  }

  return i;
}

uint64_t executeDelete(CliSDK* sdk, uint64_t start, uint64_t end)
{
  uint64_t i= 0;
  int rc= -1;
  int tryCount= 0;
  char keyBuf[128]= {'\0'};
  char valueBuf[128]= {'\0'};
  std::string key;
  std::string value;
  for (i= start; i <= end; ++i)
  {
    if (optSleep > 0)
    {
      usleep(optSleep * 1000);
    }
    snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
    key= keyBuf;
    rc= sdk->del(key);
    if (rc != 0)
    {
      if (tryCount < 100)
      {
        i--;
        printf("try delete again: %s\n", key.c_str());
        sleep(1);
        tryCount++;
        continue;
      }
      else
      {
        tryCount= 0;
        printf("fail to delete: %s\n", key.c_str());
        break;
      }
    }
    if (i % 1000 == 0)
    {
      printf("delete %s\n", key.c_str());
    }
  }

  return i;
}

uint64_t executeLoad(CliSDK* sdk, CliSDK* loadSourcecCli, 
                     uint64_t start, uint64_t end)
{
  uint64_t i= 0;
  int rc= -1;
  char keyBuf[128]= {'\0'};
  int tryCount= 0;
  std::string key;
  std::string value;
  std::ofstream file;
  file.open("data.txt");

  for (i= start; i <= end; ++i)
  {
    if (optSleep > 0)
    {
      usleep(optSleep * 1000);
    }
    snprintf(keyBuf, sizeof(keyBuf), "@@tc_biz_order_0016.%d", i);
    key= keyBuf;
    rc= loadSourcecCli->get(key, value);
    if (rc != 0)
    {
      printf("fail to get key: %s\n", key.c_str());
    }
    else 
    {
      rc= sdk->put(key, value);
      if (rc != 0)
      {
        if (tryCount < 100)
        {
          i--;
          printf("try put again: %s\n", key.c_str());
          sleep(1);
          tryCount++;
          continue;
        }
        else
        {
          tryCount= 0;
          printf("fail to put: %s\n", key.c_str());
          break;
        }
      }
    }

    if (i % 1000 == 0)
    {
      printf("load %s\n", key.c_str());
    }
  }

  file.close();
  return i;
}

uint64_t executeLoadFromFile(CliSDK* sdk, uint64_t start, uint64_t end)
{
  uint64_t i= 0;
  int rc= -1;
  std::string key;
  std::string value;
  std::ifstream in("data.txt");  

  if (!in.is_open())  
  { 
    std::cout << "Error opening file"; 
    return -1; 
  }

  for (i= start; i <= end && !in.eof(); ++i)
  {
    std::getline(in, key); 
    std::getline(in, value);
    rc= sdk->put(key, value);
    if (rc != 0)
    {
      for (int j= 0; j < 100; ++j)
      {
        printf("try put again: %s\n", key.c_str());
        rc= sdk->put(key, value);
        if (rc == 0)
        {
          break;
        }
      }
    }

    if (i % 1000 == 0)
    {
      printf("load %s\n", key.c_str());
    }
  }

  return i;
}

uint64_t executeDump(CliSDK* loadSourcecCli, 
                     uint64_t start, uint64_t end)
{
  uint64_t i= 0;
  int rc= -1;
  char keyBuf[128]= {'\0'};
  int tryCount= 0;
  std::string key;
  std::string value;
  std::ofstream file;
  file.open("data.txt");

  for (i= start; i <= end; ++i)
  {
    snprintf(keyBuf, sizeof(keyBuf), "@@tc_biz_order_0016.%d", i);
    key= keyBuf;
    value.clear();
    rc= loadSourcecCli->get(key, value);
    if (rc != 0)
    {
      printf("fail to get key: %s\n", key.c_str());
    }
    else
    {
      file << key << std::endl;
      file << value << std::endl;     
    }

    if (i % 1000 == 0)
    {
      printf("dump %s\n", key.c_str());
    }
  }

  file.close();
  return i;
}

long int timedif(struct timeval a, struct timeval b)
{
  long us, s;

  us = (int)(a.tv_usec - b.tv_usec);
  us /= 1000;
  s = (int)(a.tv_sec - b.tv_sec);
  s *= 1000;
  return s + us;
}

extern "C" {

static __attribute__((noreturn)) void *runTask(void *p)
{
  ThreadContext *context= (ThreadContext *)p;

  pthread_mutex_lock(&sleeperMutex);
  while (masterWakeup)
  {
    pthread_cond_wait(&sleepThreshhold, &sleeperMutex);
  }
  pthread_mutex_unlock(&sleeperMutex);
  CliSDK cliSdk(members);
  
  /* Do Stuff */
  switch (context->test)
  {
    case SET_TEST:
      executeSet(&cliSdk, context->start, context->end);
      break;
  
    case GET_TEST:
      executeGet(&cliSdk, context->start, context->end);
      break;
  
    case DELETE_TEST:
      executeDelete(&cliSdk, context->start, context->end);
      break;
  
    case GET_ALL_TEST:
    {
      std::vector<std::string> memberList;
      for (int i= 0; i < members.size(); i++)
      {
        memberList.clear();
        std::string member= members[i];
        memberList.push_back(member);
        CliSDK sdk(memberList);
        executeGet(&cliSdk, context->start, context->end);
      }
      break;
    }

	  case NOCAS_TEST:
      executeNoCas(&cliSdk);
      break;
  
    case CAS_TEST:
      executeCas(&cliSdk, context);
      break;

    case DUMP_TEST:
    {
      std::vector<std::string> loadSource;
      loadSource.push_back(optLoadSource);
      CliSDK loadSourcecCli(loadSource);
      executeDump(&loadSourcecCli, context->start, context->end);
      break;
    }

    case LOAD_TEST:
    {
      std::vector<std::string> loadSource;
      loadSource.push_back(optLoadSource);
      CliSDK loadSourcecCli(loadSource);
      executeLoad(&cliSdk, &loadSourcecCli, context->start, context->end);
      break;
    }
    
    case LOAD_FROM_FILE_TEST:
    {
      executeLoadFromFile(&cliSdk, context->start, context->end);
      break;
    }

    default:
      break;
  }

  delete context;

  pthread_exit(0);
}

}

void conclusionsPrint(Conclusions *conclusion)
{
  printf("\tThreads connecting to servers %u\n", optConcurrency);
  if (optTest == SET_TEST || optTest == CAS_TEST || 
      optTest == NOCAS_TEST || optTest == DELETE_TEST)
    printf("\tTook %ld.%03ld seconds to load data\n", conclusion->loadTime / 1000,
           conclusion->loadTime % 1000);
  else
    printf("\tTook %ld.%03ld seconds to read data\n", conclusion->readTime / 1000,
           conclusion->readTime % 1000);
}

void optionsParse(int argc, char *argv[])
{
  static struct option longOptions[]=
    {
      {"concurrency", required_argument, NULL, OPT_CONCURRENCY},
      {"execute-number", required_argument, NULL, OPT_EXECUTE_NUMBER},
      {"help", no_argument, NULL, OPT_HELP},
      {"servers", required_argument, NULL, OPT_SERVERS},
      {"test", required_argument, NULL, OPT_TEST},
      {"sleep", required_argument, NULL, OPT_SLEEP},
      {"loadsources", required_argument, NULL, OPT_LOAD_SOURCE},
      {0, 0, 0, 0},
    };

  bool optHelp= false;
  int optionIndex= 0;
  while (1)
  {
    int optionRv= getopt_long(argc, argv, "Vhvds:", longOptions, &optionIndex);

    if (optionRv == -1) break;

    switch (optionRv)
    {
    case 0:
      break;

    case OPT_HELP: /* --help or -h */
      optHelp= true;
      break;

    case OPT_SERVERS: /* --servers or -s */
      optServers= strdup(optarg);
      break;

    case OPT_LOAD_SOURCE:
      optLoadSource= strdup(optarg);
      break;

    case OPT_TEST:
      if (strcmp(optarg, "get") == 0)
      {
        optTest= GET_TEST ;
      }
      else if (strcmp(optarg, "set") == 0)
      {
        optTest= SET_TEST;
      }
      else if (strcmp(optarg, "delete") == 0)
      {
        optTest= DELETE_TEST;
      }
      else if (strcmp(optarg, "getall") == 0)
      {
        optTest= GET_ALL_TEST;
      }
	    else if (strcmp(optarg, "cas") == 0)
      {
        optTest= CAS_TEST;
      }
      else if (strcmp(optarg, "nocas") == 0)
      {
        optTest= NOCAS_TEST;
      }
      else if (strcmp(optarg, "load") == 0)
      {
        optTest= LOAD_TEST;
      }
      else if (strcmp(optarg, "dump") == 0)
      {
        optTest= DUMP_TEST;
      }
      else if (strcmp(optarg, "loadfromfile") == 0)
      {
        optTest= LOAD_FROM_FILE_TEST;
      }
      else
      {
        fprintf(stderr, "Your test, %s, is not a known test\n", optarg);
        exit(-1);
      }
      break;

    case OPT_CONCURRENCY:
      errno= 0;
      optConcurrency= (unsigned int)strtoul(optarg, (char **)NULL, 10);
      if (errno != 0)
      {
        fprintf(stderr, "Invalid value for concurrency: %s\n", optarg);
        exit(-1);
      }
      break;

    case OPT_EXECUTE_NUMBER:
      errno= 0;
      optExecuteNumber= (unsigned int)strtoul(optarg, (char **)NULL, 10);
      if (errno != 0)
      {
        fprintf(stderr, "Invalid value for execute: %s\n", optarg);
        exit(-1);
      }
      break;

    case OPT_SLEEP:
      errno= 0;
      optSleep= (unsigned int)strtoul(optarg, (char **)NULL, 10);
      if (errno != 0)
      {
        fprintf(stderr, "Invalid value for sleep: %s\n", optarg);
        exit(-1);
      }
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(-1);

    default:
      abort();
    }
  }

  if (optHelp)
  {
    helpCommand(PROGRAM_NAME, PROGRAM_DESCRIPTION, longOptions);
    exit(0);
  }

  if (optExecuteNumber == 0)
    optExecuteNumber= DEFAULT_EXECUTE_NUMBER;

  if (optConcurrency == 0)
    optConcurrency= DEFAULT_CONCURRENCY;
}

KeyValuePair *pairsGenerate(uint64_t start, uint64_t end, std::vector<KeyValuePair> &executePairs)
{
  char keyBuf[128]= {'\0'};
  char valueBuf[128]= {'\0'};
  for (uint64_t i= start; i <= end; i++)
  {
    KeyValuePair pairs;
    snprintf(keyBuf, sizeof(keyBuf), "key_%d", i);
    snprintf(valueBuf, sizeof(valueBuf), "value_%d", i); 
    pairs.key= keyBuf;
    pairs.value= valueBuf;
    executePairs.push_back(pairs);
  }
}

void rdScheduler(Conclusions *conclusion)
{
  struct timeval startTime, endTime;
  int rc= -1;
  pthread_mutex_lock(&sleeperMutex);
  masterWakeup= 1;
  pthread_mutex_unlock(&sleeperMutex);
  CliSDK cliSdk(members);
  std::string key= "cas";
  std::string value= "0";
  if (optTest == CAS_TEST || optTest == NOCAS_TEST)
  {
    rc= cliSdk.put(key, value);
    if (rc != 0)
    {
      fprintf(stderr,"Fail to init cas value\n");
      exit(1);
    }
  
    rc= cliSdk.get(key, value);
    if (rc != 0)
    {
      printf("fail to get key: %s\n", key.c_str());
      exit(1);
    }
    else
    {
      printf("cas init as: %s\n", value.c_str());
    }
  }
  pthread_t *threads= new (std::nothrow) pthread_t[optConcurrency];

  if (threads == NULL)
  {
    exit(-1);
  }

  uint64_t range= optExecuteNumber / optConcurrency;
  uint64_t tailer= optExecuteNumber % optConcurrency;
  
  for (uint32_t i= 0; i < optConcurrency; i++)
  {
    ThreadContext *context= new ThreadContext(optTest);
    context->test= optTest;

    uint64_t start= range * i + 1;
    uint64_t end= range * (i + 1);
    if (i == optConcurrency - 1)
    {
      end += tailer;
    }
    context->start= start;
    context->end= end;
    context->threadID= i + 1;
    //pairsGenerate(start, end, context->executePairs);
    
    /* now create the thread */
    if (pthread_create(threads + i, NULL, runTask, (void *)context) != 0)
    {
      fprintf(stderr,"Could not create thread\n");
      exit(1);
    }
  }

  pthread_mutex_lock(&sleeperMutex);
  masterWakeup= 0;
  pthread_mutex_unlock(&sleeperMutex);
  pthread_cond_broadcast(&sleepThreshhold);
  gettimeofday(&startTime, NULL);

  for (uint32_t i= 0; i < optConcurrency; i++)
  {
    void *retval;
    pthread_join(threads[i], &retval);
  }
  delete [] threads;

  if (optTest == CAS_TEST || optTest == NOCAS_TEST)
  {
    rc= cliSdk.get(key, value);
    if (rc != 0)
    {
      printf("fail to get key: %s\n", key.c_str());
    }
    else
    {
      printf("value result: %s\n", value.c_str());
    }
  }
  gettimeofday(&endTime, NULL);

  conclusion->loadTime= timedif(endTime, startTime);
  conclusion->readTime= timedif(endTime, startTime);
}


int main(int argc, char* argv[])
{
  Conclusions conclusion;
  optionsParse(argc, argv);

  if (optServers == NULL)
  {
    std::cerr << "No Servers provided" << std::endl;
    exit(-1);
  }

  try
  {
    boost::split(members, optServers,
                 boost::is_any_of(","), boost::token_compress_on);
  }
  catch(std::exception& e)
  {
    printf("Error, No Servers provided\n");
    exit(-1);
  }

  int ret= 0;

  pthread_mutex_init(&sleeperMutex, NULL);
  pthread_cond_init(&sleepThreshhold, NULL);
  
  try
  {
    rdScheduler(&conclusion);
  }
  catch(std::exception& e)
  {
    std::cerr << "Died with exception: " << e.what() << std::endl;
    ret= -1;
  }

  (void)pthread_mutex_destroy(&sleeperMutex);
  (void)pthread_cond_destroy(&sleepThreshhold);
  conclusionsPrint(&conclusion);

  return ret;
}


