/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  dummy_learner.cc,v 1.0 04/05/2017 7:35:17 PM jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file dummy_learner.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 04/05/2017 7:35:17 PM
 * @version 1.0
 * @brief dummy_learner: transfer backup data to OSS
 *
 **/
#include <iostream>
#include <unistd.h>
#include <string>
#include <stddef.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include "easy_log.h"
#include "learner_client.h"

using namespace alisql;
using namespace std;

#define BUFFER_SIZE 128

/**
 * ./dummy_learner <socketFilePath> <startLogIndex> <endLogIndex>
 *
 * To test socket: nc -U /tmp/dl.sock
 */

int main(int argc, char *argv[])
{
  if (argc < 6) {
    easy_error_log("Usage: ./dummy_learner <clusterId> <listenAddr> <socketFilePath> <startLogIndex> <endLogIndex>\n");
    easy_error_log("Example: ./dummy_learner 1 127.0.0.1:10005 /tmp/dl.sock 1 0\n");
    return 1;
  }
  uint64_t clusterId = strtoull(argv[1], NULL, 10);
  const char *laddr = argv[2];
  const char *socketFilePath = argv[3];
  uint64_t startLogIndex= strtoull(argv[4], NULL, 10);
  uint64_t endLogIndex= strtoull(argv[5], NULL, 10);
  if (endLogIndex != 0 && startLogIndex > endLogIndex) {
    easy_error_log("Error: startLogIndex is larger than endLogIndex.\n");
    return 1;
  }

  int socketfd, afd, ret = 0;
  struct sockaddr_un sa;
  char wbuf[BUFFER_SIZE];

  /* decide write to socket or file */
  int slen = strlen(socketFilePath);
  if (strcmp(socketFilePath+slen-5, ".sock") != 0)
  {
    afd = open(socketFilePath, O_RDWR|O_CREAT, 0666);
    if (afd < 0)
    {
      easy_error_log("Fail to open file.\n");
      return 1;
    }
  }
  else
  {
    /* open the socket */
    unlink(socketFilePath);
    socketfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (socketfd == -1) {
      easy_error_log("Fail to create socket.\n");
      return 1;
    }
    easy_warn_log("Create socket done.\n");
    memset(&sa, 0, sizeof(struct sockaddr_un));
    sa.sun_family = AF_UNIX;
    strncpy(sa.sun_path, socketFilePath, sizeof(sa.sun_path)-1);
    int len = offsetof(struct sockaddr_un, sun_path) + strlen(sa.sun_path);
    if (bind(socketfd, (struct sockaddr *) &sa, len) == -1) {
      easy_error_log("Fail to bind socket.\n");
      close(socketfd);
      return 1;
    }
    easy_warn_log("Bind socket to socket file %s.\n", sa.sun_path);
    if (listen(socketfd, 1) == -1) {
      easy_error_log("Fail to listen socket.\n");
      close(socketfd);
      return 1;
    }
    easy_warn_log("Listen to socket done.\n");
  }
  /* start learner client */
  LearnerClient *client = new LearnerClient();
  if (client->open(laddr, clusterId, startLogIndex, 0, 100) != 0) {
    easy_error_log("Open client fail.\n");
    close(socketfd);
    return 1;
  }
  easy_warn_log("Start new learner client with learner address %s and startLogIndex %llu.\n", laddr, startLogIndex);
  if (strcmp(socketFilePath+slen-5, ".sock") == 0)
  {
    easy_warn_log("Waiting socket connection...\n");
    afd = accept(socketfd, NULL, NULL);
    if(afd < 0) {
      easy_error_log("Fail to accept socket.\n");
      close(socketfd);
      return 1;
    }
    easy_warn_log("Get a new socket connection, start reading log...\n");
  }
  /* write data */
  while(1) {
    LogEntry le;
    client->read(le);
    uint64_t contentlen = le.ByteSize();
    char *logdata = new char[contentlen+10];
    le.SerializeToArray(logdata, contentlen);
    uint64_t tmp;
    tmp = le.term();
    memcpy(wbuf, (char *)&tmp, sizeof(uint64_t));
    tmp = le.index();
    memcpy(wbuf + sizeof(uint64_t)*1, (char *)&tmp, sizeof(uint64_t));
    tmp = le.optype();
    memcpy(wbuf + sizeof(uint64_t)*2, (char *)&tmp, sizeof(uint64_t));
    memcpy(wbuf + sizeof(uint64_t)*3, (char *)&contentlen, sizeof(uint64_t));
    if (write(afd, wbuf, sizeof(uint64_t)*4) == -1) {
      easy_error_log("Fail to write to socket. Error %s\n", strerror(errno));
      ret = 1;
      break;
    }
    if (write(afd, logdata, contentlen) == -1) {
      easy_error_log("Fail to write to socket. Error %s\n", strerror(errno));
      ret = 1;
      break;
    }
    easy_warn_log("Send log index %llu, contentlen %llu, term %llu, optype %llu.\n", le.index(), contentlen, le.term(), le.optype());
    if (endLogIndex != 0 && le.index() >= endLogIndex) {
      easy_warn_log("Finish sending log from %llu to %llu.\n", startLogIndex, endLogIndex);
      break;
    }
  }
  delete client;
  close(afd);
  close(socketfd);
  return ret;
}


