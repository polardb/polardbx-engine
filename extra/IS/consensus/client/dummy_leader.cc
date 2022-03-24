/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  dummy_leader.cc,v 1.0 05/08/2017 3:47:33 PM jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file dummy_leader.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 05/08/2017 3:47:33 PM
 * @version 1.0
 * @brief
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
#include <errno.h>
#include "single_leader.h"

using namespace alisql;
using namespace std;

#define PARSE_THRESHOLD (30*1024*1024) // 30M
#define BLOB_SPLIT (2*1024*1024) // 2M

/*
 * For test:
 * ./echo_debug 1 127.0.0.1:11001 1 127.0.0.1:11002
 * ./dummy_learner 1 127.0.0.1:11002 ./dl.sock 0 0
 * ./dummy_leader 1 11003 127.0.0.1:11004 ./dl.sock 0 0
 * ./echo_learner_client 127.0.0.1:11004 0 1 0
 */
/*
 * add a special mode: read from file
 * socketFilePath ends with '.sock': read from socket
 * others: read from file
 */

struct BlobEntryManager {
  BlobEntryManager(): startindex(0), endindex(0) {}
  void reset()
  {
    startindex = endindex = 0;
    for (auto leptr: entryList)
      delete leptr;
    entryList.clear();
  }
  uint64_t startindex;
  uint64_t endindex;
  vector<LogEntry*> entryList;
};

BlobEntryManager blobMgr;

int processBlobEntry(LogEntry &le)
{
  /*
   * FLAG_BLOB          = 1 << 5
   * FLAG_BLOB_END      = 1 << 6
   * FLAG_BLOB_START    = 1 << 7
   */
  if (le.info() & (1 << 7))
  {
    assert(blobMgr.startindex == 0);
    assert(blobMgr.endindex == 0);
    blobMgr.startindex = le.index();
  }

  LogEntry *newle = new LogEntry();
  newle->set_term(le.term());
  newle->set_index(le.index());
  newle->set_optype(le.optype());
  newle->set_info(le.info());
  newle->set_checksum(le.checksum());
  blobMgr.entryList.push_back(newle);

  if (le.info() & (1 << 6)) {
    if (blobMgr.startindex == 0)
    {
      easy_error_log("blob entry parse fail, start index not found.");
      return 1;
    }
    blobMgr.endindex = le.index();
    const char *buf = le.value().c_str();
    uint64_t buf_len = le.value().length();
    uint64_t splitlen = buf_len / (blobMgr.endindex - blobMgr.startindex + 1);
    splitlen = BLOB_SPLIT;
    uint64_t spos = 0;
    uint64_t epos = spos + splitlen > buf_len? buf_len: spos + splitlen;
    for (int i=0; i<blobMgr.entryList.size(); ++i)
    {
      if (i == (blobMgr.entryList.size()-1))
        epos = buf_len;
      blobMgr.entryList[i]->set_value(buf + spos, epos - spos);
      spos = epos;
      epos = spos + splitlen > buf_len? buf_len: spos + splitlen;
    }
    assert(epos == buf_len);
  }
  return 0;
}

int main(int argc, char *argv[])
{
  if (argc < 7)
  {
    easy_error_log("Usage: ./dummy_leader <clusterId> <leaderPort> <learnerAddr> <socketFilePath> <lastLogIndex> <lastTerm>\n");
    easy_error_log("Example: ./dummy_leader 1 10001 127.0.0.1:10005 /tmp/dl.sock 0 0\n");
    return 1;
  }
  uint64_t clusterId = strtoull(argv[1], NULL, 10);
  int port = atol(argv[2]);
  const char *laddr = argv[3];
  const char *socketFilePath = argv[4];
  uint64_t lastLogIndex = strtoull(argv[5], NULL, 10);
  uint64_t lastTerm = strtoull(argv[6], NULL, 10);

  int socketfd;

  /* decide read from socket or file */
  int slen = strlen(socketFilePath);
  if (strcmp(socketFilePath+slen-5, ".sock") == 0)
  {
    struct sockaddr_un sa;
    // connect socket
    socketfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (socketfd < 0) {
      easy_error_log("Fail to create socket.\n");
      return 1;
    }
    memset(&sa, 0, sizeof(struct sockaddr_un));
    sa.sun_family = AF_UNIX;
    strncpy(sa.sun_path, socketFilePath, sizeof(sa.sun_path)-1);
    int len = offsetof(struct sockaddr_un, sun_path) + strlen(sa.sun_path);
    if (connect(socketfd, (struct sockaddr *) &sa, len) < 0) {
      easy_error_log("Fail to connect socket.\n");
      close(socketfd);
      return 1;
    }
  }
  else
  {
    /* read from file */
    socketfd = open(socketFilePath, O_RDONLY);
    if (socketfd < 0)
    {
      easy_error_log("Fail to read file, errno = %d.\n", errno);
      return 1;
    }
  }
  // read from socket and send log
  SingleLeader *dl = new SingleLeader(clusterId);
  dl->init(port, laddr, lastLogIndex, lastTerm);

  uint64_t w;
  uint64_t rindex, term, optype;
  int ret = 0;
  bool markStart= false; // make sure we send logentry: index=lastLogIndex+1
  while(1) {
    LogEntry le;
    if (read(socketfd, (void *)&w, sizeof(uint64_t)) < (ssize_t)sizeof(uint64_t))
    {
      easy_error_log("Fail to read from fd or EOF. Error %s\n", strerror(errno));
      ret = 1;
      break;
    }
    term = w;
    if (read(socketfd, (void *)&w, sizeof(uint64_t)) < (ssize_t)sizeof(uint64_t))
    {
      easy_error_log("Fail to read from fd or EOF. Error %s\n", strerror(errno));
      ret = 1;
      break;
    }
    rindex = w;
    if (read(socketfd, (void *)&w, sizeof(uint64_t)) < (ssize_t)sizeof(uint64_t))
    {
      easy_error_log("Fail to read from fd or EOF. Error %s\n", strerror(errno));
      ret = 1;
      break;
    }
    optype = w;
    if (read(socketfd, (void *)&w, sizeof(uint64_t)) < (ssize_t)sizeof(uint64_t))
    {
      easy_error_log("Fail to read from fd or EOF. Error %s\n", strerror(errno));
      ret = 1;
      break;
    }
    char *conbuf = new char[w+100];
    if (read(socketfd, conbuf, w) < (ssize_t)w)
    {
      easy_error_log("Fail to read from fd or EOF. Error %s\n", strerror(errno));
      ret = 1;
      delete conbuf;
      break;
    }
    if (w < PARSE_THRESHOLD)
    {
      le.ParseFromArray((void*)conbuf, w);
    }
    else
    {
      if (!MyParseFromArray(le, conbuf, w))
      {
        easy_error_log("Error parse logentry (index: %d).", rindex);
        abort();
      }
    }
    delete conbuf;
    assert(le.index() == rindex);
    assert(le.term() == term);
    assert(le.optype() == optype);
    if (rindex == (lastLogIndex+1))
      markStart = true;
    if (!markStart && rindex > (lastLogIndex+1))
    {
      easy_error_log("The current logindex is %ld, logindex %ld not found, recheck the data source.\n", rindex, (lastLogIndex+1));
      abort();
    }
    if (le.info() & (1 << 5 | 1 << 6))
    {
      if (processBlobEntry(le))
        abort();
      if (le.info() & (1 << 6)) // blob end
      {
        for (auto leptr: blobMgr.entryList)
        {
          if (leptr->index() > lastLogIndex)
          {
            dl->replicateLog(*leptr);
          }
        }
        blobMgr.reset();
      }
    }
    else
    {
      if (markStart)
        dl->replicateLog(le);
    }
  }
  close(socketfd);
  /* wait to send all data before stop */
  while(dl->getLearnerMatchIndex() < dl->getLastLogIndex())
  {
    dl->appendLogToLearner(true);
    sleep(1);
  }
  delete dl;
  return ret;
}
