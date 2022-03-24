/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:   text_request.cc,v 1.0 08/31/2016 04:53:21 PM hangfeng.fj(hangfeng.fj(@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file text_request.cc
 * @author hangfeng.fj(hangfeng.fj(@alibaba-inc.com)
 * @date 08/31/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "text_request.h"
#include <stdlib.h>
#include <algorithm>
#include "memcached_utils.h"

namespace alisql {

TextRequest::TextRequest() :
                        requestLen_(0),
                        valid_(false),
                        command_(TextCommand::UNKNOWN),
                        flags_(0),
                        exptime_(0),
                        noReply_(false),
                        casUnique_(0),
                        stats_(stats_t::GENERAL),
                        namespace_(0),
                        version_(0)
{
}

void TextRequest::init(const char *p, uint32_t len)
{
  msgPos_= p;
  msgLen_= len;
}

void TextRequest::parse()
{
  const char *endOfLine= cFind(msgPos_, LF, msgLen_);
  // incomplete message
  if (endOfLine == NULL)
  {
    LOG_INFO("incomplete msg: %s length: %ld\n", msgPos_, msgLen_);
    return;
  }

  requestLen_= endOfLine - msgPos_ + 1;

  // invalid message
  if (requestLen_ == 1 || *(--endOfLine) != CR)
  {
    LOG_INFO("invalid msg: %s length: %ld\n", msgPos_, msgLen_);
    return;
  }

  const char *cmdStart= msgPos_;
  while (*cmdStart == SP)
  {
    ++cmdStart;
  }

  const char *cmdEnd= cFind(cmdStart, SP, endOfLine - cmdStart);

  if (cmdEnd == NULL)
  {
    cmdEnd= endOfLine;
  }

  uint32_t cmdLen= cmdEnd - cmdStart;

  switch (cmdLen)
  {
    case 3:
      if (memcmp(cmdStart, "set", 3) == 0)
      {
        command_= TextCommand::SET;
        parseStorage(cmdEnd, endOfLine, false, false);
        return;
      }
      if (memcmp(cmdStart, "get", 3) == 0)
      {
        command_= TextCommand::GET;
        parseGet(cmdEnd, endOfLine);
        return;
      }
      if (memcmp(cmdStart, "cas", 3) == 0)
      {
        command_= TextCommand::CAS;
        parseStorage(cmdEnd, endOfLine, true, false);
        return;
      }
      break;
    case 4:
      if (memcmp(cmdStart, "gets", 4) == 0)
      {
        command_= TextCommand::GETS;
        parseGet(cmdEnd, endOfLine);
        return;
      }
      break;
    case 5:
      if (memcmp(cmdStart, "stats", 5) == 0)
      {
        command_= TextCommand::STATS;
        parseStats(cmdEnd, endOfLine);
        return;
      }
      break;
    case 6:
      if (memcmp(cmdStart, "delete", 6) == 0)
      {
        command_= TextCommand::DELETE;
        parseDelete(cmdEnd, endOfLine);
        return;
      }
      break;
    case 7:
      if (memcmp(cmdStart, "version", 7) == 0 )
      {
        command_= TextCommand::VERSION;
        valid_= true;
        return;
      }
      break;
    case 8:
      if (memcmp(cmdStart, "tair_set", 8) == 0)
      {
        command_= TextCommand::TAIR_SET;
        parseStorage(cmdEnd, endOfLine, false, true);
        return;
      }
      break;
    default:
      LOG_INFO("Unknown cmd: %s\n", cmdStart);
      break;
  }
}

// Storage commands
// The client sends a command line which looks like this:
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
void TextRequest::parseStorage(const char *beg, const char *end,
                               bool isCas, bool isTair)
{
  // parse `key`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg == end)
  {
    return;
  }
  const char *keyEnd= cFind(beg, SP, end - beg);
  if (keyEnd == NULL)
  {
    LOG_INFO("No key found: %s\n", beg);
    return;
  }
  key_= item(beg, keyEnd - beg);
  beg= keyEnd;

  // parse `flags`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg == end)
  {
    return;
  }
  const char *flagsEnd= cFind(beg, SP, end - beg);
  if (flagsEnd == NULL)
  {
    LOG_INFO("No flags found: %s\n", beg);
    return;
  }
  bool result;
  flags_= toUint<uint32_t>(beg, result);
  if (!result)
  {
    LOG_INFO("Parse flags failed: %s\n", beg);
    return;
  }
  beg= flagsEnd;

  // parse `exptime`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg == end)
  {
    return;
  }
  const char *exptimeEnd= cFind(beg, SP, end - beg);
  if (exptimeEnd == NULL)
  {
    LOG_INFO("No exptime found: %s\n", beg);
    return;
  }
  exptime_= toUint<std::uint64_t>(beg, result);
  if (!result)
  {
    LOG_INFO("Parse exptime failed: %s\n", beg);
    return;
  }
  beg= exptimeEnd;

  // parse `bytes`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg == end)
  {
    return;
  }
  uint32_t nbytes= toUint<std::uint32_t>(beg, result);
  if (!result)
  {
    LOG_INFO("Parse bytes failed: %s\n", beg);
    return;
  }
  const char* bytesEnd= cFind(beg, SP, end - beg);
  beg= (bytesEnd == NULL) ? end : bytesEnd;

  if (isCas)
  {
    while (*beg == SP)
    {
      ++beg;
    }
    if (beg == end)
    {
      return;
    }
    casUnique_= toUint<std::uint64_t>(beg, result);
    if (!result)
    {
      LOG_INFO("Parse cas unique failed: %s\n", beg);
      return;
    }
    const char* casEnd= cFind(beg, SP, end - beg);
    beg= (casEnd == NULL) ? end : casEnd;
  }

  if (isTair)
  {
    // parse `namespace`
    while (*beg == SP)
    {
      ++beg;
    }
    if (beg == end)
    {
      return;
    }
    namespace_= toUint<std::uint64_t>(beg, result);
    if (!result)
    {
      LOG_INFO("Parse namespace failed: %s\n", beg);
      return;
    }
    const char* nameSpaceEnd= cFind(beg, SP, end - beg);
    beg= (nameSpaceEnd == NULL) ? end : nameSpaceEnd;

    // parse `version`
    while (*beg == SP)
    {
      ++beg;
    }
    if (beg == end)
    {
      return;
    }
    version_= toUint<std::uint64_t>(beg, result);
    if (!result)
    {
      LOG_INFO("Parse version failed: %s\n", beg);
      return;
    }
    const char* versionEnd= cFind(beg, SP, end - beg);
    beg= (versionEnd == NULL) ? end : versionEnd;
  }

  // parse `noreply`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg != end)
  {
    const char* noreplyEnd= cFind(beg, SP, end - beg);
    uint32_t noreplyLen= (noreplyEnd != NULL) ? (noreplyEnd - beg) :
                         (end - beg);
    if (noreplyLen != 7)
    {
      return;
    }
    if (memcmp(beg, "noreply", 7) != 0)
    {
      return;
    }
    noReply_= true;
    beg= (noreplyEnd != NULL) ? noreplyEnd : end;
  }

  while (*beg == SP)
  {
    ++beg;
  }
  if (beg != end)
  {
    return;
  }

  beg += 2; //CRLF
  requestLen_= (beg - msgPos_) + nbytes + 2;
  if (msgLen_ < requestLen_)
  {
    requestLen_= 0;
    return;
  }

  // check CRLF after data
  if (*(beg + nbytes) != CR || *(beg + nbytes + 1) != LF)
  {
    return;
  }

  data_= item(beg, nbytes);
  valid_= true;
}

void TextRequest::parseGet(const char *beg, const char *end)
{
  // parse `key`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg == end)
  {
    return;
  }
  const char *keyEnd= cFind(beg, SP, end - beg);
  if (keyEnd == NULL)
  {
    keyEnd= end;
  }
  key_= item(beg, keyEnd - beg);
  valid_= true;
}

void TextRequest::parseDelete(const char *beg, const char *end)
{
  // parse `key`
  while (*beg == SP)
  {
    ++beg;
  }
  if (beg == end)
  {
    return;
  }
  const char *keyEnd= cFind(beg, SP, end - beg);
  if (keyEnd == NULL)
  {
    keyEnd= end;
  }
  key_= item(beg, keyEnd - beg);
  beg= keyEnd;

  while (*beg == SP)
  {
    ++beg;
  }

  // TODO, remove duplicated code
  if (beg != end)
  {
    const char* noreplyEnd= cFind(beg, SP, end - beg);
    uint32_t noreplyLen= (noreplyEnd != NULL) ? (noreplyEnd - beg) :
                         (end - beg);
    if (noreplyLen != 7)
    {
      return;
    }
    if (memcmp(beg, "noreply", 7) != 0)
    {
      return;
    }
    noReply_= true;
    beg= (noreplyEnd != NULL) ? noreplyEnd : end;
  }

  while (*beg == SP) ++beg;
  if (beg != end) return;
  valid_= true;
}

void TextRequest::parseStats(const char *beg, const char *end)
{
  while (*beg == SP)
  {
    ++beg;
  }

  if (beg == end)
  {
    valid_= true;
    return;
  }

  const char *keyEnd= cFind(beg, SP, end - beg);
  if (keyEnd == NULL)
  {
    keyEnd= end;
  }

  std::size_t valueLen= keyEnd - beg;

  if (valueLen == 5 && std::memcmp(beg, "local", 5) == 0)
  {
    stats_= stats_t::LOCAL;
    valid_= true;
    return;
  }
  else if (valueLen == 7 && std::memcmp(beg, "cluster", 7) == 0)
  {
    stats_= stats_t::CLUSTER;
    valid_= true;
    return;
  }
}

} //namespace alisql
