/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:   text_response.cc,v 1.0 08/31/2016 04:53:21 PM hangfeng.fj(hangfeng.fj(@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file text_response.cc
 * @author hangfeng.fj(hangfeng.fj(@alibaba-inc.com)
 * @date 08/31/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "text_response.h"
#include <sstream>
#include <iomanip>

namespace alisql {

void TextResponse::setValueResult(const std::string &key, const std::string &buf, bool cas)
{
  loadObject(buf);

  result_.append(VALUE);
  result_.append(key);
  result_.append(" ").append(std::to_string(object_.getFlags()));
  result_.append(" ").append(std::to_string(object_.getData().size()));
  if (cas)
  {
    result_.append(" ").append(std::to_string(object_.getSeqNum()));
  }
  result_.append(CRLF);
  result_.append(object_.getData().data());
  result_.append(CRLF);
  result_.append(TEXT_END);
}

std::string TextResponse::getStateType(Raft::StateType role)
{
  std::string state;

  if (role == Raft::State::FOLLOWER)
  {
    state= "FOLLOWER";
  }
  else if (role == Raft::State::CANDIDATE)
  {
    state= "CANDIDATE";
  }
  else if (role == Raft::State::LEADER)
  {
    state= "LEADER";
  }
  else if (role == Raft::State::NOROLE)
  {
    state= "NOROLE";
  }
  else
  {
    state= "UNKNOWN";
  }

  return state;
}

void TextResponse::setClusterStatsResult(Raft::ClusterInfoType *cis, uint64_t size)
{
  std::ostringstream os;
  std::string state;

  os << std::setfill('0');

  for (uint64_t i= 0; i < size; ++i)
  {
    os << "STAT serverId " << cis[i].serverId << CRLF;
    os << "STAT ipPort " << cis[i].ipPort << CRLF;
    os << "STAT matchIndex " << cis[i].matchIndex << CRLF;
    os << "STAT nextIndex " << cis[i].nextIndex << CRLF;
    os << "STAT role " << getStateType(cis[i].role) << CRLF;
    os << "STAT hasVoted " << cis[i].hasVoted << CRLF;
  }

  os << TEXT_END;
  result_= os.str();
}

void TextResponse::setLocalStatsResult(Raft::MemberInfoType &mi, uint64_t lastAppliedIndex,
                                       uint64_t cmdGet)
{
  std::ostringstream os;
  std::string state;

  os << std::setfill('0');

  os << "STAT serverId " << mi.serverId << CRLF;
  os << "STAT currentTerm " << mi.currentTerm << CRLF;
  os << "STAT currentLeader " << mi.currentLeader << CRLF;
  os << "STAT commitIndex " << mi.commitIndex << CRLF;
  os << "STAT lastLogTerm " << mi.lastLogTerm << CRLF;
  os << "STAT lastLogIndex " << mi.lastLogIndex << CRLF;
  os << "STAT role " << getStateType(mi.role) << CRLF;
  os << "STAT votedFor " << mi.votedFor << CRLF;
  os << "STAT lastAppliedIndex " << lastAppliedIndex << CRLF;
  os << "STAT cmd_get " << cmdGet << CRLF;

  os << TEXT_END;
  result_= os.str();
}

void TextResponse::setVersionResult()
{
  result_.append(TEXT_VERSION);
  result_.append("1.0");
  result_.append(CRLF);
}

void TextResponse::loadObject(const std::string &buf)
{
  object_.loadObject(buf);
}

uint64_t TextResponse::getResponseLen()
{
  uint64_t length= result_.length();
  return length;
}

void TextResponse::serializeToArray(char *buffer)
{
  memcpy(buffer, result_.c_str(), result_.length());
}

} //namespace alisql
