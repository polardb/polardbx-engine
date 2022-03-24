/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:   memcached_object.cc,v 1.0 08/31/2016 04:53:21 PM hangfeng.fj(hangfeng.fj(@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file memcached_object.cc
 * @author hangfeng.fj(hangfeng.fj(@alibaba-inc.com)
 * @date 08/31/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "memcached_object.h"
#include <stdlib.h>
#include <algorithm>
#include "memcached_utils.h"
#include "raft.h"

namespace alisql {

MemcachedObject::MemcachedObject()
 : op_('\0'), flags_(0), exptime_(0), sequenceNumber_(0)
 {
 }

MemcachedObject::MemcachedObject(uint8_t op, std::uint32_t flags,
                                 std::time_t exptime, const std::string &data)
    : op_(op), flags_(flags), exptime_(exptime), data_(data), sequenceNumber_(0)
{
}

void MemcachedObject::dumpObject(std::string *buf)
{
  uint32_t totalLen = sizeof(uint64_t) + sizeof(uint8_t) 
                    + sizeof(uint32_t) + sizeof(std::time_t)
                    + sizeof(uint64_t) + data_.size();

  buf->resize(totalLen);
  uint64_t valueSize= data_.size();
  char *p= reinterpret_cast<char*>(& ((*buf)[0]));
  memcpy(p, static_cast<const void*>(&sequenceNumber_), sizeof(uint64_t));
  p += sizeof(uint64_t);
  memcpy(p, static_cast<const void*>(&op_), sizeof(uint8_t));
  p += sizeof(uint8_t);
  memcpy(p, static_cast<const void*>(&flags_), sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(p, static_cast<const void*>(&exptime_), sizeof(std::time_t));
  p += sizeof(std::time_t);
  memcpy(p, static_cast<const void*>(&valueSize), sizeof(uint64_t));
  p += sizeof(uint64_t);
  memcpy(p, static_cast<const void*>(data_.data()), valueSize);
}

void MemcachedObject::loadObject(const std::string &buf)
{
  const char *p = buf.data();
  uint64_t valueSize= 0;
  uint8_t opcode= 0;
  
  memcpy(static_cast<void*>(&sequenceNumber_), p, sizeof(uint64_t));
  p += sizeof(uint64_t);
  memcpy(static_cast<void*>(&opcode), p, sizeof(uint8_t));
  op_= opcode;
  p += sizeof(uint8_t);
  memcpy(static_cast<void*>(&flags_), p, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(static_cast<void*>(&exptime_), p, sizeof(std::time_t));
  p += sizeof(std::time_t);
  memcpy(static_cast<void*>(&valueSize), p, sizeof(uint64_t));
  data_.resize(valueSize);
  p += sizeof(uint64_t);
  memcpy(static_cast<void*>(&data_[0]), p, valueSize);
}

void MemcachedObject::serializeSeqNum(std::string &value, 
                                      uint64_t seqNum)
{
  char *p= reinterpret_cast<char*>(& (value[0]));
  memcpy(p, static_cast<void*>(&seqNum), sizeof(uint64_t));
}

void MemcachedObject::deserializeSeqNum(std::string &value,
                                        uint64_t &seqNum)
{
  const char *p = value.data();
  memcpy(static_cast<void*>(&seqNum), p, sizeof(uint64_t));
}

} //namespace alisql
