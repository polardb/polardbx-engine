/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  memcached_object.h,v 1.0 08/27/2016 03:51:26 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file memcached_object
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 03:51:26 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_memcached_object_INC
#define  cluster_memcached_object_INC
#include <inttypes.h>
#include "consts_def.h"
#include <ctime>
namespace alisql {

/**
 * @class MemcachedObject
 *
 * @brief 
 *
 **/
class MemcachedObject
{
public:
  MemcachedObject();
  MemcachedObject(uint8_t op, std::uint32_t flags, 
                  std::time_t exptime, const std::string &data);
  virtual ~MemcachedObject() {};
  void dumpObject(std::string *buf);
  void loadObject(const std::string &buf);
  void static serializeSeqNum(std::string &value, uint64_t seqNum);
  void static deserializeSeqNum(std::string &value, uint64_t &seqNum);
  uint64_t getSeqNum() {return sequenceNumber_;}
  uint8_t getOP() {return op_;}
  std::uint32_t getFlags() {return flags_;}
  std::time_t getExptime() {return exptime_;}
  std::string getData() {return data_;}

private:
  // sequence number for each record stored in rocksdb, this is used
  // as cas unique nubmer
  uint64_t sequenceNumber_;
  uint8_t op_;
  std::uint32_t flags_;
  std::time_t exptime_;
  std::string data_;
};/* end of class MemcachedObject */

} //namespace alisql
#endif     //#ifndef cluster_memcached_object_INC 
