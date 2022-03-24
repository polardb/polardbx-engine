/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  text_response.h,v 1.0 08/27/2016 03:51:26 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file text_response.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 03:51:26 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef  cluster_text_response_INC
#define  cluster_text_response_INC
#include <inttypes.h>
#include "consts_def.h"
#include <string>
#include <vector>
#include "memcached_object.h"
#include "../service/state_machine_service.h"
#include "raft.h"
namespace alisql {

/**
 * @class TextResponse
 *
 * @brief
 *
 **/
struct ResponseData
{
  const char *data;
  std::size_t len;
};

class TextResponse
{
public:
  TextResponse() {};
  virtual ~TextResponse() {};
  void setValueResult(const std::string &key, const std::string &buf, bool cas);
  void setClusterStatsResult(Raft::ClusterInfoType *cis, uint64_t size);
  void setLocalStatsResult(Raft::MemberInfoType &mi, uint64_t lastAppliedIndex, uint64_t cmdGet);
  void setVersionResult();
  std::string getResult() {return result_;}
  void setResult(const char *result) {result_= result;}
  uint64_t getResponseLen();
  void serializeToArray(char *buffer);
  std::string getStateType(Raft::StateType role);

private:
  void loadObject(const std::string &buf);
  std::string result_;
  MemcachedObject object_;
};/* end of class TextResponse */

} //namespace alisql
#endif     //#ifndef cluster_text_response_INC
