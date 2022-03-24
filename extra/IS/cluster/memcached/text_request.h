/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  text_request.h,v 1.0 08/27/2016 03:51:26 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file text_request.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 03:51:26 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_text_request_INC
#define  cluster_text_request_INC
#include <inttypes.h>
#include "consts_def.h"
#include <ctime>
#include "raft.h"
namespace alisql {

// Possible stats categories.
enum class stats_t {
    GENERAL, LOCAL, CLUSTER
};

/**
 * @class TextRequest
 *
 * @brief 
 *
 **/
class TextRequest
{

public:
  TextRequest();
  virtual ~TextRequest () {};
  void init(const char *p, uint32_t len);
  void parse();
  void parseStorage(const char *beg, const char *end, bool isCas, bool isTair);
  void parseGet(const char *beg, const char *end);
  void parseDelete(const char *beg, const char *end);
  void parseStats(const char *beg, const char *end);
  // Return the command type
  TextCommand getCommand() const {return command_;}

  // Return length of the request
  // If the request is incomplete, zero is returned
  uint32_t getLength() const {return requestLen_;}

  // Return `falgs` sent with command
  uint32_t getFlags() const {return flags_;}

  // Return `exptime` sent with command
  uint64_t getExptime() const {return exptime_;}
  
  // Return `casUnique` sent with command
  uint64_t getCasUnique() const {return casUnique_;}

  // True if the command has "noreply" option
  bool getNoreply() const {return noReply_;}

  // Return `namespace` sent with command
  uint64_t getNameSpace() const {return namespace_;}

  // Return `version` sent with command
  uint64_t getVersion() const {return version_;}

  // Return `key` sent with command
  item getKey() const {return key_;}
  
  // Return data block sent with command
  item getData() const {return data_;}

  // Return true if the request can be parsed correctly
  bool getValid() const {return valid_;}

  // Return stats item sent with command
  stats_t getStats() const {return stats_;}

private:
  const char* msgPos_;
  uint32_t msgLen_;
  uint32_t requestLen_;
  bool valid_;
  TextCommand command_;
  item key_;
  item data_;
  uint32_t flags_;
  uint64_t exptime_;
  bool noReply_;
  uint64_t casUnique_;
  stats_t stats_;
  uint64_t namespace_;
  uint64_t version_;
};/* end of class TextRequest */

} //namespace alisql
#endif     //#ifndef cluster_text_request_INC 
