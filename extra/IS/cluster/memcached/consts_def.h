/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consts_def.h,v 1.0 08/27/2016 03:51:26 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consts_def.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 03:51:26 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_consts_def_INC
#define  cluster_consts_def_INC
#include <tuple>
#include <stdlib.h>
#include <limits>

namespace alisql {

const char CRLF[]= "\x0d\x0a";
// Enter key \r
const char CR= '\x0d';
// New line \n
const char LF= '\x0a';
// Space
const char SP= '\x20';
const char VALUE[]= "VALUE ";
const char TEXT_ERROR[] = "ERROR\x0d\x0a";
const char TEXT_OK[] = "OK\x0d\x0a";
const char TEXT_STORED[] = "STORED\x0d\x0a";
const char TEXT_NOT_STORED[] = "NOT_STORED\x0d\x0a";
const char TEXT_EXISTS[] = "EXISTS\x0d\x0a";
const char TEXT_NOT_FOUND[] = "NOT_FOUND\x0d\x0a";
const char TEXT_TOUCHED[] = "TOUCHED\x0d\x0a";
const char TEXT_DELETED[] = "DELETED\x0d\x0a";
const char TEXT_END[] = "END\x0d\x0a";
const char TEXT_LOCKED[] = "LOCKED\x0d\x0a";
const char TEXT_VERSION[] = "VERSION ";

// Memcache text commands.
enum class TextCommand
{
    UNKNOWN, SET, ADD, REPLACE, APPEND, PREPEND, CAS, GET, GETS, DELETE,
    INCR, DECR, TOUCH, LOCK, UNLOCK, UNLOCK_ALL, SLABS, STATS, FLUSH_ALL,
    VERSION, VERBOSITY, QUIT, KEYS,
    END_OF_COMMAND, TAIR_SET // must be defined the last
};

using item= std::tuple<const char*, std::size_t>;

template<typename UInt>
UInt toUint(const char *p, bool &result) 
{
  result= false;
  char* end;
  unsigned long long i= strtoull(p, &end, 10);
  if (i == 0 && p == end )
  {
    return 0;
  }
  if (i == std::numeric_limits<unsigned long long>::max() &&
      errno == ERANGE)
  {
    return 0;
  }
  char c= *end;
  if (c != CR && c != LF && c != SP)
  {
    return 0;
  }
  if (i > std::numeric_limits<UInt>::max())
  {
    return 0;
  } 
  result= true;
  return static_cast<UInt>(i);
}

} //namespace alisql
#endif     //#ifndef cluster_consts_def_INC 
