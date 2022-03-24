/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  memcached_utils.h,v 1.0 08/27/2016 03:51:26 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file memcached_utils.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 03:51:26 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_memcached_utils_INC
#define  cluster_memcached_utils_INC
#include <inttypes.h>
#include <stdlib.h>
#include <cstring> 

namespace alisql {

static const char* cFind(const char* p, char c, uint32_t len)
{
    return (const char*)std::memchr(p, c, len);
}

} //namespace alisql
#endif     //#ifndef cluster_memcached_utils_INC 
