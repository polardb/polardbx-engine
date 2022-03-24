/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  memcached_easyNet.h,v 1.0 08/27/2016 03:51:26 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file easyNet.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 03:51:26 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_memcached_easyNet_INC
#define  cluster_memcached_easyNet_INC

#include <memory>
#include <easy_io.h>
#include "net.h"
#include "easyNet.h"
#include "text_request.h"

namespace alisql {

typedef struct MemcachedPacket {
    bool success;
    TextRequest textRequest;
} MemcachedPacket;

/**
 * @class MemcachedEasyNet
 *
 * @brief 
 *
 **/
class MemcachedEasyNet : public EasyNet {
  public:
    MemcachedEasyNet ();
    virtual ~MemcachedEasyNet () {};

    virtual int init(void *ptr);
    virtual int startEio();
    static void *MemcachedDecode(easy_message_t *m);
    static int MemcachedEncode(easy_request_t *r, void *data);
    static int clientReciveProcess(easy_request_t * r);
    static int reciveProcess(easy_request_t *r);
    int waitEio() {return easy_eio_wait(eio_);}

  private:
    MemcachedEasyNet ( const MemcachedEasyNet &other );   // copy constructor
    const MemcachedEasyNet& operator = ( const MemcachedEasyNet &other ); // assignment operator

};/* end of class MemcachedEasyNet */

} //namespace alisql
#endif     //#ifndef cluster_memcached_easyNet_INC 
