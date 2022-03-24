/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  easyNmemcached_easyNetet.cc,v 1.0 08/31/2016 04:53:21 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file memcached_easyNet.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/31/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "memcached_easyNet.h"
#include <string>
#include <iostream>
#include <endian.h>
#include <google/protobuf/text_format.h>
#include <string.h>
#include <gflags/gflags.h>

DECLARE_uint64(io_thread_num);

namespace alisql {

MemcachedEasyNet::MemcachedEasyNet()
  :EasyNet(8)
{

}

int MemcachedEasyNet::init(void *ptr)
{
  memset(&clientHandler_, 0, sizeof(clientHandler_));
  clientHandler_.decode= MemcachedEasyNet::MemcachedDecode;
  clientHandler_.encode= MemcachedEasyNet::MemcachedEncode;
  clientHandler_.process= MemcachedEasyNet::clientReciveProcess;
  clientHandler_.user_data2= ptr;
  
  memset(&serverHandler_, 0, sizeof(serverHandler_));
  serverHandler_.decode= MemcachedEasyNet::MemcachedDecode;
  serverHandler_.encode= MemcachedEasyNet::MemcachedEncode;
  serverHandler_.process= MemcachedEasyNet::reciveProcess;
  serverHandler_.user_data= (void *)workPool_;
  serverHandler_.user_data2= ptr;

  return 0;
};

int MemcachedEasyNet::reciveProcess(easy_request_t *r)
{
  easy_thread_pool_t *tp;
  EasyNet *en;

  if (r == NULL)
    return 0;

  tp= (easy_thread_pool_t *) (r->ms->c->handler->user_data);

  if (NULL == r->ipacket)
    return 0;
  
  if (tp == NULL)
    return EASY_ERROR;

  TextRequest *packet= reinterpret_cast<TextRequest*>(r->ipacket);
  
  // Return `ERROR` if the request is not valid
  if (!packet->getValid())
  {
    if (packet->getLength() != 0)
    {
      NetPacket *packet;
      uint64_t len = sizeof(TEXT_ERROR) - 1;
      uint64_t extraLen= sizeof(NetPacket);
      if ((packet= (NetPacket *) easy_pool_alloc(r->ms->pool, extraLen + len)) == NULL)
      {
        r->opacket= NULL;
        return EASY_OK;
      }
      packet->len= len;
      packet->data= &packet->buffer[0];
      memcpy(packet->data, TEXT_ERROR, len);
      r->opacket= (void *)packet;
      return EASY_OK;
    }
  }
  else {
    easy_thread_pool_push(tp, r, easy_hash_key((uint64_t)(long)r));
    return EASY_AGAIN;
  }
}


int MemcachedEasyNet::clientReciveProcess(easy_request_t * r)
{
  if (r == NULL)
    return 0;

  return EASY_OK;
}

void *MemcachedEasyNet::MemcachedDecode(easy_message_t *m)
{
  TextRequest *packet= NULL;

  int32_t len= 0;
  
  if ((packet= (TextRequest *)easy_pool_calloc(m->pool, sizeof(TextRequest))) == NULL)
    return NULL;
  
  char* head= (char *)m->input->pos;
  len= static_cast<int32_t>(m->input->last - m->input->pos);
  
  std::string message(head, len);

  packet->init(head, len);
  packet->parse();

  uint32_t c= packet->getLength();
  // the message is not complete for `set` command, don't update the
  // input pos
  if (c == 0)  
  {
    return NULL;
  }
  // consume
  m->input->pos += len;
  return (void *)packet;
}

int MemcachedEasyNet::MemcachedEncode(easy_request_t *r, void *data)
{
  easy_buf_t              *b;
  NetPacket *packet= (NetPacket *)data;

  int length= packet->len;
  
  if ((b = easy_buf_create(r->ms->pool, length)) == NULL)
    return EASY_ERROR;

  memcpy(b->pos, packet->data, packet->len);

  b->last += length;

  easy_request_addbuf(r, b);

  return EASY_OK;
}

int MemcachedEasyNet::startEio() 
{
  if (eio_ == NULL)
    return -3;

  if (easy_eio_start(eio_))
    return -2;

  return 0;
}
} //namespace alisql
