/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  service.cc,v 1.0 08/01/2016 10:59:50 AM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file service.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/01/2016 10:59:50 AM
 * @version 1.0
 * @brief implement of Service
 *
 **/

#include <stdlib.h>
#include <easy_inet.h>

#include "consensus.h"
#include "service.h"
#include "msg_compress.h"

namespace alisql {

Service::Service(Consensus *cons)
  :cs(NULL)
   ,cons_(cons)
{
}

std::atomic<uint64_t> Service::running(0);
uint64_t Service::workThreadCnt= 0;

int Service::init(uint64_t ioThreadCnt, uint64_t workThreadCntArg, uint64_t ConnectTimeout, bool memory_usage_count, uint64_t heartbeatThreadCnt)
{
  /* TODO here we should use factory. */

  net_= std::shared_ptr<EasyNet>(new EasyNet(ioThreadCnt, ConnectTimeout, memory_usage_count));

  pool_eio_ = easy_eio_create(NULL, 1);
  pool_eio_->do_signal = 0;
  workPool_= easy_thread_pool_create(pool_eio_, workThreadCntArg, Service::process, NULL);
  workThreadCnt= workThreadCntArg;

  if (heartbeatThreadCnt)
    heartbeatPool_= easy_thread_pool_create(pool_eio_, heartbeatThreadCnt, Service::process, NULL);
  else
    heartbeatPool_= NULL;

  tts_= std::make_shared<ThreadTimerService>();

  net_->setWorkPool(workPool_);
  net_->init(this);

  return 0;
}

int Service::start(int port)
{
  if (easy_eio_start(pool_eio_))
    return -4;
  shutdown_stage_1 = false;
  return net_->start(port);
}

void Service::closeThreadPool()
{
  easy_eio_shutdown(pool_eio_);
  easy_eio_stop(pool_eio_);
  easy_eio_wait(pool_eio_);
  easy_eio_destroy(pool_eio_);
  shutdown_stage_1 = true;
}

int Service::shutdown()
{
  if (!shutdown_stage_1)
    closeThreadPool();
  net_->shutdown();
  tts_.reset();
  return 0;
}

int Service::stop()
{
  easy_eio_stop(pool_eio_);
  net_->stop();
  tts_.reset();
  return 0;
}

void Service::setSendPacketTimeout(uint64_t t)
{
  /* will apply to all sendPacket */
  net_->setSessionTimeout(t);
}

int Service::sendPacket(easy_addr_t addr, const std::string& buf, uint64_t id)
{
  return net_->sendPacket(addr, buf, id);
}

int Service::resendPacket(easy_addr_t addr, void *ptr, uint64_t id)
{
  return net_->resendPacket(addr, ptr, id);
}

/*
int Service::sendAsyncEvent(ulong type, void *arg, void *arg1)
{
  easy_session_t *s;
  NetPacket *np;
  ServiceEvent *se;

  uint64_t len= sizeof(ServiceEvent);
  if ((np= easy_session_packet_create(NetPacket, s, len)) == NULL) {
    return -1;
  }

  np->type= NetPacketTypeAsync;
  np->data= &np->buffer[0];
  np->len= len;

  se= (ServiceEvent *)np->data;
  se->type= type;
  se->arg= arg;
  se->arg1= arg1;
  se->paxos= paxos_;

  // easy_thread_pool_push_session will call easy_hash_key if we pass NULL.
  easy_thread_pool_push_session(workPool_, s, 0);

  return 0;
}
*/

int Service::onAsyncEvent(Service::CallbackType cb)
{
  if (cb != nullptr)
    cb->run();
  return 0;
}


int Service::pushAsyncEvent(CallbackType cb)
{
  easy_session_t *s;
  NetPacket *np;
  ServiceEvent *se;

  uint64_t len= sizeof(ServiceEvent);
  if ((np= easy_session_packet_create(NetPacket, s, len)) == NULL) {
    return -1;
  }

  np->type= NetPacketTypeAsync;
  np->data= &np->buffer[0];
  np->len= len;

  se= (ServiceEvent *)np->data;
  memset(se, 0, sizeof(ServiceEvent));
  /*
  se->type= type;
  se->arg= arg;
  se->arg1= arg1;
  */
  se->cons= cons_;
  se->cb= cb;

  /* easy_thread_pool_push_session will call easy_hash_key if we pass NULL. */
  return easy_thread_pool_push_session(workPool_, s, 0);
}

int Service::process(easy_request_t *r, void *args)
{
  PaxosMsg *msg, omsg;
  NetPacket *np= NULL;
  Service *srv= NULL;
  Consensus *cons= NULL;

  np= (NetPacket *)r->ipacket;

  // updateRunning is false only when msg is heartbeat and it is in heartbeat pool
  bool updateRunning = true;
  if (np && r->ms->c->type != EASY_TYPE_CLIENT && r->args) {
    PaxosMsg *m = (PaxosMsg *)r->args;
    if (m->msgtype() == Consensus::OptimisticHeartbeat) {
      updateRunning = false;
      m->set_msgtype(Consensus::AppendLog);
    }
  }

  if (updateRunning && ++ Service::running >= Service::workThreadCnt)
    easy_warn_log("Almost out of workers total:%ld, running:%ld\n", Service::workThreadCnt, Service::running.load());

  /* Deal with send fail or Async Event */
  if (np == NULL)
  {
    np= (NetPacket *)(r->opacket);
    if (np->type == NetPacketTypeAsync)
    {
      auto se= (ServiceEvent *)np->data;
      CallbackType cb= se->cb;
      Service::onAsyncEvent(cb);
      cb.reset();
      se->cb.~shared_ptr();
    }
    else
    {
      /* Send fail case. */
      srv= (Service *) (r->user_data);
      cons= srv->getConsensus();
      if (cons->isShutdown())
        return EASY_ABORT;

      if (cons == NULL) //for unittest consensus.RemoteServerSendMsg
      {
        np= NULL;
        -- Service::running;
        return EASY_OK;
      }

      assert(np->type == NetPacketTypeNet);
      assert(r->ms->c->type == EASY_TYPE_CLIENT);
      PaxosMsg *tmpMsg= nullptr;
      if (np->msg)
      {
        msg= static_cast<PaxosMsg *>(np->msg);
      }
      else
      {
        msg= tmpMsg= new PaxosMsg();
        assert(np->len > 0);
        msg->ParseFromArray(np->data, np->len);
      }

      uint64_t newId= 0;
      if (r->ms->c->status == EASY_CONN_OK && 0 == cons->onAppendLogSendFail(msg, &newId))
      {
        easy_warn_log("Resend msg msgId(%llu) rename to msgId(%llu) to server %ld, term:%ld, startLogIndex:%ld, entries_size:%d, pli:%ld\n", msg->msgid(), newId, msg->serverid(), msg->term(), msg->entries_size() >= 1 ? msg->entries().begin()->index() : -1, msg->entries_size(), msg->prevlogindex());
        msg->set_msgid(newId);
        np->packetId= newId;
        msg->SerializeToArray(np->data, np->len);
        srv->resendPacket(r->ms->c->addr, np, newId);
      }

      if (tmpMsg)
        delete tmpMsg;

    }
    r->opacket= (void *)NULL;
    //TODO we return EASY_ABORT if there is nothing we need io thread to do.
    -- Service::running;
    return EASY_ABORT;
    //return EASY_OK;
  }

  srv= (Service *) (r->user_data);
  cons= srv->getConsensus();
  if (cons->isShutdown())
    return EASY_ABORT;

  /* For ClientService Callback */
  if (srv->cs)
  {
    if (srv->cs->serviceProcess(r, (void *)cons) == EASY_OK)
    {
      -- Service::running;
      return EASY_OK;
    }
  }

    /*
    */
  if (r->ms->c->type == EASY_TYPE_CLIENT) {
    msg= static_cast<PaxosMsg *>(np->msg);
  } else if (r->args) {
    msg = (PaxosMsg *)r->args;
  } else {
    msg= &omsg;
  }

  PaxosMsg rsp;
  rsp.set_clusterid(cons->getClusterId());

  if (cons->onMsgPreCheck(msg, &rsp))
  {
    uint64_t len= rsp.ByteSize();
    uint64_t extraLen= sizeof(NetPacket);
    if ((np= (NetPacket *) easy_pool_alloc(r->ms->pool, extraLen + len)) == NULL)
    {
      r->opacket= NULL;
      -- Service::running;
      return EASY_OK;
    }

    np->type= NetPacketTypeNet;
    np->len= len;
    np->data= &np->buffer[0];
    rsp.SerializeToArray(np->data, np->len);

    r->opacket= (void *)np;
    -- Service::running;
    return EASY_OK;
  }

  switch(msg->msgtype()){
    case Consensus::RequestVote:
      {
        cons->onRequestVote(msg, &rsp);

        uint64_t len= rsp.ByteSize();
        uint64_t extraLen= sizeof(NetPacket);
        if (cons->isShutdown() || ((np= (NetPacket *) easy_pool_alloc(r->ms->pool, extraLen + len)) == NULL))
        {
          r->opacket= NULL;
          -- Service::running;
          return EASY_OK;
        }

        np->type= NetPacketTypeNet;
        np->len= len;
        np->data= &np->buffer[0];
        rsp.SerializeToArray(np->data, np->len);
      }
      break;

    case Consensus::RequestVoteResponce:
      {
        cons->onRequestVoteResponce(msg);
        np= NULL;
      }
      break;

    case Consensus::AppendLog:
      {
        if (msgDecompress(*msg) == false) {
          easy_error_log("msg(%llu) from leader(%ld) decompression failed, potential data corruption!", msg->msgid(), msg->leaderid());
          r->opacket= NULL;
          if (updateRunning)
            -- Service::running;
          return EASY_OK;
        }
        cons->onAppendLog(msg, &rsp);

        uint64_t len= rsp.ByteSize();
        uint64_t extraLen= sizeof(NetPacket);
        if (cons->isShutdown() || ((np= (NetPacket *) easy_pool_alloc(r->ms->pool, extraLen + len)) == NULL))
        {
          r->opacket= NULL;
          if (updateRunning)
            -- Service::running;
          return EASY_OK;
        }

        np->type= NetPacketTypeNet;
        np->len= len;
        np->data= &np->buffer[0];
        rsp.SerializeToArray(np->data, np->len);
      }
      break;

    case Consensus::AppendLogResponce:
      {
        cons->onAppendLogResponce(msg);
        np= NULL;
      }
      break;

    case Consensus::LeaderCommand:
      {
        cons->onLeaderCommand(msg, &rsp);

        uint64_t len= rsp.ByteSize();
        uint64_t extraLen= sizeof(NetPacket);
        if (cons->isShutdown() || ((np= (NetPacket *) easy_pool_alloc(r->ms->pool, extraLen + len)) == NULL))
        {
          r->opacket= NULL;
          -- Service::running;
          return EASY_OK;
        }

        np->type= NetPacketTypeNet;
        np->len= len;
        np->data= &np->buffer[0];
        rsp.SerializeToArray(np->data, np->len);
      }
      break;

    case Consensus::LeaderCommandResponce:
      {
        cons->onLeaderCommandResponce(msg);
        np= NULL;
      }
      break;

    case Consensus::ClusterIdNotMatch:
    case Consensus::PreCheckFailedResponce:
      {
        cons->onMsgPreCheckFailed(msg);
        np= NULL;
      }
      break;
    default:
      np= NULL;
      break;
  }	//endswitch


  if (r->ipacket)
    EasyNet::tryFreeMsg(static_cast<NetPacket *>(r->ipacket));
  if (r->args) {
    delete (PaxosMsg *)r->args;
    r->args = 0;
  }
  r->opacket= (void *)np;
  if (updateRunning)
    -- Service::running;
  return EASY_OK;
}

bool MyParseFromArray(google::protobuf::Message &msg, const void* data, int size)
{
  google::protobuf::io::CodedInputStream decoder((uint8_t *)data, size);
  decoder.SetTotalBytesLimit(size, 64*1024*1024);
  return msg.ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
}

};/* end of namespace alisql */
