/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  service.cc,v 1.0 08/01/2016 10:59:50 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
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

#include <easy_inet.h>
#include <cstdlib>

#include <memory>

#include "consensus.h"
#include "easy_io_struct.h"
#include "msg_compress.h"
#include "service.h"

namespace alisql {

template <typename T>
void for_each(easy_list_t &list_head, std::function<void(T &)> f) {
  for (easy_list_t *p = list_head.next; p != &list_head; p = p->next) {
    f(*reinterpret_cast<T *>((char *)p - offsetof(T, list_node)));
  }
}

void for_each(easy_thread_pool_t &pool,
              const std::function<void(easy_baseth_t &)> &f) {
  for (auto *th = reinterpret_cast<easy_baseth_t *>(&pool.data[0]);
       (char *)th < pool.last;
       th = reinterpret_cast<easy_baseth_t *>((char *)th + pool.member_size)) {
    f(*th);
  }
}

class easy_eio_start_wrapper {
 private:
  ThreadHook *hook;

 public:
  explicit easy_eio_start_wrapper(ThreadHook *hook) : hook(hook) {}

  int operator()(easy_io_t *eio) {
    if (eio == nullptr || eio->pool == nullptr) {
      return EASY_ERROR;
    }

    if (eio->started) {
      return EASY_ABORT;
    }
    for_each<easy_thread_pool_t>(
        eio->thread_pool_list, [this](easy_thread_pool_t &tp) {
          for_each(
              tp, [this](easy_baseth_t &th) { th.user_data = (void *)(hook); });
        });
    return easy_eio_start(eio);
  }
};

void *easy_baseth_on_start_wrapper(void *args) {
  auto th = (easy_baseth_t *)args;
  auto hook = (ThreadHook *)th->user_data;
  ThreadHookTrigger trigger(hook);
  return easy_baseth_on_start(args);
}

Service::Service(Consensus *cons) : cs(nullptr), cons_(cons) {}

std::atomic<uint64_t> Service::running(0);
uint64_t Service::workThreadCnt = 0;

int Service::init(uint64_t ioThreadCnt, uint64_t workThreadCntArg,
                  uint64_t ConnectTimeout, bool memory_usage_count,
                  uint64_t heartbeatThreadCnt, ThreadHook *threadHook) {
  /* TODO here we should use factory. */

  net_ = std::make_shared<EasyNet>(ioThreadCnt, ConnectTimeout,
                                   memory_usage_count);

  pool_eio_ = easy_eio_create(nullptr, 1);
  pool_eio_->do_signal = 0;

  eio_start_wrapper_ = std::make_shared<easy_eio_start_wrapper>(threadHook);
  workPool_ = easy_thread_pool_create_ex(pool_eio_, workThreadCntArg,
                                         easy_baseth_on_start_wrapper,
                                         Service::process, nullptr);

  workThreadCnt = workThreadCntArg;

  if (heartbeatThreadCnt) {
    heartbeatPool_ = easy_thread_pool_create_ex(pool_eio_, heartbeatThreadCnt,
                                                easy_baseth_on_start_wrapper,
                                                Service::process, nullptr);
  } else {
    heartbeatPool_ = nullptr;
  }

  tts_ = std::make_shared<ThreadTimerService>();

  net_->setWorkPool(workPool_);
  net_->init(this);

  return 0;
}

int Service::start(int port) {
  assert(eio_start_wrapper_ != nullptr);
  if ((*eio_start_wrapper_)(pool_eio_)) return -4;
  shutdown_stage_1 = false;
  return net_->start(port);
}

void Service::closeThreadPool() {
  if (shutdown_stage_1) return;
  easy_eio_shutdown(pool_eio_);
  easy_eio_stop(pool_eio_);
  easy_eio_wait(pool_eio_);
  easy_eio_destroy(pool_eio_);
  shutdown_stage_1 = true;
}

int Service::shutdown() {
  if (!shutdown_stage_1) closeThreadPool();
  net_->shutdown();
  tts_.reset();
  return 0;
}

int Service::stop() {
  easy_eio_stop(pool_eio_);
  net_->stop();
  tts_.reset();
  return 0;
}

void Service::setSendPacketTimeout(uint64_t t) {
  /* will apply to all sendPacket */
  net_->setSessionTimeout(t);
}

int Service::sendPacket(easy_addr_t addr, const std::string &buf, uint64_t id) {
  return net_->sendPacket(addr, buf, id);
}

int Service::resendPacket(easy_addr_t addr, void *ptr, uint64_t id) {
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

int Service::onAsyncEvent(Service::CallbackType cb) {
  if (cb != nullptr) cb->run();
  return 0;
}

int Service::pushAsyncEvent(CallbackType cb) {
  easy_session_t *s;
  NetPacket *np;
  ServiceEvent *se;

  uint64_t len = sizeof(ServiceEvent);
  if ((np = easy_session_packet_create(NetPacket, s, len)) == nullptr) {
    return -1;
  }

  np->type = NetPacketTypeAsync;
  np->data = &np->buffer[0];
  np->len = len;

  se = (ServiceEvent *)np->data;
  memset(se, 0, sizeof(ServiceEvent));
  /*
  se->type= type;
  se->arg= arg;
  se->arg1= arg1;
  */
  se->cons = cons_;
  se->cb = cb;

  /* easy_thread_pool_push_session will call easy_hash_key if we pass NULL. */
  return easy_thread_pool_push_session(workPool_, s, 0);
}

int Service::process(easy_request_t *r, void *args) {
  PaxosMsg *msg, omsg;
  NetPacket *np = nullptr;
  Service *srv = nullptr;
  Consensus *cons = nullptr;

  np = (NetPacket *)r->ipacket;

  // updateRunning is false only when msg is heartbeat and it is in heartbeat
  // pool
  bool updateRunning = true;
  if (np && r->ms->c->type != EASY_TYPE_CLIENT && r->args) {
    PaxosMsg *m = (PaxosMsg *)r->args;
    if (m->msgtype() == Consensus::OptimisticHeartbeat) {
      updateRunning = false;
      m->set_msgtype(Consensus::AppendLog);
    }
  }

  if (updateRunning && ++Service::running >= Service::workThreadCnt)
    easy_warn_log("Almost out of workers total:%ld, running:%ld\n",
                  Service::workThreadCnt, Service::running.load());

  /* Deal with send fail or Async Event */
  if (np == NULL) {
    np = (NetPacket *)(r->opacket);
    if (np->type == NetPacketTypeAsync) {
      auto se = (ServiceEvent *)np->data;
      CallbackType cb = se->cb;
      Service::onAsyncEvent(cb);
      cb.reset();
      se->cb.~shared_ptr();
    } else {
      /* Send fail case. */
      srv = (Service *)(r->user_data);
      cons = srv->getConsensus();
      if (cons->isShutdown()) return EASY_ABORT;

      if (cons == nullptr)  // for unittest consensus.RemoteServerSendMsg
      {
        np = NULL;
        --Service::running;
        return EASY_OK;
      }

      assert(np->type == NetPacketTypeNet);
      if (r->ms->c->status == EASY_CONN_OK) {
        assert(r->ms->c->type == EASY_TYPE_CLIENT);
        PaxosMsg *tmpMsg = nullptr;
        if (np->msg) {
          msg = static_cast<PaxosMsg *>(np->msg);
        } else {
          msg = tmpMsg = new PaxosMsg();
          assert(np->len > 0);
          msg->ParseFromArray(np->data, np->len);
        }

        uint64_t newId = 0;
        if (0 == cons->onAppendLogSendFail(msg, &newId)) {
          easy_warn_log(
              "Resend msg msgId(%llu) rename to msgId(%llu) to server %ld, "
              "term:%ld, startLogIndex:%ld, entries_size:%d, pli:%ld\n",
              msg->msgid(), newId, msg->serverid(), msg->term(),
              msg->entries_size() >= 1 ? msg->entries().begin()->index() : -1,
              msg->entries_size(), msg->prevlogindex());
          msg->set_msgid(newId);
          np->packetId = newId;
          msg->SerializeToArray(np->data, np->len);
          srv->resendPacket(r->ms->c->addr, np, newId);
        }
        if (tmpMsg) delete tmpMsg;
      }

    }
    r->opacket = (void *)nullptr;
    // TODO we return EASY_ABORT if there is nothing we need io thread to do.
    --Service::running;
    return EASY_ABORT;
    // return EASY_OK;
  }

  srv = (Service *)(r->user_data);
  cons = srv->getConsensus();
  if (cons->isShutdown()) return EASY_ABORT;

  /* For ClientService Callback */
  if (srv->cs) {
    if (srv->cs->serviceProcess(r, (void *)cons) == EASY_OK) {
      --Service::running;
      return EASY_OK;
    }
  }

  /*
   */
  if (r->ms->c->type == EASY_TYPE_CLIENT) {
    msg = static_cast<PaxosMsg *>(np->msg);
  } else if (r->args) {
    msg = (PaxosMsg *)r->args;
  } else {
    msg = &omsg;
  }

  PaxosMsg rsp;
  rsp.set_clusterid(cons->getClusterId());

  if (cons->onMsgPreCheck(msg, &rsp)) {
    uint64_t len = rsp.ByteSize();
    uint64_t extraLen = sizeof(NetPacket);
    if ((np = (NetPacket *)easy_pool_alloc(r->ms->pool, extraLen + len)) ==
        NULL) {
      r->opacket = nullptr;
      --Service::running;
      return EASY_OK;
    }

    np->type = NetPacketTypeNet;
    np->len = len;
    np->data = &np->buffer[0];
    rsp.SerializeToArray(np->data, np->len);

    r->opacket = (void *)np;
    --Service::running;
    return EASY_OK;
  }

  switch (msg->msgtype()) {
    case Consensus::RequestVote: {
      cons->onRequestVote(msg, &rsp);

      uint64_t len = rsp.ByteSize();
      uint64_t extraLen = sizeof(NetPacket);
      if (cons->isShutdown() || ((np = (NetPacket *)easy_pool_alloc(
                                      r->ms->pool, extraLen + len)) == NULL)) {
        r->opacket = nullptr;
        --Service::running;
        return EASY_OK;
      }

      np->type = NetPacketTypeNet;
      np->len = len;
      np->data = &np->buffer[0];
      rsp.SerializeToArray(np->data, np->len);
    } break;

    case Consensus::RequestVoteResponce: {
      cons->onRequestVoteResponce(msg);
      np = nullptr;
    } break;

    case Consensus::AppendLog: {
      if (msgDecompress(*msg) == false) {
        easy_error_log(
            "msg(%llu) from leader(%ld) decompression failed, potential data "
            "corruption!",
            msg->msgid(), msg->leaderid());
        r->opacket = nullptr;
        if (updateRunning) --Service::running;
        return EASY_OK;
      }
      cons->onAppendLog(msg, &rsp);

      uint64_t len = rsp.ByteSize();
      uint64_t extraLen = sizeof(NetPacket);
      if (cons->isShutdown() || ((np = (NetPacket *)easy_pool_alloc(
                                      r->ms->pool, extraLen + len)) == NULL)) {
        r->opacket = nullptr;
        if (updateRunning) --Service::running;
        return EASY_OK;
      }

      np->type = NetPacketTypeNet;
      np->len = len;
      np->data = &np->buffer[0];
      rsp.SerializeToArray(np->data, np->len);
    } break;

    case Consensus::AppendLogResponce: {
      cons->onAppendLogResponce(msg);
      np = nullptr;
    } break;

    case Consensus::LeaderCommand: {
      cons->onLeaderCommand(msg, &rsp);

      uint64_t len = rsp.ByteSize();
      uint64_t extraLen = sizeof(NetPacket);
      if (cons->isShutdown() || ((np = (NetPacket *)easy_pool_alloc(
                                      r->ms->pool, extraLen + len)) == NULL)) {
        r->opacket = nullptr;
        --Service::running;
        return EASY_OK;
      }

      np->type = NetPacketTypeNet;
      np->len = len;
      np->data = &np->buffer[0];
      rsp.SerializeToArray(np->data, np->len);
    } break;

    case Consensus::LeaderCommandResponce: {
      cons->onLeaderCommandResponce(msg);
      np = nullptr;
    } break;

    case Consensus::ClusterIdNotMatch:
    case Consensus::PreCheckFailedResponce: {
      cons->onMsgPreCheckFailed(msg);
      np = nullptr;
    } break;
    default:
      np = nullptr;
      break;
  }  // endswitch

  if (r->ipacket) EasyNet::tryFreeMsg(static_cast<NetPacket *>(r->ipacket));
  if (r->args) {
    delete (PaxosMsg *)r->args;
    r->args = 0;
  }
  r->opacket = (void *)np;
  if (updateRunning) --Service::running;
  return EASY_OK;
}

bool MyParseFromArray(google::protobuf::MessageLite &msg, const void *data,
                      int size) {
  google::protobuf::io::CodedInputStream decoder((uint8_t *)data, size);
  decoder.SetTotalBytesLimit(size);
  return msg.ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
}

}; /* end of namespace alisql */
