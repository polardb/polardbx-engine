/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  easyNet.cc,v 1.0 07/31/2016 04:53:21 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file easyNet.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/31/2016 04:53:21 PM
 * @version 1.0
 * @brief
 *
 **/

#include "easyNet.h"
#include "paxos.h"         //TODO no driect include
#include "paxos.pb.h"
#include "paxos_server.h"  //TODO no driect include
#include "service.h"

namespace alisql {

easy_atomic_t easy_pool_alloc_byte = 0;

static void *easy_count_realloc(void *ptr, size_t size) {
  char *p1, *p = (char *)ptr;
  if (p) {
    p -= 8;
    easy_atomic_add(&easy_pool_alloc_byte, -(*((int *)p)));
  }
  if (size) {
    easy_atomic_add(&easy_pool_alloc_byte, size);
    p1 = (char *)easy_realloc(p, size + 8);
    if (p1) {
      *((int *)p1) = size;
      p1 += 8;
    }
    return p1;
  } else if (p)
    easy_free(p);
  return NULL;
}

EasyNet::EasyNet(uint64_t num, const uint64_t sessionTimeout,
                 bool memory_usage_count)
    : reciveCnt_(0), isShutdown_(false), sessionTimeout_(sessionTimeout) {
  if (unlikely(memory_usage_count)) {
    // set allocator for memory usage count
    easy_pool_set_allocator(easy_count_realloc);
  }
  eio_ = easy_eio_create(NULL, num);
  eio_->do_signal = 0;
}

int EasyNet::init(void *ptr) {
  memset(&clientHandler_, 0, sizeof(clientHandler_));
  clientHandler_.decode = EasyNet::paxosDecode;
  clientHandler_.encode = EasyNet::paxosEncode;
  clientHandler_.process = EasyNet::reciveProcess;
  clientHandler_.on_connect = EasyNet::onConnected;
  clientHandler_.on_disconnect = EasyNet::onDisconnected;
  clientHandler_.cleanup = EasyNet::onClientCleanup;
  clientHandler_.get_packet_id = EasyNet::getPacketId;
  clientHandler_.user_data = (void *)workPool_;
  clientHandler_.user_data2 = ptr;

  memset(&serverHandler_, 0, sizeof(serverHandler_));
  serverHandler_.decode = EasyNet::paxosDecode;
  serverHandler_.encode = EasyNet::paxosEncode;
  serverHandler_.process = EasyNet::reciveProcess;
  serverHandler_.user_data = (void *)workPool_;
  serverHandler_.user_data2 = ptr;

  return 0;
};

int EasyNet::start(int port) {
  if (eio_ == NULL) return -3;
  if (easy_connection_add_listen(eio_, NULL, port, &serverHandler_) == NULL)
    return -1;

  if (easy_eio_start(eio_)) return -2;

  return 0;
}

int EasyNet::shutdown() {
  if (isShutdown_) return 0;
  lock_.lock();
  isShutdown_ = true;
  lock_.unlock();
  /*
   * The follow function should not be called by hold the lock.
   * Because some callback function which try to hold lock, will be called.
   */
  easy_eio_shutdown(eio_);
  easy_eio_stop(eio_);
  easy_eio_wait(eio_);
  easy_eio_destroy(eio_);

  return 0;
}

int EasyNet::stop() {
  easy_eio_stop(eio_);

  return 0;
}

easy_addr_t EasyNet::createConnection(const std::string &addr,
                                      NetServerRef server, uint64_t timeout,
                                      uint64_t index) {
  easy_addr_t iaddr;

  if (isShutdown_) {
    iaddr.port = 0;
    return iaddr;
  }

  iaddr = easy_inet_str_to_addr(addr.c_str(), 0);
  iaddr.cidx = index;

  if (server != nullptr) setConnData(iaddr, server);

  if (easy_connection_connect(eio_, iaddr, &clientHandler_, timeout, NULL,
                              EASY_CONNECT_AUTOCONN) != EASY_OK)
    // if (easy_connection_connect(eio_, iaddr, &clientHandler_, timeout, NULL,
    // 0) != EASY_OK)
    iaddr.port = 0;

  /*
  easy_session_t *s= easy_connection_connect_init(NULL, &clientHandler_, 200,
  NULL, 0, NULL);

  if (iaddr.family == 0 || s == NULL)
    iaddr.port= 0;

  if (NULL == easy_client_send(eio_, iaddr, s))
    iaddr.port= 0;
  */

  return iaddr;
}

void EasyNet::disableConnection(easy_addr_t addr) {
  easy_connection_disconnect(eio_, addr);
}

int EasyNet::sendPacket(easy_addr_t addr, const char *buf, uint64_t len,
                        uint64_t id) {
  easy_session_t *s;
  NetPacket *np;

  if ((np = easy_session_packet_create(NetPacket, s, len)) == NULL) {
    return -1;
  }
  auto server = std::dynamic_pointer_cast<RemoteServer>(getConnData(addr));
  if (!server || !server->isLearner || !server->paxos ||
      server->paxos->getLocalServer()->learnerConnTimeout == 0)
    easy_session_set_timeout(s, sessionTimeout_);  // ms
  else
    easy_session_set_timeout(
        s, server->paxos->getLocalServer()->learnerConnTimeout * 4);

  np->data = &np->buffer[0];
  np->len = len;
  np->packetId = id;
  memcpy(np->data, buf, len);

  if (easy_client_dispatch(eio_, addr, s) == EASY_ERROR) {
    easy_session_destroy(s);
    return -2;
  }

  /* TODO: onFail callback function need to be added*/

  return 0;
}

int EasyNet::sendPacket(easy_addr_t addr, const std::string &buf, uint64_t id) {
  return sendPacket(addr, buf.c_str(), buf.length(), id);
}

int EasyNet::resendPacket(easy_addr_t addr, void *ptr, uint64_t id) {
  NetPacket *np = (NetPacket *)ptr;

  return sendPacket(addr, np->data, np->len, id);
}

int EasyNet::setRecvPacketCallback(void *handler) {
  setWorkPool((easy_thread_pool_t *)handler);
  return 0;
}

NetServerRef EasyNet::getConnData(easy_addr_t addr, bool locked) {
  std::unique_lock<std::mutex> lg(lock_, std::defer_lock);
  if (!locked) lg.lock();

  auto it = connStatus_.find(getAddrKey(addr));
  if (it != connStatus_.end())
    return it->second;
  else
    return nullptr;
}

NetServerRef EasyNet::getConnDataAndSetFail(easy_connection_t *c, bool isFail) {
  auto addr = c->addr;
  std::lock_guard<std::mutex> lg(lock_);
  auto s = getConnData(addr, true);
  auto server = std::dynamic_pointer_cast<RemoteServer>(s);
  if (server == nullptr) return s;
  if (isFail) {
    if (!server->netError.load()) {
      if (server->c == c || server->c == nullptr)
        easy_system_log("EasyNet::onDisconnected server %ld\n",
                       server->serverId);
      else
        easy_error_log(
            "EasyNet::onDisconnected server %ld, which is the old one\n",
            server->serverId);
    }
    /* disconnect */
    if (server->c != nullptr && server->c != c) return s;
    server->waitForReply = 0;
    server->netError.store(isFail);
  } else {
    if (server->netError.load())
      easy_system_log("EasyNet::onConnected server %ld\n", server->serverId);
    /* connect */
    server->c = c;
    server->waitForReply = 0;
    server->netError.store(isFail);
  }
  return s;
}

void EasyNet::setConnData(easy_addr_t addr, NetServerRef server) {
  std::lock_guard<std::mutex> lg(lock_);
  connStatus_.insert(std::make_pair(getAddrKey(addr), server));
}

void EasyNet::delConnDataById(uint64_t id) {
  /* Protect the connStatus_ map */
  std::lock_guard<std::mutex> lg(lock_);
  for (auto it = connStatus_.begin(); it != connStatus_.end();) {
    if (std::dynamic_pointer_cast<RemoteServer>(it->second)->serverId == id) {
      connStatus_.erase(it++);
    } else {
      ++it;
    }
    if (connStatus_.size() == 0) break;
  }
}

uint64_t EasyNet::getAddrKey(easy_addr_t addr) {
  uint64_t ret = addr.u.addr;
  ret <<= 32;
  ret |= addr.port;

  return ret;
}

void EasyNet::tryFreeMsg(NetPacket *np) {
  if (np->msg) {
    delete static_cast<PaxosMsg *>(np->msg);
    np->msg = nullptr;
  }
}

int EasyNet::reciveProcess(easy_request_t *r) {
  if (r == NULL) return EASY_ERROR;

  /* TODO means send callback process here? called by easy_session_process */
  if (r->ms->c == NULL) return EASY_ERROR;

  auto tp = (easy_thread_pool_t *)(r->ms->c->handler->user_data);
  auto srv = (Service *)(r->ms->c->handler->user_data2);
  if (srv == NULL) return EASY_ERROR;

  assert(r->user_data == NULL);
  r->user_data = (void *)srv;
  auto en = srv->getEasyNet();

  en->incRecived();

  if (tp == NULL || !srv->workPoolIsRunning()) return EASY_ERROR;

  int ret = EASY_AGAIN;

  if (r->ipacket == NULL) {
    /*
     * timeout case.
     * we push the session to thread pool.
     * Service::process will handle this case.
     */
    auto server = std::dynamic_pointer_cast<RemoteServer>(
        en->getConnData(r->ms->c->addr));
    /* May already be remove. */
    if (server != nullptr) {
      NetPacket *np = (NetPacket *)(r->opacket);
      easy_warn_log(
          "EasyNet::reciveProcess sendMsg timeout for serverid:%ld "
          "packet_id:0x%llx(%llu)\n",
          server->serverId, np->packetId, np->packetId);
    }
    // server->waitForReply= 0;
    /* Here we should not return EASY_ERROR. */
    // ret= EASY_AGAIN;
    ret = EASY_ERROR;
  }

  if (r->ms->c->type == EASY_TYPE_SERVER) {
    auto tp2 = srv->getHeartbeatPool();
    NetPacket *np = (NetPacket *)r->ipacket;
    assert(np);
    PaxosMsg *msg = new PaxosMsg;
    if (!msg->ParseFromArray(np->data, np->len)) {
      // receive a wrong msg.
      easy_error_log("A msg have %ld entries!! droped!!\n",
                     msg->entries_size());
      r->opacket = (void *)NULL;
      return EASY_OK;
    }
    assert(r->args == 0);
    r->args = (void *)msg;
    if (tp2 == NULL || msg->msgtype() != Consensus::AppendLog ||
        msg->entries_size() != 0 || Service::running < Service::workThreadCnt ||
        !((Paxos *)(srv->getConsensus()))->getOptimisticHeartbeat()) {
      easy_thread_pool_push(tp, r, easy_hash_key((uint64_t)(long)r));
    } else {
      // set msg type to OptimisticHeartbeat to let service know it is in
      // heartbeat pool
      msg->set_msgtype(Consensus::OptimisticHeartbeat);
      // msg is heartbeat and work pool is full, put in heartbeat pool
      easy_thread_pool_push(tp2, r, easy_hash_key((uint64_t)(long)r));
    }
  } else {
    easy_session_t *s = (easy_session_t *)r->ms;
    easy_thread_pool_push_session(tp, s, easy_hash_key((uint64_t)(long)r));
  }

  return ret;
}

void *EasyNet::paxosDecode(easy_message_t *m) {
  NetPacket *np;
  uint64_t len, datalen;

  if ((len = m->input->last - m->input->pos) < NetPacketHeaderSize) return NULL;

  datalen = *((uint64_t *)m->input->pos);

  if (datalen > 0x4000000) {
    easy_error_log("data_len is invalid: %llu\n", datalen);
    m->status = EASY_ERROR;
    return NULL;
  }

  len -= NetPacketHeaderSize;

  if (len < datalen) {
    m->next_read_len = datalen - len;
    easy_debug_log("Decode a net packet fail, data len expect:%llu got:%llu",
                   datalen, len);
    return NULL;
  }

  if ((np = (NetPacket *)easy_pool_calloc(m->pool, sizeof(NetPacket))) ==
      NULL) {
    m->status = EASY_ERROR;
    return NULL;
  }

  m->input->pos += NetPacketHeaderSize;
  np->type = NetPacketTypeNet;
  np->data = (char *)m->input->pos;
  np->len = datalen;
  if (m->c->type == EASY_TYPE_CLIENT) {
    auto msg = new PaxosMsg();
    msg->ParseFromArray(np->data, np->len);
    np->packetId = msg->msgid();
    np->msg = (void *)msg;
  }
  m->input->pos += datalen;

  easy_debug_log("Decode a net packet success, total lens:%llu",
                 datalen + NetPacketHeaderSize);

  return (void *)np;
}

int EasyNet::paxosEncode(easy_request_t *r, void *data) {
  easy_buf_t *b;
  NetPacket *np = (NetPacket *)data;

  if ((b = easy_buf_create(r->ms->pool, NetPacketHeaderSize + np->len)) == NULL)
    return EASY_ERROR;

  *((uint64_t *)b->last) = np->len;
  memcpy(b->last + NetPacketHeaderSize, np->data, np->len);

  b->last += (NetPacketHeaderSize + np->len);

  easy_request_addbuf(r, b);

  return EASY_OK;
}

int EasyNet::onConnected(easy_connection_t *c) {
  auto srv = (Service *)(c->handler->user_data2);
  if (srv == NULL) return 0;

  auto en = srv->getEasyNet();

  auto server = std::dynamic_pointer_cast<RemoteServer>(
      en->getConnDataAndSetFail(c, false));
  if (server != nullptr) {
    /* We don't start heartbeatTimer for learner, so we send heartbeat here. */
    server->onConnectCb();
  }
  return 0;
}

int EasyNet::onDisconnected(easy_connection_t *c) {
  auto srv = (Service *)(c->handler->user_data2);
  if (srv == NULL) return 0;

  auto en = srv->getEasyNet();

  auto server = std::dynamic_pointer_cast<RemoteServer>(
      en->getConnDataAndSetFail(c, true));

  if (server != nullptr) {
    // onDisconnectCB
  }
  return 0;
}

int EasyNet::onClientCleanup(easy_request_t *r, void *apacket) {
  NetPacket *np = nullptr;
  if (r == NULL) {
    np = static_cast<NetPacket *>(apacket);
    if (np && np->msg) {
      PaxosMsg *msg = static_cast<PaxosMsg *>(np->msg);
      easy_warn_log(
          "EasyNet::onClientCleanup: msgId(%llu) packet_id:%llu responce from "
          "server %llu which already be deleted for timeout.",
          msg->msgid(), np->packetId, msg->serverid());
      delete msg;
      np->msg = NULL;
    }
    return 0;
  }

  np = static_cast<NetPacket *>(r->ipacket);
  if (np && np->msg) {
    PaxosMsg *msg = static_cast<PaxosMsg *>(np->msg);
    easy_warn_log(
        "EasyNet::onClientCleanup: msgId(%llu) packet_id:%llu responce from "
        "server %llu.",
        msg->msgid(), np->packetId, msg->serverid());
    delete msg;
    np->msg = NULL;
  }
  return 0;
}

uint64_t EasyNet::getPacketId(easy_connection_t *c, void *data) {
  NetPacket *np = static_cast<NetPacket *>(data);
  return np->packetId;
}

}  // namespace alisql
