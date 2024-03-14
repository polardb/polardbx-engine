/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  Service.h,v 1.0 07/15/2016 02:46:50 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file Service.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/15/2016 02:46:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef cluster_service_INC
#define cluster_service_INC

#include <google/protobuf/io/coded_stream.h>
#include "client_service.h"
#include "easyNet.h"
#include "ev.h"
#include "paxos.pb.h"
#include "thread_timer.h"

namespace alisql {

class Consensus;

bool MyParseFromArray(google::protobuf::MessageLite &msg, const void *data,
                      int size);

class ThreadHook {
  friend class ThreadHookTrigger;
 private:
  std::function<void()> start;
  std::function<void()> end;

 public:
  ThreadHook(std::function<void()> start, std::function<void()> end)
      : start(std::move(start)), end(std::move(end)) {}
};

class ThreadHookTrigger {
 private:
  const ThreadHook *hook;
 public:
  explicit ThreadHookTrigger(ThreadHook *hook): hook(hook) {
    if (hook != nullptr) {
      hook->start();
    }
  }
  ~ThreadHookTrigger() {
    if (hook != nullptr) {
      hook->end();
    }
  }
};

class easy_eio_start_wrapper;

/**
 * @class Service
 *
 * @brief interface class for Service
 *
 **/
class Service {
 public:
  explicit Service(Consensus *cons);
  virtual ~Service() = default;

  // todo delete confusing default value of this function parameters
  virtual int init(uint64_t ioThreadCnt = 4, uint64_t workThreadCnt = 4,
                   uint64_t ConnectTimeout = 300,
                   bool memory_usage_count = false,
                   uint64_t heartbeatThreadCnt = 0,
                   ThreadHook *threadHook = nullptr);
  virtual int start(int port);
  virtual void closeThreadPool();
  virtual int shutdown();
  virtual int stop();
  virtual void setSendPacketTimeout(uint64_t t);
  virtual int sendPacket(easy_addr_t addr, const std::string &buf,
                         uint64_t id = 0);
  virtual int resendPacket(easy_addr_t addr, void *ptr, uint64_t id = 0);
  virtual easy_addr_t createConnection(const std::string &addr,
                                       NetServerRef server, uint64_t timeout,
                                       uint64_t index = 0) {
    return net_->createConnection(addr, server, timeout, index);
  }
  virtual void disableConnnection(easy_addr_t addr) {
    net_->disableConnection(addr);
  }
  // --- async stuff---
  struct CallbackBase;
  typedef std::shared_ptr<CallbackBase> CallbackType;
  class ServiceEvent {
   public:
    Consensus *cons;
    ulong type;
    void *arg;
    void *arg1;
    CallbackType cb;
  }; /* end of class ServiceEvent */
  struct CallbackBase {
    virtual void run() = 0;
    virtual ~CallbackBase() = default;
  };
  template <typename Callable>
  struct Callback : public CallbackBase {
    Callable cb;
    Callback(Callable &&f) : cb(std::forward<Callable>(f)) {}
    virtual void run() { cb(); }
  };
  int pushAsyncEvent(CallbackType cb);
  template <typename Callable, typename... Args>
  int sendAsyncEvent(Callable &&f, Args &&...args) {
    CallbackType callBackPtr;
    callBackPtr = makeCallback(
        std::bind(std::forward<Callable>(f), std::forward<Args>(args)...));
    pushAsyncEvent(callBackPtr);
    return 0;
  }
  template <typename Callable>
  std::shared_ptr<Callback<Callable>> makeCallback(Callable &&f) {
    return std::make_shared<Callback<Callable>>(std::forward<Callable>(f));
  }
  // virtual int sendAsyncEvent(ulong type, void *arg= NULL, void *arg1= NULL);
  static int onAsyncEvent(Service::CallbackType cb);

  static int process(easy_request_t *r, void *args);

  std::shared_ptr<EasyNet> getEasyNet() { return net_; }
  Consensus *getConsensus() { return cons_; }
  void setConsensus(Consensus *cons) { cons_ = cons; }
  easy_thread_pool_t *getWorkPool() { return workPool_; }
  easy_thread_pool_t *getHeartbeatPool() { return heartbeatPool_; }
  ThreadTimerService *getThreadTimerService() { return tts_.get(); }
  bool workPoolIsRunning() { return !shutdown_stage_1.load(); }

  static std::atomic<uint64_t> running;
  static uint64_t workThreadCnt;
  ClientService *cs;

 protected:
  easy_io_t *pool_eio_;
  std::atomic<bool> shutdown_stage_1;
  easy_thread_pool_t *workPool_;
  easy_thread_pool_t *heartbeatPool_;
  std::shared_ptr<EasyNet> net_;
  std::shared_ptr<ThreadTimerService> tts_;
  Consensus *cons_;

  std::shared_ptr<easy_eio_start_wrapper> eio_start_wrapper_ = nullptr;
};      /* end of class Service */

}      /* end of namespace alisql */

#endif  // #ifndef cluster_service_INC
