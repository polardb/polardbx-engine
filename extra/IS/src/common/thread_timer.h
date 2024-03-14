/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  thread_timer.h,v 1.0 11/22/2016 03:05:36 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file thread_timer.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 11/22/2016 03:05:36 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef thread_timer_INC
#define thread_timer_INC

#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <string.h>
#include "ev.h"
// #include "easyNet.h"
// #include "service.h"

namespace alisql {

class Service;

/* ThreadTimerService based on ev_timer, for thread safe, dynamic create */
typedef void (*TimerCallback)(void *);

class ThreadTimerService {
 public:
  class LoopData {
   public:
    LoopData() : shutdown(false) {
      ev_async_init(&asyncWatcher, ThreadTimerService::loopAsync);
    }
    std::recursive_mutex lock;
    ev_async asyncWatcher;
    bool shutdown;
  };
  ThreadTimerService();
  virtual ~ThreadTimerService();

  /*
  void *timeoutData;
  TimerCallback timerCallback;
  double timeoutVal;
  std::atomic<double> lastResetTS;

  void init(std::shared_ptr<Service> srv, TimerCallback cb, void *ptr) {
    srv_= srv;timerCallback= cb;timeoutData= ptr;
  }
  int start(uint64_t timeout, bool needRandom= false);
  int setTimeout(uint64_t timeout, bool needRandom= false);
  int resetTimer(bool isStop= false);
  static void timerCallbackInternal(struct ev_loop *loop, ev_timer *w, int
  revents);
  */

  struct ev_loop *getEvLoop() { return loop_; }
  void ldlock() { ld_->lock.lock(); }
  void ldunlock() { ld_->lock.unlock(); }
  /* Should under ldlock */
  void wakeup() { ev_async_send(loop_, &ld_->asyncWatcher); }

  // void startService();
  // void stopService();
  static void mainService(EV_P);
  static void loopRelease(EV_P);
  static void loopAcquire(EV_P);
  static void loopAsync(EV_P, ev_async *w, int revents);

 protected:
  /*
  std::atomic<uint64_t> isTimerInited_;
  std::atomic<uint64_t> isStoped_;
  ev_timer timer_;
  std::shared_ptr<Service> srv_;
  //easy_baseth_t *thread_;
  */

  std::mutex lock_;
  struct ev_loop *loop_;
  LoopData *ld_;
  std::thread *thread_;
};

class ThreadTimer {
 public:
  typedef enum State {
    Repeatable = 1,
    Oneshot = 2,
    /* Stage Timer always repeatable, and have two stages. */
    Stage = 3,
  } TimerType;

  struct CallbackBase;
  typedef std::shared_ptr<CallbackBase> CallbackType;
  typedef std::weak_ptr<CallbackBase> CallbackWeakType;

  struct CallbackBase {
    virtual void run() = 0;
    virtual ~CallbackBase() {}
  };

  template <typename Callable>
  struct Callback : public CallbackBase {
    Callable cb;
    Callback(Callable &&f) : cb(std::forward<Callable>(f)) {}
    virtual void run() { cb(); }
  };

  template <typename Callable, typename... Args>
  void init(ThreadTimerService *tts, std::shared_ptr<Service> srv, double t,
            TimerType type, Callable &&f, Args &&...args) {
    service_ = tts;
    srv_ = srv;
    time_ = t;
    type_ = type;
    randWeight_ = 0;
    callBackPtr = makeCallback(
        std::bind(std::forward<Callable>(f), std::forward<Args>(args)...));
    if (type_ == Oneshot) {
      ev_timer_init(&timer_, timerCallbackInternal, t, 0.0);
      timer_.data = this;
      start();
    } else {
      ev_timer_init(&timer_, timerCallbackInternal, t, t);
      timer_.data = this;
    }
  }
  template <typename Callable, typename... Args>
  explicit ThreadTimer(ThreadTimerService *tts, double t, TimerType type,
                       Callable &&f, Args &&...args) {
    init(tts, nullptr, t, type, std::forward<Callable>(f),
         std::forward<Args>(args)...);
  }
  template <typename Callable, typename... Args>
  explicit ThreadTimer(ThreadTimerService *tts, const uint64_t t,
                       TimerType type, Callable &&f, Args &&...args) {
    double ft = (double)t;
    ft /= 1000;
    init(tts, nullptr, ft, type, std::forward<Callable>(f),
         std::forward<Args>(args)...);
  }
  template <typename Callable, typename... Args>
  explicit ThreadTimer(ThreadTimerService *tts, std::shared_ptr<Service> srv,
                       double t, TimerType type, Callable &&f, Args &&...args)
      : service_(tts), srv_(srv), time_(t), type_(type), randWeight_(0) {
    init(tts, srv, t, type, std::forward<Callable>(f),
         std::forward<Args>(args)...);
  }
  template <typename Callable, typename... Args>
  explicit ThreadTimer(ThreadTimerService *tts, std::shared_ptr<Service> srv,
                       const uint64_t t, TimerType type, Callable &&f,
                       Args &&...args)
      : service_(tts), srv_(srv), type_(type), randWeight_(0) {
    double ft = (double)t;
    ft /= 1000;
    init(tts, srv, ft, type, std::forward<Callable>(f),
         std::forward<Args>(args)...);
  }
  virtual ~ThreadTimer();
  void start();
  void restart(double t);
  void restart();
  void restart(uint64_t t, bool needRandom);
  void stop();
  TimerType getType() { return type_; }
  double getTime() { return time_; }
  double getStageExtraTime() { return stageExtraTime_; }
  void setStageExtraTime(double baseTime);
  void setRandWeight(uint64_t w) { randWeight_ = w % 10; }
  uint64_t getCurrentStage() { return currentStage_.load(); }
  uint64_t getAndSetStage();
  std::shared_ptr<Service> &getService() { return srv_; }
  /* we use callBackPtr as arg to avoid CallbackType destruct.  */
  static void callbackRunWeak(CallbackWeakType callBackPtr);
  static void callbackRun(CallbackType callBackPtr);
  // static void callbackRun(CallbackType callBackPtr) {assert(callBackPtr !=
  // nullptr);callBackPtr->run();}

  static void timerCallbackInternal(struct ev_loop *loop, ev_timer *w,
                                    int revents);
  CallbackType callBackPtr;

  template <typename Callable>
  std::shared_ptr<Callback<Callable>> makeCallback(Callable &&f) {
    // Create and allocate full data structure, not base.
    return std::make_shared<Callback<Callable>>(std::forward<Callable>(f));
  }

 protected:
  ThreadTimerService *service_;
  std::shared_ptr<Service> srv_;
  double time_;
  TimerType type_;
  ev_timer timer_;
  double stageExtraTime_;
  std::atomic<uint64_t> currentStage_;
  uint64_t randWeight_;
};

}      /* end of namespace alisql */

#endif  // #ifndef thread_timer_INC
