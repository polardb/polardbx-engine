/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  thread_timer.cc,v 1.0 11/22/2016 03:08:37 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file thread_timer.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 11/22/2016 03:08:37 PM
 * @version 1.0
 * @brief
 *
 **/

#include "thread_timer.h"
#include "paxos.h"

namespace alisql {

ThreadTimerService::ThreadTimerService()
    // isTimerInited_(0)
    //,isStoped_(1)
    : loop_(NULL) {
  ld_ = new LoopData();
  loop_ = ev_loop_new(0);
  ld_->lock.lock();
  ev_async_start(loop_, &ld_->asyncWatcher);
  ev_set_userdata(loop_, ld_);
  /* ev_set_invoke_pending_cb */
  ev_set_loop_release_cb(loop_, ThreadTimerService::loopRelease,
                         ThreadTimerService::loopAcquire);
  ld_->lock.unlock();
  thread_ = new std::thread(ThreadTimerService::mainService, loop_);
}

/* TODO shutdown  */
ThreadTimerService::~ThreadTimerService() {
  ld_->lock.lock();
  ld_->shutdown = true;
  wakeup();
  ld_->lock.unlock();
  thread_->join();
  delete thread_;
  ld_->lock.lock();
  ev_loop_destroy(loop_);
  ld_->lock.unlock();
  loop_ = NULL;
  thread_ = NULL;
  delete ld_;
  ld_ = NULL;
}

/*
void ThreadTimerService::startService()
{
  ;
}

void ThreadTimerService::stopService()
{
  ;
}
*/

void ThreadTimerService::mainService(EV_P) {
  LoopData *ld = (LoopData *)ev_userdata(EV_A);
  ld->lock.lock();
  ev_run(EV_A, 0);
  ld->lock.unlock();
}

void ThreadTimerService::loopRelease(EV_P) {
  LoopData *ld = (LoopData *)ev_userdata(EV_A);
  ld->lock.unlock();
}

void ThreadTimerService::loopAcquire(EV_P) {
  LoopData *ld = (LoopData *)ev_userdata(EV_A);
  ld->lock.lock();
}

void ThreadTimerService::loopAsync(EV_P, ev_async *w, int revents) {
  LoopData *ld = (LoopData *)ev_userdata(EV_A);
  if (ld->shutdown) ev_break(EV_A, EVBREAK_ALL);
}

ThreadTimer::~ThreadTimer() { stop(); }

void ThreadTimer::start() {
  if (type_ == Stage) setStageExtraTime(time_);

  service_->ldlock();
  ev_now_update(service_->getEvLoop());
  ev_timer_start(service_->getEvLoop(), &timer_);
  service_->wakeup();
  service_->ldunlock();
  auto loop = service_->getEvLoop();
  auto w = &timer_;
  // easy_info_log("ThreadTimer::start ev_now:%lf mn_now:%lf at:%lf,
  // repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
}

void ThreadTimer::restart(double t) {
  assert(type_ == Repeatable || type_ == Stage);
  auto loop = service_->getEvLoop();
  auto w = &timer_;
  // easy_info_log("ThreadTimer::restarts ev_now:%lf mn_now:%lf at:%lf,
  // repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);

  if (t != 0.0) time_ = t;
  if (type_ == Stage) setStageExtraTime(time_);

  if (t == 0.0) {
    service_->ldlock();
    /* We should update the mn_now&ev_rt_now because the loop may suspend a long
     * time. */
    ev_now_update(service_->getEvLoop());
    if (type_ == Stage) ev_timer_set(&timer_, time_, time_);
    ev_timer_again(service_->getEvLoop(), &timer_);
    service_->wakeup();
    service_->ldunlock();
  } else {
    service_->ldlock();
    /* We should update the mn_now&ev_rt_now because the loop may suspend a long
     * time. */
    ev_now_update(service_->getEvLoop());
    ev_timer_set(&timer_, t, t);
    ev_timer_again(service_->getEvLoop(), &timer_);
    service_->wakeup();
    service_->ldunlock();
  }
  // easy_info_log("ThreadTimer::restarte ev_now:%lf mn_now:%lf at:%lf,
  // repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
}

void ThreadTimer::restart() { restart(0.0); }

void ThreadTimer::restart(uint64_t t, bool needRandom) {
  double f = (double)t;
  f /= 1000;

  restart(f);
}

void ThreadTimer::stop() {
  if (ev_is_active(&timer_)) {
    service_->ldlock();
    ev_timer_stop(service_->getEvLoop(), &timer_);
    service_->wakeup();
    service_->ldunlock();

    auto loop = service_->getEvLoop();
    auto w = &timer_;
    easy_info_log(
        "ThreadTimer::stop ev_now:%lf mn_now:%lf at:%lf, repeat:%lf\n",
        ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
  }
}

void ThreadTimer::setStageExtraTime(double baseTime) {
  assert(type_ == Stage);
  uint64_t base = 10000, randn = rand() % base;
  if (randWeight_ != 0) {
    /* if weigth == 9 we use lest time here. */
    randn = randn / 10 + 1000 * (9 - randWeight_);
  }
  double tmp = baseTime * randn / base;
  stageExtraTime_ = (tmp < 0.001 ? 0.001 : tmp);
  currentStage_.store(0);
}

uint64_t ThreadTimer::getAndSetStage() {
  if (currentStage_.load() == 0) {
    currentStage_.store(1);
    return 0;
  } else {
    currentStage_.store(0);
    return 1;
  }
}

void ThreadTimer::callbackRunWeak(CallbackWeakType callBackPtr) {
  // if (callBackPtr != nullptr)
  //   callBackPtr->run();
  if (auto spt = callBackPtr.lock())
    spt->run();
  else
    easy_error_log(
        "ThreadTimer::callbackRun : the callBackPtr already be deteled, stop "
        "this async call.");
}

void ThreadTimer::callbackRun(CallbackType callBackPtr) {
  if (callBackPtr != nullptr) callBackPtr->run();
}

void ThreadTimer::timerCallbackInternal(struct ev_loop *loop, ev_timer *w,
                                        int revents) {
  // easy_info_log("timerCallbackInternal ev_now:%lf mn_now:%lf at:%lf,
  // repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
  ThreadTimer *tt = (ThreadTimer *)(w->data);
  if (tt->getType() == ThreadTimer::Stage) {
    double t;
    if (tt->getAndSetStage() == 0) {
      t = tt->getStageExtraTime();
      easy_warn_log("ThreadTimer change stage from 0 to 1, extraTime:%lf", t);
    } else {
      t = tt->getTime();
      if (tt->getService() != nullptr) {
        assert(tt->callBackPtr != nullptr);
        // tt->getService()->sendAsyncEvent(ThreadTimer::callbackRun,
        // tt->callBackPtr);
        tt->getService()->sendAsyncEvent(
            ThreadTimer::callbackRunWeak,
            std::move(CallbackWeakType(tt->callBackPtr)));
      } else
        tt->callBackPtr->run();
    }
    ev_timer_set(w, t, t);
    ev_timer_again(loop, w);
    // easy_info_log("timerCallbackInternal Stage ev_now:%lf mn_now:%lf at:%lf,
    // repeat:%lf\n", ev_now(loop), ev_mn_now(loop), w->at, w->repeat);
  } else {
    if (tt->getService() !=
        nullptr)  //&& tt->getType() != ThreadTimer::Oneshot)
    {
      /* Oneshot should also use asyc call.
         otherwise deadlock will happened between LoopData::lock and
         Paxos::lock. */
      // assert(tt->callBackPtr != nullptr);
      if (tt->getType() != ThreadTimer::Oneshot)
        tt->getService()->sendAsyncEvent(
            ThreadTimer::callbackRunWeak,
            std::move(CallbackWeakType(tt->callBackPtr)));
      else
        tt->getService()->sendAsyncEvent(ThreadTimer::callbackRun,
                                         tt->callBackPtr);
    } else
      tt->callBackPtr->run();
    if (tt->getType() == ThreadTimer::Oneshot) delete tt;
  }
}
}; /* end of namespace alisql */
