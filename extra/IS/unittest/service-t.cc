/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  service-t.cc,v 1.0 08/01/2016 11:14:06 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file service-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/01/2016 11:14:06 AM
 * @version 1.0
 * @brief unit test for alisql::Service
 *
 **/

#include <gtest/gtest.h>
#include <unistd.h>
#include "service.h"
#include "thread_timer.h"

using namespace alisql;

extern easy_log_level_t easy_log_level;
easy_log_level_t save_easy_log_level;

static void msleep(uint64_t t) {
  struct timeval sleeptime;
  if (t == 0) return;
  sleeptime.tv_sec = t / 1000;
  sleeptime.tv_usec = (t - (sleeptime.tv_sec * 1000)) * 1000;
  select(0, 0, 0, 0, &sleeptime);
}

TEST(Service, enableDebugLog) {
  save_easy_log_level = easy_log_level;
  easy_log_level = EASY_LOG_INFO;
}

TEST(Service, srv) {
  Service *srv = new Service(NULL);
  EXPECT_TRUE(srv);

  srv->init();
  srv->start(11001);

  sleep(1);

  srv->shutdown();
  delete srv;
}

/*
static void
timeout_cb (EV_P_ ev_timer *w, int revents)
{
  static int cnt= 0;
  std::cout << "aaaaaaaa" << std::endl << std::flush;
  if (++cnt > 5)
    ev_break (EV_A_ EVBREAK_ONE);
}
TEST(Service, ev_timer)
{
  struct ev_loop *loop = EV_DEFAULT;
  ev_timer timeout_watcher;

  ev_timer_init (&timeout_watcher, timeout_cb, 0.0, 0.2);
  ev_timer_start (loop, &timeout_watcher);

  std::cout << "Start sleep." << std::endl << std::flush;
  ev_run (loop, 0);
  std::cout << "Stop sleep." << std::endl << std::flush;

}
*/

static std::atomic<int> timerCnt(0);

void foo1(int i) {
  std::cout << "Execute foo1!!! arg:" << i << std::endl << std::flush;
}
void foo2() { std::cout << "Execute foo2!!!" << std::endl << std::flush; }
std::atomic<int> tt3Called(0);
TEST(Service, ThreadTimer) {
  extern easy_log_level_t easy_log_level;
  easy_log_level_t old_level = easy_log_level;
  easy_log_level = EASY_LOG_INFO;
  auto tts = new ThreadTimerService();
  auto *tt1 = new ThreadTimer(tts, 0.2, ThreadTimer::Repeatable, foo1, 2);
  tt1->start();
  /* There is no need to delete tt2 or the new timer. */
  /*ThreadTimer *tt2= */ new ThreadTimer(tts, 1.0, ThreadTimer::Oneshot, foo2);

  new ThreadTimer(
      tts, 0.5, ThreadTimer::Oneshot,
      [=](uint64_t i) {
        std::cout << "Execute index:" << i << std::endl << std::flush;
      },
      123);

  ThreadTimer *tt3 = new ThreadTimer(tts, 2.0, ThreadTimer::Stage, [] {
    std::cout << "foo3" << std::endl << std::flush;
    tt3Called.fetch_add(1);
  });
  tt3->start();
  std::cout << "StageExtraTime:" << tt3->getStageExtraTime() << std::endl
            << std::flush;

  std::cout << "Start sleep in ThreadTimerTest" << std::endl << std::flush;
  sleep(1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  sleep(1);
  std::cout << "Stop sleep in ThreadTimerTest" << std::endl << std::flush;
  tt1->stop();
  sleep(2);
  EXPECT_EQ(tt3Called.load(), 1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  tt3->restart(3.0);
  sleep(2);
  EXPECT_EQ(tt3Called.load(), 1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  sleep(2);
  EXPECT_TRUE(tt3Called.load() == 2 || tt3->getCurrentStage() == 1);
  tt3->stop();

  delete tt1;
  delete tt3;
  delete tts;
  easy_log_level = old_level;
}

void ttCallback(std::string *str) {
  std::cout << *str << std::endl << std::flush;
  ++timerCnt;
}

TEST(Service, ThreadTimerRepeatable) {
  auto tts = new ThreadTimerService();
  timerCnt = 0;

  std::string str("aaaaaaa");
  ThreadTimer *tmr =
      new ThreadTimer(tts, 0.3, ThreadTimer::Repeatable, ttCallback, &str);
  tmr->start();

  std::cout << "Start sleep." << std::endl << std::flush;
  sleep(1);
  EXPECT_EQ(timerCnt, 3);
  timerCnt = 0;
  std::cout << "Change." << std::endl << std::flush;
  tmr->restart(0.7);
  sleep(2);
  EXPECT_EQ(timerCnt, 2);
  timerCnt = 0;
  std::cout << "Reset." << std::endl << std::flush;
  tmr->restart(1.5);
  sleep(1);
  tmr->restart();
  sleep(1);
  tmr->restart();
  sleep(2);
  std::cout << "Reset.3" << std::endl << std::flush;
  EXPECT_EQ(timerCnt, 1);
  timerCnt = 0;
  std::cout << "Stop sleep." << std::endl << std::flush;

  tmr->stop();
  delete tmr;
  delete tts;
}

TEST(Service, ThreadTimerRepeatableStop) {
  auto tts = new ThreadTimerService();
  timerCnt = 0;

  std::string str("aaaaaaa");
  ThreadTimer *tmr =
      new ThreadTimer(tts, 2.0, ThreadTimer::Repeatable, ttCallback, &str);
  tmr->start();

  std::cout << "Start sleep." << std::endl << std::flush;
  sleep(3);
  EXPECT_EQ(timerCnt, 1);

  easy_info_log("stop timer.");
  tmr->stop();

  msleep(1500);

  easy_info_log("start timer 1.5s after last stop.");
  tmr->start();
  msleep(1500);
  easy_info_log("start timer after 1.5s.");
  EXPECT_EQ(timerCnt, 1);
  msleep(1000);
  easy_info_log("start timer after 2.5s.");
  EXPECT_EQ(timerCnt, 2);

  tmr->stop();

  delete tmr;
  delete tts;
}

TEST(Service, ThreadTimer_weight) {
  auto tts = new ThreadTimerService();
  tt3Called.store(0);

  ThreadTimer *tt3 = new ThreadTimer(tts, 2.0, ThreadTimer::Stage, [] {
    std::cout << "foo3" << std::endl << std::flush;
    tt3Called.fetch_add(1);
  });
  tt3->setRandWeight(5);
  tt3->start();

  std::cout << "Start sleep in ThreadTimerTest" << std::endl << std::flush;
  sleep(1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  sleep(1);
  std::cout << "Stop sleep in ThreadTimerTest" << std::endl << std::flush;
  sleep(1);
  EXPECT_EQ(tt3Called.load(), 1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  tt3->setRandWeight(1);
  tt3->restart(3);
  std::cout << "Start sleep 2 in ThreadTimerTest" << std::endl << std::flush;
  sleep(2);
  std::cout << "Stop sleep 2 in ThreadTimerTest" << std::endl << std::flush;
  EXPECT_EQ(tt3Called.load(), 1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  sleep(2);
  EXPECT_EQ(tt3Called.load(), 1);
  EXPECT_EQ(tt3->getCurrentStage(), 1);
  tt3->stop();

  delete tt3;
  delete tts;
}

TEST(Service, ThreadTimer_weight_repeat1) {
  auto tts = new ThreadTimerService();
  tt3Called.store(0);

  ThreadTimer *tt3 = new ThreadTimer(tts, 10.0, ThreadTimer::Stage, [] {
    std::cout << "foo3" << std::endl << std::flush;
    tt3Called.fetch_add(1);
  });
  tt3->setRandWeight(6);
  tt3->start();

  std::cout << "Start sleep in ThreadTimerTest" << std::endl << std::flush;
  sleep(11);
  std::cout << "Stop sleep in ThreadTimerTest" << std::endl << std::flush;
  EXPECT_EQ(tt3->getCurrentStage(), 1);
  EXPECT_EQ(tt3Called.load(), 0);
  tt3->restart();
  std::cout << "Start sleep 2 in ThreadTimerTest" << std::endl << std::flush;
  sleep(8);
  std::cout << "Stop sleep 2 in ThreadTimerTest" << std::endl << std::flush;
  EXPECT_EQ(tt3Called.load(), 0);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  tt3->stop();

  delete tt3;
  delete tts;
}

TEST(Service, disableDebugLog) { easy_log_level = save_easy_log_level; }
