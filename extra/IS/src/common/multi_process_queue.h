/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  multi_process_queue.h,v 1.0 04/05/2017 11:45:50 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file multi_process_queue.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 04/05/2017 11:45:50 AM
 * @version 1.0
 * @brief
 *
 **/

#ifndef multi_process_queue_INC
#define multi_process_queue_INC

// #include "single_process_queue.h"

namespace alisql {

template <typename TaskType>
// class MultiProcessQueue : public SingleProcessQueue<TaskType> {
class MultiProcessQueue {
 public:
  explicit MultiProcessQueue(uint64_t maxConc)
      : stop_(false), maxConc_(maxConc), curConc_(0) {
    onProcess_.store(false);
  }
  virtual ~MultiProcessQueue(){};
  bool push(TaskType *task) {
    std::lock_guard<std::mutex> lg(lock_);
    if (!stop_) {
      taskList_.push(task);
      return (curConc_.load() < maxConc_.load());
    } else
      return false;
  }
  void stop() {
    lock_.lock();
    stop_ = true;
    while (!taskList_.empty()) {
      TaskType *task = taskList_.front();
      taskList_.pop();
      delete task;
    }
    lock_.unlock();
    while (curConc_.load() > 0)
      ;
  }
  void multiProcess(std::function<void(TaskType *)> cb) {
    uint64_t tasks;
    lock_.lock();
    if (curConc_.load() >= maxConc_.load() || stop_) {
      lock_.unlock();
      return;
    } else {
      curConc_.fetch_add(1);
      lock_.unlock();
    }

    for (;;) {
      lock_.lock();
      tasks = taskList_.size();
      if (tasks == 0) {
        curConc_.fetch_sub(1);
        lock_.unlock();
        break;
      }
      TaskType *task = taskList_.front();
      taskList_.pop();
      lock_.unlock();

      cb(task);
      delete task;
    }
  }

 protected:
  std::queue<TaskType *> taskList_;
  std::mutex lock_;
  std::atomic<bool> onProcess_;
  bool stop_;

  std::atomic<uint64_t> maxConc_;
  std::atomic<uint64_t> curConc_;

 private:
  MultiProcessQueue(const MultiProcessQueue &other);  // copy constructor
  const MultiProcessQueue &operator=(
      const MultiProcessQueue &other);                // assignment operator
};

}  // namespace alisql
#endif  // #ifndef multi_process_queue_INC
