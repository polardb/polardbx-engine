//
// Created by wumu on 2022/5/13.
//

#include "thread_pool.h"

namespace im {

ThreadPool *thread_pool = nullptr;

int InitThreadPool(uint32_t count) {
  thread_pool = new ThreadPool(count);
  return 0;
}
}  // namespace im

void UninitChangesetThreadPool() {
  delete im::thread_pool;
  im::thread_pool = nullptr;
}
