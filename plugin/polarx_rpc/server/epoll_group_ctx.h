//
// Created by zzy on 2022/9/2.
//

#pragma once

#include <memory>

#include "../session/session_base.h"
#include "../sql_query/query_string_builder.h"
#include "../utility/array_queue.h"
#include "../utility/time.h"

namespace polarx_rpc {

struct reusable_session_t {
  int64_t start_time_ms;
  Query_string_builder qb;
  CsessionBase session;

  reusable_session_t()
      : start_time_ms(Ctime::steady_ms()), qb(1024), session(0) {}
};

struct epoll_group_ctx_t final {
  static const auto BUFFERED_REUSABLE_SESSION_COUNT = 4;
  CarrayQueue<std::unique_ptr<reusable_session_t>> reusable_sessions;

  epoll_group_ctx_t() : reusable_sessions(BUFFERED_REUSABLE_SESSION_COUNT) {}
};

}  // namespace polarx_rpc
