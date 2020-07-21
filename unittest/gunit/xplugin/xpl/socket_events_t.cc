/* Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.

This program is also distributed with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have included with MySQL.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License, version 2.0, for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <atomic>
#include <thread>

#include "plugin/x/ngs/include/ngs/socket_events.h"

namespace ngs {
namespace test {

class Socket_events_task_suite : public ::testing::Test {
 public:
  Socket_events m_sut;
};

TEST_F(Socket_events_task_suite, loop_doesnt_block_when_no_events) {
  m_sut.loop();
}

TEST_F(Socket_events_task_suite, execute_loop_until_no_events) {
  int execution_count = 4;
  m_sut.add_timer(10, [&execution_count]() { return --execution_count; });
  m_sut.loop();
  ASSERT_EQ(0, execution_count);
}

TEST_F(Socket_events_task_suite,
       break_loop_is_queued_and_ignores_active_events) {
  int execution_count = 0;

  m_sut.break_loop();
  m_sut.add_timer(10, [&execution_count]() {
    ++execution_count;
    return true;
  });
  m_sut.loop();
  ASSERT_EQ(0, execution_count);
}

TEST_F(Socket_events_task_suite, break_loop_from_thread) {
  std::atomic<int> execution_count;

  std::thread break_thread{[this, &execution_count]() {
    while (execution_count.load() < 10) {
    }
    m_sut.break_loop();
  }};

  m_sut.add_timer(10, [&execution_count]() {
    ++execution_count;
    return true;
  });
  m_sut.loop();
  ASSERT_LT(0, execution_count.load());
  break_thread.join();
}

TEST_F(Socket_events_task_suite, break_loop_from_thread_always_active) {
  std::atomic<int> execution_count;

  std::thread break_thread{[this, &execution_count]() {
    while (execution_count.load() < 10) {
    }
    m_sut.break_loop();
  }};

  m_sut.add_timer(0, [&execution_count]() {
    ++execution_count;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return true;
  });
  m_sut.loop();
  ASSERT_LT(0, execution_count.load());
  break_thread.join();
}

}  // namespace test
}  // namespace ngs
