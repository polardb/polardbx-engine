/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#include "plugin/performance_point/pps_stat.h"
#include <memory>
#include "plugin/performance_point/pps.h"

void PPS_thread::begin_statement() {
  PPS_statement *sub_statement = nullptr;
  /* Top level statement */
  if (m_statement == nullptr) {
    m_statement = &m_main_statement;
    assert(m_statement->prev == nullptr);
  } else {
    std::unique_ptr<PPS_statement, PPS_element_deleter<PPS_statement>>
        m_pps_statement(allocate_object<PPS_statement>());

    if (m_pps_statement) {
      sub_statement = m_pps_statement.release();
      sub_statement->prev = m_statement;
      m_statement = sub_statement;
    }
  }
}

/**
  End statement:
  1) Aggregate statement stats to THD;
  2) Reset statement stats.
*/
void PPS_thread::end_statement() {
  assert(m_statement);
  /**
    If malloc failed when begin statement, then current m_statement will be
    NULL. and result in statistics loss, here allow to happen it.
  */
  if (m_statement) {
    m_cpu_stat.aggregate_from(m_statement->cpu_stat());
    m_io_stat.aggregate_from(m_statement->io_stat());
    m_lock_stat.aggregate_from(m_statement->lock_stat());
    m_statement->reset();
    if (m_statement == &m_main_statement) {
      assert(m_statement->prev == nullptr);
      m_statement = nullptr;
    } else {
      PPS_statement *current = m_statement;
      m_statement = m_statement->prev;
      assert(m_statement != nullptr);
      destroy_object<PPS_statement>(current);
    }
  }
}
