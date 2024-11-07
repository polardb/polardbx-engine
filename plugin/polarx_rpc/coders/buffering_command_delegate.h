/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by zzy on 2022/8/31.
//

#pragma once

#include <functional>
#include <list>

#include "callback_command_delegate.h"

namespace polarx_rpc {

class CbufferingCommandDelegate : public CcallbackCommandDelegate {
 public:
  CbufferingCommandDelegate()
      : CcallbackCommandDelegate(
            std::bind(&CbufferingCommandDelegate::begin_row_cb, this),
            std::bind(&CbufferingCommandDelegate::end_row_cb, this,
                      std::placeholders::_1)) {}

  // When vector is going to be reallocated then the Field pointers are copied
  // but are release by destructor of Row_data
  using Resultset = std::list<Row_data>;

  void set_resultset(const Resultset &resultset) { m_resultset = resultset; }
  const Resultset &get_resultset() const { return m_resultset; }
  void set_status_info(const info_t &status_info) { info_ = status_info; }
  void reset() override {
    m_resultset.clear();
    CcommandDelegate::reset();
  }

 private:
  Resultset m_resultset;

  Row_data *begin_row_cb() {
    m_resultset.push_back(Row_data());
    return &m_resultset.back();
  }

  bool end_row_cb(Row_data *row) { return true; }
};

}  // namespace polarx_rpc
