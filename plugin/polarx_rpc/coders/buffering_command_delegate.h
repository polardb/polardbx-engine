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
