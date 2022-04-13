//
// Created by zzy on 2022/4/13.
//

#include "galaxy_flow_control.h"
#include "mysql/service_thd_wait.h"

namespace xpl {

void Galaxy_flow_control::wait_begin() {
  if (m_thd != nullptr) {
    thd_wait_begin(m_thd, THD_WAIT_USER_LOCK);
  }
}

void Galaxy_flow_control::wait_end() {
  if (m_thd != nullptr) {
    thd_wait_end(m_thd);
  }
}

}  // namespace xpl
