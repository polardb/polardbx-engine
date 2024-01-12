//
// Created by zzy on 2022/9/6.
//

#include "mysql/service_thd_wait.h"

#include "flow_control.h"

namespace polarx_rpc {

void CflowControl::wait_begin() {
  if (thd_ != nullptr) thd_wait_begin(thd_, THD_WAIT_USER_LOCK);
}

void CflowControl::wait_end() {
  if (thd_ != nullptr) thd_wait_end(thd_);
}

}  // namespace polarx_rpc
