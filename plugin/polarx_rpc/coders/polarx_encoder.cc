//
// Created by zzy on 2022/8/4.
//

#include <mutex>

#include "../server/tcp_connection.h"

#include "polarx_encoder.h"

namespace polarx_rpc {

bool CpolarxEncoder::flush(CtcpConnection &tcp) {
  auto write_success = true;
  if (!enc_buf_.is_empty()) {
    std::lock_guard<std::mutex> lck(tcp.send_lock());
    auto page = enc_buf_.m_front;
    while (page) {
      const auto len = page->get_used_bytes();
      write_success = tcp.send(page->m_begin_data, len);
      if (!write_success) break;
      flushed_bytes_ += len;
      page = page->m_next_page;
    }
  }
  /// reset buffer any way
  reset();
  return write_success;
}

}  // namespace polarx_rpc
