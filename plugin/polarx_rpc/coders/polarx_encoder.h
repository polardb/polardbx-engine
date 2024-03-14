//
// Created by zzy on 2022/8/4.
//

#pragma once

#include <cstdint>

#include "../common_define.h"
#include "../server/server_variables.h"
#include "encoders/encoding_buffer.h"
#include "encoders/encoding_polarx_messages.h"
#include "encoders/encoding_pool.h"

namespace polarx_rpc {

class CtcpConnection;

class CpolarxEncoder final {
  NO_COPY_MOVE(CpolarxEncoder)

 private:
  protocol::Encoding_pool enc_pool_;
  protocol::Encoding_buffer enc_buf_;
  protocol::PolarX_Message_encoder msg_enc_;

  /// for flow control
  int64_t flushed_bytes_;

 public:
  explicit CpolarxEncoder(const uint64_t &sid)
      : enc_pool_(max_cached_output_buffer_pages,
                  protocol::Encoding_buffer::k_page_size),
        enc_buf_(&enc_pool_),
        msg_enc_(sid, &enc_buf_),
        flushed_bytes_(0) {}

  inline const protocol::Encoding_buffer &encoding_buffer() { return enc_buf_; }
  inline protocol::PolarX_Message_encoder &message_encoder() {
    return msg_enc_;
  }

  inline void reset() {
    /// clear all data and keep one page in enc_buf_ and msg_enc_
    msg_enc_.buffer_reset();
  }

  inline int64_t get_flushed_bytes() const { return flushed_bytes_; }

  inline void reset_flushed_bytes() { flushed_bytes_ = 0; }

  bool flush(CtcpConnection &tcp);
};

}  // namespace polarx_rpc
