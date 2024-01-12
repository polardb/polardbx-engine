//
// Created by zzy on 2022/7/25.
//

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>

#include "../coders/protocol_fwd.h"

namespace polarx_rpc {

#pragma pack(push, 1)

struct galaxy_pkt_hdr_t final {
  uint64_t sid;
  uint8_t version;
  uint32_t length;
  uint8_t type;
};

static_assert(14 == sizeof(galaxy_pkt_hdr_t), "Bad size of galaxy_pkt_hdr_t.");

struct polarx_pkt_hdr_t final {
  uint64_t sid;
  uint32_t length;
  uint8_t type;
};

static_assert(13 == sizeof(polarx_pkt_hdr_t), "Bad size of polarx_pkt_hdr_t.");

#pragma pack(pop)

struct msg_t final {
  uint8_t type;
  std::unique_ptr<ProtoMsg> msg;
};

}  // namespace polarx_rpc
