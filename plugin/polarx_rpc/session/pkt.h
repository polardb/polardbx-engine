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
