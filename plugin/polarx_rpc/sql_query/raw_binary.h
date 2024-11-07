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
// Created by zzy on 2021/11/16.
//

#pragma once

#include <string>
#include <utility>

namespace polarx_rpc {

class RawBinary {
 private:
  std::string value_;

 public:
  explicit RawBinary(std::string val) : value_(std::move(val)) {}

  const std::string &get_value() const { return value_; }

  std::string to_hex_string() const {
    std::string buf;
    buf.resize(3 + value_.length() * 2);  // x'{hex string}'
    buf[0] = 'x';
    buf[1] = '\'';
    auto idx = 2;
    for (const auto &b : value_) {
      auto h = (((uint8_t)b) >> 4) & 0xF;
      buf[idx++] = h >= 10 ? (char)(h - 10 + 'a') : (char)(h + '0');
      auto l = ((uint8_t)b) & 0xF;
      buf[idx++] = l >= 10 ? (char)(l - 10 + 'a') : (char)(l + '0');
    }
    buf[idx] = '\'';
    return buf;
  }
};

}  // namespace polarx_rpc
