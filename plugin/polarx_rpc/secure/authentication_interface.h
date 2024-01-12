/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "../utility/error.h"

#include "account_verification_interface.h"
#include "sha256_password_cache_interface.h"

namespace polarx_rpc {

class Authentication_info {
 public:
  std::string m_tried_account_name;
  bool m_was_using_password{false};

  std::string m_authed_account_name;
  std::string m_authed_host_or_ip;

  void reset() {
    m_was_using_password = false;
    m_tried_account_name.clear();
    m_authed_account_name.clear();
    m_authed_host_or_ip.clear();
  }

  bool is_valid() const { return !m_tried_account_name.empty(); }
};

class Authentication_interface {
 public:
  enum Status { Ongoing, Succeeded, Failed, Error };

  struct Response {
    Response(const Status status_ = Ongoing, const int error_ = 0,
             std::string data_ = "")
        : data(std::move(data_)), status(status_), error_code(error_) {}
    std::string data;
    Status status;
    int error_code;
  };

  Authentication_interface() = default;
  Authentication_interface(const Authentication_interface &) = delete;
  Authentication_interface &operator=(const Authentication_interface &) =
      delete;

  virtual ~Authentication_interface() = default;

  virtual Response handle_start(const std::string &mechanism,
                                const std::string &data,
                                const std::string &initial_response) = 0;

  virtual Response handle_continue(const std::string &data) = 0;

  virtual err_t authenticate_account(const std::string &user,
                                     const std::string &host,
                                     const std::string &passwd) = 0;

  virtual const Authentication_info &get_authentication_info() const = 0;
};

typedef std::unique_ptr<Authentication_interface> Authentication_interface_ptr;

}  // namespace polarx_rpc
