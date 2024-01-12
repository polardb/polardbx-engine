/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

#include <map>
#include <memory>
#include <string>
#include <thread>

#include "../session/session_base.h"
#include "../utility/error.h"

#include "account_verification_interface.h"
#include "authentication_interface.h"

namespace polarx_rpc {
class CtcpConnection;

class Account_verification_handler {
 public:
  explicit Account_verification_handler(CtcpConnection &tcp)
      : m_tcp(tcp), m_session(0) {
    m_err = m_session.init(0);
    if (!m_err)  /// detach when initialized
      m_session.detach();
    if (UNLIKELY(!m_session.is_detach_and_tls_cleared()))
      sql_print_error("FATAL: polarx_rpc session not detach and cleared!");
  }
  Account_verification_handler(
      CtcpConnection &tcp,
      const Account_verification_interface::Account_type account_type,
      Account_verification_interface *verificator)
      : m_tcp(tcp), m_session(0), m_account_type(account_type) {
    m_err = m_session.init(0);
    if (!m_err)  /// detach when initialized
      m_session.detach();
    if (UNLIKELY(!m_session.is_detach_and_tls_cleared()))
      sql_print_error("FATAL: polarx_rpc session not detach and cleared!");
    add_account_verificator(account_type, verificator);
  }

  virtual ~Account_verification_handler() = default;

  virtual err_t authenticate(Authentication_interface &account_verificator,
                             Authentication_info *authenication_info,
                             const std::string &sasl_message);

  err_t verify_account(const std::string &user, const std::string &host,
                       const std::string &passwd,
                       Authentication_info *authenication_info);

  void add_account_verificator(
      const Account_verification_interface::Account_type account_type,
      Account_verification_interface *verificator) {
    m_verificators[account_type].reset(verificator);
  }

  virtual const Account_verification_interface *get_account_verificator(
      Account_verification_interface::Account_type account_type) const;

 private:
  typedef std::map<Account_verification_interface::Account_type,
                   Account_verification_interface_ptr>
      Account_verificator_list;

  struct Account_record {
    bool require_secure_transport{true};
    std::string db_password_hash;
    std::string auth_plugin_name;
    bool is_account_locked{true};
    bool is_password_expired{true};
    bool disconnect_on_expired_password{true};
    bool is_offline_mode_and_not_super_user{true};
  };

  static bool extract_sub_message(const std::string &message,
                                  std::size_t &element_position,
                                  std::string &sub_message);

  bool extract_last_sub_message(const std::string &message,
                                std::size_t &element_position,
                                std::string &sub_message) const;

  static Account_verification_interface::Account_type
  get_account_verificator_id(const std::string &plugin_name);

  err_t get_account_record(const std::string &user, const std::string &host,
                           Account_record &record);

  static std::string get_sql(const std::string &user, const std::string &host);

  CtcpConnection &m_tcp;
  CsessionBase m_session;  /// internal session for auth
  std::thread::id m_attached{std::thread::id()};
  err_t m_err;
  Account_verificator_list m_verificators;
  Account_verification_interface::Account_type m_account_type =
      Account_verification_interface::Account_unsupported;
};

typedef std::unique_ptr<Account_verification_handler>
    Account_verification_handler_ptr;

}  // namespace polarx_rpc
