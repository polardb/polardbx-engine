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


/*
 * Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include <cstddef>
#include <cstdint>
#include <string>

#include "my_sys.h"

#include "../coders/buffering_command_delegate.h"
#include "../server/tcp_connection.h"
#include "../sql_query/query_string_builder.h"
#include "../utility/error.h"

#include "account_verification_handler.h"

namespace polarx_rpc {

err_t Account_verification_handler::authenticate(
    Authentication_interface &account_verificator,
    Authentication_info *authenication_info, const std::string &sasl_message) {
  /// return error if failed to init
  if (m_err) return m_err;

  std::size_t message_position = 0;
  std::string schema;
  std::string account;
  std::string passwd;
  if (sasl_message.empty() ||
      !extract_sub_message(sasl_message, message_position, schema) ||
      !extract_sub_message(sasl_message, message_position, account) ||
      !extract_last_sub_message(sasl_message, message_position, passwd))
    return err_t::SQLError_access_denied();

  authenication_info->m_tried_account_name = account;
  authenication_info->m_was_using_password = !passwd.empty();

  if (account.empty()) return err_t::SQLError_access_denied();

  /// attach before authenticate
  assert(m_attached == std::thread::id());
  auto err = m_session.attach();
  if (err) return err;
  m_attached = std::this_thread::get_id();
  err = m_session.authenticate(account.c_str(), m_tcp.host().c_str(),
                               m_tcp.ip().c_str(), schema.c_str(), passwd,
                               account_verificator, false);
  /// detach
  m_session.detach();  /// ignore result
  m_attached = std::thread::id();
  if (UNLIKELY(!m_session.is_detach_and_tls_cleared()))
    sql_print_error("FATAL: polarx_rpc session not detach and cleared!");
  return err;
}

bool Account_verification_handler::extract_last_sub_message(
    const std::string &message, std::size_t &element_position,
    std::string &sub_message) const {
  if (element_position >= message.size()) return true;

  sub_message = message.substr(element_position);
  element_position = std::string::npos;

  return true;
}

bool Account_verification_handler::extract_sub_message(
    const std::string &message, std::size_t &element_position,
    std::string &sub_message) {
  if (element_position >= message.size()) return true;

  if (message[element_position] == '\0') {
    ++element_position;
    sub_message.clear();
    return true;
  }

  std::string::size_type last_character_of_element =
      message.find('\0', element_position);
  sub_message = message.substr(element_position, last_character_of_element);
  element_position = last_character_of_element;
  if (element_position != std::string::npos)
    ++element_position;
  else
    return false;
  return true;
}

const Account_verification_interface *
Account_verification_handler::get_account_verificator(
    const Account_verification_interface::Account_type account_type) const {
  Account_verificator_list::const_iterator i =
      m_verificators.find(account_type);
  return i == m_verificators.end() ? nullptr : i->second.get();
}

Account_verification_interface::Account_type
Account_verification_handler::get_account_verificator_id(
    const std::string &name) {
  if (name == "mysql_native_password")
    return Account_verification_interface::Account_native;
  /// not support others
  return Account_verification_interface::Account_unsupported;
}

err_t Account_verification_handler::verify_account(
    const std::string &user, const std::string &host, const std::string &passwd,
    Authentication_info *authenication_info) {
  Account_record record;
  if (auto error = get_account_record(user, host, record)) return error;

  auto account_verificator_id =
      get_account_verificator_id(record.auth_plugin_name);
  auto *p = get_account_verificator(account_verificator_id);

  // password check
  if (!p || !p->verify_authentication_string(user, host, passwd,
                                             record.db_password_hash))
    return err_t::SQLError_access_denied();

  // password check succeeded but...
  if (record.is_account_locked) {
    return err_t::SQLError(ER_ACCOUNT_HAS_BEEN_LOCKED,
                           authenication_info->m_tried_account_name.c_str(),
                           m_tcp.host().c_str());
  }

  if (record.is_offline_mode_and_not_super_user)
    return err_t::SQLError(ER_SERVER_OFFLINE_MODE);

  // password expiration check must come last, because password expiration
  // is not a fatal error, a client that supports expired password state,
  // will be let in... so the user can only  get this error if the auth
  // succeeded
  if (record.is_password_expired) {
    // if the password is expired, it's only a fatal error if
    // disconnect_on_expired_password is enabled AND the client doesn't support
    // expired passwords (this check is done by the caller of this)
    // if it's NOT enabled, then the user will be allowed to login in
    // sandbox mode, even if the client doesn't support expired passwords
    auto result = err_t::SQLError(ER_MUST_CHANGE_PASSWORD_LOGIN);
    return record.disconnect_on_expired_password ? err_t::Fatal(result)
                                                 : result;
  }

  /// ignore secure transport
  //  if (record.require_secure_transport &&
  //      !Connection_type_helper::is_secure_type(
  //          m_session->client().connection().get_type()))
  //    return ngs::SQLError(ER_SECURE_TRANSPORT_REQUIRED);
  //
  //  return record.user_required.validate(
  //      Ssl_session_options(&m_session->client().connection()));

  /// store final name and host_or_ip
  authenication_info->m_authed_account_name = user;
  authenication_info->m_authed_host_or_ip = host;
  return err_t::Success();
}

err_t Account_verification_handler::get_account_record(
    const std::string &user, const std::string &host,
    Account_record &record) try {
  /// return error if failed to init
  if (m_err) return m_err;

  auto sql = get_sql(user, host);
  CbufferingCommandDelegate delegate;
  assert(m_attached == std::this_thread::get_id());
  m_session.execute_sql(sql.c_str(), sql.length(), delegate);
  auto result = delegate.get_resultset();
  // The query asks for primary key, thus here we should get only one row
  if (result.size() != 1)
    return err_t(ER_NO_SUCH_USER, "Invalid user or password");

  auto row = result.front();
  record.require_secure_transport = row.fields[0]->value.v_long;
  record.db_password_hash = *row.fields[1]->value.v_string;
  record.auth_plugin_name = *row.fields[2]->value.v_string;
  record.is_account_locked = row.fields[3]->value.v_long;
  record.is_password_expired = row.fields[4]->value.v_long;
  record.disconnect_on_expired_password = row.fields[5]->value.v_long;
  record.is_offline_mode_and_not_super_user = row.fields[6]->value.v_long;
  return err_t::Success();
} catch (const err_t &e) {
  return e;
}

std::string Account_verification_handler::get_sql(const std::string &user,
                                                  const std::string &host) {
  Query_string_builder qb;

  // Query for a concrete users primary key (USER,HOST columns) which was
  // chosen by MySQL Server and verify hash and plugin column.
  // There are also other informations, like account lock, if password expired
  // and user can be connected, if server is in offline mode and the user is
  // with super priv.
  // column `is_password_expired` is set to true if - password expired
  // column `disconnect_on_expired_password` is set to true if
  //  - disconnect_on_expired_password
  // column `is_offline_mode_and_not_super_user` is set true if it
  //  - offline mode and user has not super priv
  qb.put(
        "/* xplugin authentication */ SELECT @@require_secure_transport, "
        "`authentication_string`, `plugin`,"
        "(`account_locked`='Y') as is_account_locked, "
        "(`password_expired`!='N') as `is_password_expired`, "
        "@@disconnect_on_expired_password as "
        "`disconnect_on_expired_password`, "
        "@@offline_mode and (`Super_priv`='N') as "
        "`is_offline_mode_and_not_super_user`,"
        "`ssl_type`, `ssl_cipher`, `x509_issuer`, `x509_subject` "
        "FROM mysql.user WHERE ")
      .quote_string(user)
      .put(" = `user` AND ")
      .quote_string(host)
      .put(" = `host` ");
  return qb.get();
}

}  // namespace polarx_rpc
