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
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include "../../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#include "sql/sql_class.h"
#endif

#include "xsession_impl.h"

#include <algorithm>
#include <array>
#include <chrono>  // NOLINT(build/c++11)
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "errmsg.h"
#include "my_compiler.h"
#include "my_config.h"
#include "my_dbug.h"
#ifdef MYSQL8
#include "my_macros.h"
#endif
#include "mysql_version.h"
#include "mysqld_error.h"

#include "mysqlxclient/xerror.h"
#include "validator/descriptor.h"
#include "validator/option_connection_validator.h"
#include "validator/option_context_validator.h"
#include "visitor/any_filler.h"
#include "xcapability_builder.h"
#include "xconnection_impl.h"
#include "xprotocol_factory.h"
#include "xprotocol_impl.h"
#include "xquery_result_impl.h"

namespace xcl {

const char *const ER_TEXT_CAPABILITY_NOT_SUPPORTED = "Capability not supported";
const char *const ER_TEXT_CAPABILITY_VALUE_INVALID =
    "Invalid value for capability";
const char *const ER_TEXT_OPTION_NOT_SUPPORTED = "Option not supported";
const char *const ER_TEXT_OPTION_VALUE_INVALID = "Invalid value for option";
const char *const ER_TEXT_OPTION_NOT_SUPPORTED_AFTER_CONNECTING =
    "Operation not supported after connecting";
const char *const ER_TEXT_NOT_CONNECTED = "Not connected";
const char *const ER_TEXT_ALREADY_CONNECTED = "Already connected";
const char *const ER_TEXT_CA_IS_REQUIRED =
    "TLS was marked that requires \"CA\", but it was not configured";
const char *const ER_TEXT_INVALID_AUTHENTICATION_CONFIGURED =
    "Ambiguous authentication methods given";

namespace details {

/** Check error code, if its client side error */
bool is_client_error(const XError &e) {
  const auto error_code = e.error();

  return (CR_X_ERROR_FIRST <= error_code && CR_X_ERROR_LAST >= error_code) ||
         (CR_ERROR_FIRST <= error_code && CR_ERROR_LAST >= error_code);
}

/**
  This class implemented the default behavior of the factory.
*/
class Protocol_factory_default : public Protocol_factory {
 public:
  std::shared_ptr<XProtocol> create_protocol(
      std::shared_ptr<Context> context) override {
    return std::make_shared<Protocol_impl>(context, this);
  }

  std::unique_ptr<XConnection> create_connection(
      std::shared_ptr<Context> context) override {
    std::unique_ptr<XConnection> result{new Connection_impl(context)};
    return result;
  }

  std::unique_ptr<XQuery_result> create_result(
      std::shared_ptr<XProtocol> protocol, Query_instances *query_instances,
      std::shared_ptr<Context> context) override {
    std::unique_ptr<XQuery_result> result{
        new Query_result(protocol, query_instances, context)};

    return result;
  }
};

bool scalar_get_v_uint(const PolarXRPC::Datatypes::Scalar &scalar,
                       uint64_t *out_value) {
  if (scalar.type() != PolarXRPC::Datatypes::Scalar::V_UINT) return false;

  *out_value = scalar.v_unsigned_int();

  return true;
}

bool get_array_of_strings_from_any(const PolarXRPC::Datatypes::Any &any,
                                   std::vector<std::string> *out_strings) {
  out_strings->clear();

  if (!any.has_type() || PolarXRPC::Datatypes::Any_Type_ARRAY != any.type())
    return false;

  for (const auto &element : any.array().value()) {
    if (!element.has_type() ||
        PolarXRPC::Datatypes::Any_Type_SCALAR != element.type())
      return false;

    const auto &scalar = element.scalar();

    if (!scalar.has_type()) return false;

    switch (scalar.type()) {
      case PolarXRPC::Datatypes::Scalar_Type_V_STRING:
        out_strings->push_back(scalar.v_string().value());
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_OCTETS:
        out_strings->push_back(scalar.v_octets().value());
        break;

      default:
        return false;
    }
  }

  return true;
}

std::string to_upper(const std::string &value) {
  std::string result;

  result.reserve(value.length() + 1);
  for (const auto c : value) {
    result.push_back(toupper(c));
  }

  return result;
}

std::string to_lower(const std::string &value) {
  std::string result;

  result.reserve(value.length() + 1);
  for (const auto c : value) {
    result.push_back(tolower(c));
  }

  return result;
}

class Capability_descriptor : public Descriptor {
 public:
  Capability_descriptor() = default;
  Capability_descriptor(const std::string name, Validator *validator)
      : Descriptor(validator), m_name(name) {}

  std::string get_name() const { return m_name; }

  XError get_supported_error() const override {
    return XError{CR_X_UNSUPPORTED_CAPABILITY_VALUE,
                  ER_TEXT_CAPABILITY_NOT_SUPPORTED};
  }

  XError get_wrong_value_error(const Argument_value &) const override {
    return XError{CR_X_UNSUPPORTED_CAPABILITY_VALUE,
                  ER_TEXT_CAPABILITY_VALUE_INVALID};
  }

 private:
  const std::string m_name;
};

Capability_descriptor get_capability_descriptor(
    const XSession::Mysqlx_capability capability) {
  switch (capability) {
    case XSession::Capability_can_handle_expired_password:
      return {"client.pwd_expire_ok", new Bool_validator()};

    case XSession::Capability_client_interactive:
      return {"client.interactive", new Bool_validator()};

    case XSession::Capability_session_connect_attrs:
      return {"session_connect_attrs", new Object_validator()};

    default:
      return {};
  }
}

class Option_descriptor : public Descriptor {
 public:
  using Descriptor::Descriptor;

  XError get_supported_error() const override {
    return XError{CR_X_UNSUPPORTED_OPTION, ER_TEXT_OPTION_NOT_SUPPORTED};
  }

  XError get_wrong_value_error(const Argument_value &) const override {
    return XError{CR_X_UNSUPPORTED_OPTION_VALUE, ER_TEXT_OPTION_VALUE_INVALID};
  }
};

Option_descriptor get_option_descriptor(const XSession::Mysqlx_option option) {
  using Mysqlx_option = XSession::Mysqlx_option;
  using Con_conf = Connection_config;
  using Ctxt = Context;

  switch (option) {
    case Mysqlx_option::Hostname_resolve_to:
      return Option_descriptor{new Contex_ip_validator()};

    case Mysqlx_option::Connect_timeout:
      return Option_descriptor{
          new Con_int_store<&Con_conf::m_timeout_connect>()};

    case Mysqlx_option::Session_connect_timeout:
      return Option_descriptor{
          new Con_int_store<&Con_conf::m_timeout_session_connect>()};

    case Mysqlx_option::Read_timeout:
      return Option_descriptor{new Con_int_store<&Con_conf::m_timeout_read>()};

    case Mysqlx_option::Write_timeout:
      return Option_descriptor{new Con_int_store<&Con_conf::m_timeout_write>()};

    case Mysqlx_option::Authentication_method:
      return Option_descriptor{new Contex_auth_validator()};

    case Mysqlx_option::Consume_all_notices:
      return Option_descriptor{
          new Ctxt_bool_store<&Ctxt::m_consume_all_notices>()};

    case Mysqlx_option::Datetime_length_discriminator:
      return Option_descriptor{
          new Ctxt_uint32_store<&Ctxt::m_datetime_length_discriminator>()};

    case Mysqlx_option::Network_namespace:
      return Option_descriptor{
          new Con_str_store<&Con_conf::m_network_namespace>()};

    default:
      return {};
  }
}

void translate_texts_into_auth_types(
    const std::vector<std::string> &values_list,
    std::set<Auth> *out_auths_list) {
  static const std::map<std::string, Auth> modes{
      {"MYSQL41", Auth::k_mysql41}/*,
      {"PLAIN", Auth::k_plain},
      {"SHA256_MEMORY", Auth::k_sha256_memory}*/};

  out_auths_list->clear();
  for (const auto &mode_text : values_list) {
    auto mode_value = modes.find(details::to_upper(mode_text));

    if (modes.end() == mode_value) continue;

    out_auths_list->insert(out_auths_list->end(), mode_value->second);
  }
}

const char *value_or_empty_string(const char *value) {
  if (nullptr == value) return "";

  return value;
}

const char *value_or_default_string(const char *value,
                                    const char *value_default) {
  if (nullptr == value) return value_default;

  if (0 == strlen(value)) return value_default;

  return value;
}

class Notice_server_hello_ignore {
 public:
  explicit Notice_server_hello_ignore(XProtocol *protocol)
      : m_protocol(protocol) {
    m_handler_id = m_protocol->add_notice_handler(
        *this, Handler_position::Begin, Handler_priority_low);
  }

  ~Notice_server_hello_ignore() {
    if (XCL_HANDLER_ID_NOT_VALID != m_handler_id) {
      m_protocol->remove_notice_handler(m_handler_id);
    }
  }

  Notice_server_hello_ignore(const Notice_server_hello_ignore &) = default;

  Handler_result operator()(XProtocol *, const bool is_global,
                            const PolarXRPC::Notice::Frame::Type type,
                            const char *, const uint32_t) {
    const bool is_hello_notice =
        PolarXRPC::Notice::Frame_Type_SERVER_HELLO == type;

    if (!is_global) return Handler_result::Continue;
    if (!is_hello_notice) return Handler_result::Continue;

    if (m_already_received) return Handler_result::Error;

    m_already_received = true;

    return Handler_result::Consumed;
  }

  bool m_already_received = false;
  XProtocol::Handler_id m_handler_id = XCL_HANDLER_ID_NOT_VALID;
  XProtocol *m_protocol;
};

template <typename Value>
XError set_object_capability(Context *context, Argument_object *capabilities,
                             const XSession::Mysqlx_capability capability,
                             const Value &value) {
  auto capability_type = details::get_capability_descriptor(capability);

  auto error = capability_type.is_valid(context, value);

  if (error) return error;

  (*capabilities)[capability_type.get_name()] = value;

  return {};
}

}  // namespace details

Session_impl::Session_impl(std::unique_ptr<Protocol_factory> factory)
    : m_context(std::make_shared<Context>()), m_factory(std::move(factory)) {
  if (nullptr == m_factory.get()) {
    m_factory.reset(new details::Protocol_factory_default());
  }

  setup_protocol();
}

Session_impl::~Session_impl() {
  auto &connection = get_protocol().get_connection();

  if (connection.state().is_connected()) {
    connection.close();
  }
}

XProtocol &Session_impl::get_protocol() { return *m_protocol; }

XError Session_impl::set_mysql_option(const Mysqlx_option option,
                                      const bool value) {
  if (is_connected()) {
    return XError{CR_ALREADY_CONNECTED,
                  ER_TEXT_OPTION_NOT_SUPPORTED_AFTER_CONNECTING};
  }

  auto option_type = details::get_option_descriptor(option);

  return option_type.is_valid(m_context.get(), value);
}

XError Session_impl::set_mysql_option(const Mysqlx_option option,
                                      const char *value) {
  const std::string value_str = nullptr == value ? "" : value;

  return set_mysql_option(option, value_str);
}

XError Session_impl::set_mysql_option(const Mysqlx_option option,
                                      const std::string &value) {
  if (is_connected())
    return XError{CR_ALREADY_CONNECTED,
                  ER_TEXT_OPTION_NOT_SUPPORTED_AFTER_CONNECTING};

  auto option_type = details::get_option_descriptor(option);

  return option_type.is_valid(m_context.get(), value);
}

XError Session_impl::set_mysql_option(
    const Mysqlx_option option, const std::vector<std::string> &values_list) {
  if (is_connected())
    return XError{CR_ALREADY_CONNECTED,
                  ER_TEXT_OPTION_NOT_SUPPORTED_AFTER_CONNECTING};

  Argument_array array;
  for (const auto &value : values_list) {
    array.push_back(Argument_value{value});
  }

  auto option_type = details::get_option_descriptor(option);

  return option_type.is_valid(m_context.get(), array);
}

XError Session_impl::set_mysql_option(const Mysqlx_option option,
                                      const int64_t value) {
  if (is_connected())
    return XError{CR_ALREADY_CONNECTED,
                  ER_TEXT_OPTION_NOT_SUPPORTED_AFTER_CONNECTING};

  auto option_type = details::get_option_descriptor(option);

  return option_type.is_valid(m_context.get(), value);
}

Argument_object &Session_impl::get_capabilites(const bool required) {
  if (required) return m_required_capabilities;

  DBUG_LOG("debug", "Returning optional set");
  return m_optional_capabilities;
}

XError Session_impl::set_capability(const Mysqlx_capability capability,
                                    const bool value, const bool required) {
  auto capability_type = details::get_capability_descriptor(capability);

  auto error = capability_type.is_valid(m_context.get(), value);

  if (error) return error;

  get_capabilites(required)[capability_type.get_name()] = value;

  return XError();
}

XError Session_impl::set_capability(const Mysqlx_capability capability,
                                    const std::string &value,
                                    const bool required) {
  auto capability_type = details::get_capability_descriptor(capability);

  auto error = capability_type.is_valid(m_context.get(), value);

  if (error) return error;

  get_capabilites(required)[capability_type.get_name()] = value;

  return {};
}

XError Session_impl::set_capability(const Mysqlx_capability capability,
                                    const char *value, const bool required) {
  auto capability_type = details::get_capability_descriptor(capability);

  auto error = capability_type.is_valid(m_context.get(), value);

  if (error) return error;

  get_capabilites(required)[capability_type.get_name()] =
      Argument_value{value, Argument_value::String_type::k_string};

  return {};
}

XError Session_impl::set_capability(const Mysqlx_capability capability,
                                    const int64_t value, const bool required) {
  auto capability_type = details::get_capability_descriptor(capability);

  auto error = capability_type.is_valid(m_context.get(), value);

  if (error) return error;

  get_capabilites(required)[capability_type.get_name()] = value;

  return {};
}

XError Session_impl::set_capability(const Mysqlx_capability capability,
                                    const Argument_object &value,
                                    const bool required) {
  return details::set_object_capability(
      m_context.get(), &get_capabilites(required), capability, value);
}

XError Session_impl::set_capability(const Mysqlx_capability capability,
                                    const Argument_uobject &value,
                                    const bool required) {
  return details::set_object_capability(
      m_context.get(), &get_capabilites(required), capability, value);
}

XError Session_impl::connect(const char *host, const uint16_t port,
                             const char *user, const char *pass,
                             const char *schema) {
  if (is_connected())
    return XError{CR_ALREADY_CONNECTED, ER_TEXT_ALREADY_CONNECTED};

  Session_connect_timeout_scope_guard timeout_guard{this};
  auto &connection = get_protocol().get_connection();
  const auto result =
      connection.connect(details::value_or_empty_string(host),
                         port ? port : 33660, m_context->m_internet_protocol);
  if (result) return result;

  const auto connection_type = connection.state().get_connection_type();
  details::Notice_server_hello_ignore notice_ignore(m_protocol.get());

  return authenticate(user, pass, schema, connection_type);
}

XError Session_impl::reauthenticate(const char *user, const char *pass,
                                    const char *schema) {
  if (!is_connected())
    return XError{CR_CONNECTION_ERROR, ER_TEXT_NOT_CONNECTED};

  auto error = get_protocol().send(::PolarXRPC::Session::Reset());

  if (error) return error;

  Session_connect_timeout_scope_guard timeout_guard{this};
  error = get_protocol().recv_ok();

  if (error) return error;

  auto connection_type =
      get_protocol().get_connection().state().get_connection_type();

  return authenticate(user, pass, schema, connection_type);
}

std::unique_ptr<XQuery_result> Session_impl::execute_sql(const std::string &sql,
                                                         XError *out_error) {
  if (!is_connected()) {
    *out_error = XError{CR_CONNECTION_ERROR, ER_TEXT_NOT_CONNECTED};

    return {};
  }

  ::PolarXRPC::Sql::StmtExecute stmt;

  stmt.set_stmt(sql);
  return m_protocol->execute_stmt(stmt, out_error);
}

std::unique_ptr<XQuery_result> Session_impl::execute_stmt(
    const std::string &ns, const std::string &sql,
    const Argument_array &arguments, XError *out_error) {
  if (!is_connected()) {
    *out_error = XError{CR_CONNECTION_ERROR, ER_TEXT_NOT_CONNECTED};

    return {};
  }

  ::PolarXRPC::Sql::StmtExecute stmt;

  stmt.set_stmt(sql);
  stmt.set_namespace_(ns);

  for (const auto &argument : arguments) {
    Any_filler filler(stmt.mutable_args()->Add());

    argument.accept(&filler);
  }

  return m_protocol->execute_stmt(stmt, out_error);
}

void Session_impl::close() {
  if (is_connected()) {
    m_protocol->execute_close();

    m_protocol.reset();
  }
}

void Session_impl::setup_protocol() {
  m_protocol = m_factory->create_protocol(m_context);
  setup_session_notices_handler();
  setup_general_notices_handler();
}

void Session_impl::setup_general_notices_handler() {
  auto context = m_context;

  m_protocol->add_notice_handler(
      [context](
          XProtocol *p MY_ATTRIBUTE((unused)),
          const bool is_global MY_ATTRIBUTE((unused)),
          const PolarXRPC::Notice::Frame::Type type MY_ATTRIBUTE((unused)),
          const char *payload MY_ATTRIBUTE((unused)),
          const uint32_t payload_size MY_ATTRIBUTE(
              (unused))) -> Handler_result {
        return context->m_consume_all_notices ? Handler_result::Consumed
                                              : Handler_result::Continue;
      },
      Handler_position::End, Handler_priority_low);
}

void Session_impl::setup_session_notices_handler() {
  auto context = m_context;

  m_protocol->add_notice_handler(
      [context](XProtocol *p MY_ATTRIBUTE((unused)),
                const bool is_global MY_ATTRIBUTE((unused)),
                const PolarXRPC::Notice::Frame::Type type, const char *payload,
                const uint32_t payload_size) -> Handler_result {
        return handle_notices(context, type, payload, payload_size);
      },
      Handler_position::End, Handler_priority_high);
}

void Session_impl::setup_server_supported_features(
    const PolarXRPC::Connection::Capabilities *capabilities) {
  for (const auto &capability : capabilities->capabilities()) {
    if ("authentication.mechanisms" == capability.name()) {
      std::vector<std::string> names_of_auth_methods;
      const auto &any = capability.value();

      details::get_array_of_strings_from_any(any, &names_of_auth_methods);

      details::translate_texts_into_auth_types(
          names_of_auth_methods, &m_server_supported_auth_methods);
    }
  }
}

bool Session_impl::is_connected() {
  if (!m_protocol) return false;

  return m_protocol->get_connection().state().is_connected();
}

XError Session_impl::authenticate(const char *user, const char *pass,
                                  const char *schema,
                                  Connection_type connection_type) {
  auto &protocol = get_protocol();
  auto &connection = protocol.get_connection();

  /* capabilities not support on polarx rpc
    // After adding pipelining to mysqlxclient, all requests below should
    // be merged into single send operation, followed by a read operation/s
    if (!m_required_capabilities.empty()) {
      Capabilities_builder builder;

      auto required_capabilities_set =
          builder.clear()
              .add_capabilities_from_object(m_required_capabilities)
              .get_result();
      auto error = protocol.execute_set_capability(required_capabilities_set);

      if (error) return error;
    }

    for (const auto &capability : m_optional_capabilities) {
      Capabilities_builder builder;
      auto optional_capabilities_set =
          builder.clear()
              .add_capability(capability.first, capability.second)
              .get_result();
      const auto error =
          protocol.execute_set_capability(optional_capabilities_set);

      // Optional capabilities might fail
      if (error.is_fatal() || details::is_client_error(error)) return error;
    }

    if (needs_servers_capabilities()) {
      XError out_error;
      const auto capabilities = protocol.execute_fetch_capabilities(&out_error);

      if (out_error) return out_error;

      setup_server_supported_features(capabilities.get());
    }
  */

  const auto is_secure_connection =
      connection.state().is_ssl_activated() ||
      (connection_type == Connection_type::Unix_socket);
  const auto &optional_auth_methods = validate_and_adjust_auth_methods(
      m_context->m_use_auth_methods, is_secure_connection);
  const auto &error = optional_auth_methods.first;
  if (error) return error;

  bool has_sha256_memory = false;
  bool fatal_error_received = false;
  XError reported_error;

  for (const auto &auth_method : optional_auth_methods.second) {
    const bool is_last = &auth_method == &optional_auth_methods.second.back();

    if (auth_method == "PLAIN" && !is_secure_connection) {
      // If this is not the last authentication mechanism then do not report
      // error but try those other methods instead.
      if (is_last) {
        return XError{
            CR_X_INVALID_AUTH_METHOD,
            "Invalid authentication method: PLAIN over unsecure channel"};
      }

      continue;
    }

    XError current_error = protocol.execute_authenticate(
        details::value_or_empty_string(user),
        details::value_or_empty_string(pass),
        details::value_or_empty_string(schema), auth_method);

    // Authentication successful, otherwise try to use different auth method
    if (!current_error) return {};

    const auto current_error_code = current_error.error();

    // In case of connection errors ('broken pipe', 'peer disconnected',
    // timeouts...) we should break the authentication sequence and return
    // an error.
    if (current_error_code == CR_SERVER_GONE_ERROR ||
        current_error_code == CR_X_WRITE_TIMEOUT ||
        current_error_code == CR_X_READ_TIMEOUT ||
        current_error_code == CR_UNKNOWN_ERROR) {
      // Expected disconnection
      if (fatal_error_received) return reported_error;

      // Unexpected disconnection
      return current_error;
    }

    // Try to choose most important error:
    //
    // |Priority |Action                        |
    // |---------|------------------------------|
    // |1        |No error was set              |
    // |2        |Last other than access denied |
    // |3        |Last access denied            |
    if (!reported_error || current_error_code != ER_ACCESS_DENIED_ERROR ||
        reported_error.error() == ER_ACCESS_DENIED_ERROR) {
      reported_error = current_error;
    }

    // Also we should stop the authentication sequence on fatal error.
    // Still it would break compatibility with servers that wrongly mark
    // PolarXRPC::Error message with fatal flag.
    //
    // To workaround the problem of backward compatibility, we should
    // remember that fatal error was received and try to continue the
    // sequence. After reception of fatal error, following connection
    // errors are expected (CR_SERVER_GONE_ERROR, CR_X_WRITE_TIMEOUT....)
    // and must be ignored.
    if (current_error.is_fatal()) fatal_error_received = true;

    if (auth_method == "SHA256_MEMORY") has_sha256_memory = true;
  }

  // In case when SHA256_MEMORY was used and no PLAIN (because of not using
  // secure connection) and all errors where ER_ACCESS_DENIED, there is
  // possibility that password cache on the server side is empty.
  // We need overwrite the error to give the user a hint that
  // secure connection can be used.
  if (has_sha256_memory && !is_secure_connection &&
      reported_error.error() == ER_ACCESS_DENIED_ERROR) {
    reported_error = XError{CR_X_AUTH_PLUGIN_ERROR,
                            "Authentication failed, check username and "
                            "password or try a secure connection"};
  }

  return reported_error;
}

std::vector<Auth> Session_impl::get_methods_sequence_from_auto(
    const Auth auto_authentication, const bool can_use_plain) {
  // Check all automatic methods and return possible sequences for them
  // This means that the corresponding auth sequences will be used:
  //   FALLBACK - MySQL 5.7 compatible automatic method:
  //     PLAIN if SSL is enabled, MYSQL41 otherwise,
  //   AUTO - MySQL 8.0 and above:
  //     sequence of SHA256_MEMORY -> (optional) PLAIN -> MYSQL41

  // Sequence like PLAIN, SHA256 or PLAIN, MYSQL41 will always fail
  // in case when PLAIN is going to fail still it may be used in future.
  const Auth plain_or_mysql41 =
      /*can_use_plain ? Auth::k_plain : */ Auth::k_mysql41;

  switch (auto_authentication) {
    case Auth::k_auto_fallback:
      return {plain_or_mysql41 /*, Auth::k_sha256_memory*/};

    case Auth::k_auto_from_capabilities:  // fall-through
    case Auth::k_auto:
      if (can_use_plain)
        return {/*Auth::k_sha256_memory, Auth::k_plain, */ Auth::k_mysql41};
      return {/*Auth::k_sha256_memory, */ Auth::k_mysql41};

    default:
      return {};
  }
}

bool Session_impl::is_auto_method(const Auth auto_authentication) {
  switch (auto_authentication) {
    case Auth::k_auto:                    // fall-through
    case Auth::k_auto_fallback:           // fall-through
    case Auth::k_auto_from_capabilities:  // fall-through
      return true;

    default:
      return false;
  }
}

std::pair<XError, std::vector<std::string>>
Session_impl::validate_and_adjust_auth_methods(std::vector<Auth> auth_methods,
                                               const bool can_use_plain) {
  const auto auth_methods_count = auth_methods.size();
  const Auth first_method =
      auth_methods_count == 0 ? Auth::k_auto : auth_methods[0];

  const auto auto_sequence =
      get_methods_sequence_from_auto(first_method, can_use_plain);
  if (!auto_sequence.empty()) {
    auth_methods.assign(auto_sequence.begin(), auto_sequence.end());
  } else {
    if (std::any_of(std::begin(auth_methods), std::end(auth_methods),
                    is_auto_method))
      return {XError{CR_X_INVALID_AUTH_METHOD,
                     ER_TEXT_INVALID_AUTHENTICATION_CONFIGURED},
              {}};
  }

  std::vector<std::string> auth_method_string_list;

  for (const auto auth_method : auth_methods) {
    if (0 < m_server_supported_auth_methods.count(auth_method))
      auth_method_string_list.push_back(get_method_from_auth(auth_method));
  }

  if (auth_method_string_list.empty()) {
    return {XError{CR_X_INVALID_AUTH_METHOD,
                   "Server doesn't support clients authentication methods"},
            {}};
  }

  return {{}, auth_method_string_list};
}

Handler_result Session_impl::handle_notices(
    std::shared_ptr<Context> context, const PolarXRPC::Notice::Frame::Type type,
    const char *payload, const uint32_t payload_size) {
  if (PolarXRPC::Notice::Frame_Type_SESSION_STATE_CHANGED == type) {
    PolarXRPC::Notice::SessionStateChanged session_changed;

    if (session_changed.ParseFromArray(payload, payload_size) &&
        session_changed.IsInitialized() && session_changed.has_value()) {
      if (PolarXRPC::Notice::SessionStateChanged::CLIENT_ID_ASSIGNED ==
          session_changed.param()) {
        return details::scalar_get_v_uint(session_changed.value(),
                                          &context->m_client_id)
                   ? Handler_result::Consumed
                   : Handler_result::Error;
      }
    }
  }

  return Handler_result::Continue;
}

std::string Session_impl::get_method_from_auth(const Auth auth) {
  switch (auth) {
    case Auth::k_auto:
      return "AUTO";
    case Auth::k_mysql41:
      return "MYSQL41";
      //    case Auth::k_sha256_memory:
      //      return "SHA256_MEMORY";
    case Auth::k_auto_from_capabilities:
      return "FROM_CAPABILITIES";
    case Auth::k_auto_fallback:
      return "FALLBACK";
      //    case Auth::k_plain:
      //      return "PLAIN";
    default:
      return "UNKNOWN";
  }
}

bool Session_impl::needs_servers_capabilities() const {
  if (m_context->m_use_auth_methods.size() == 1 &&
      m_context->m_use_auth_methods[0] == Auth::k_auto_from_capabilities)
    return true;

  return false;
}

Argument_uobject Session_impl::get_connect_attrs() const {
  return {
      {"_client_name", Argument_value{HAVE_MYSQLX_FULL_PROTO(
                           "libmysqlxclient", "libmysqlxclient_lite")}},
      {"_client_version", Argument_value{PACKAGE_VERSION}},
      {"_os", Argument_value{SYSTEM_TYPE}},
      {"_platform", Argument_value{MACHINE_TYPE}},
      {"_client_license", Argument_value{STRINGIFY_ARG(LICENSE)}},
#ifdef _WIN32
      {"_pid", Argument_value{std::to_string(
                   static_cast<uint64_t>(GetCurrentProcessId()))}},
      {"_thread", Argument_value{std::to_string(
                      static_cast<uint64_t>(GetCurrentThreadId()))}},
#else
      {"_pid", Argument_value{std::to_string(static_cast<uint64_t>(getpid()))}},
#endif
  };
}

Session_impl::Session_connect_timeout_scope_guard::
    Session_connect_timeout_scope_guard(Session_impl *parent)
    : m_parent{parent}, m_start_time{std::chrono::steady_clock::now()} {
  m_handler_id = m_parent->get_protocol().add_send_message_handler(
      [this](xcl::XProtocol *, const xcl::XProtocol::Client_message_type_id,
             const xcl::XProtocol::Message &) -> xcl::Handler_result {
        const auto timeout =
            m_parent->m_context->m_connection_config.m_timeout_session_connect;
        // Infinite timeout, do not set message handler
        if (timeout < 0) return Handler_result::Continue;

        auto &connection = m_parent->get_protocol().get_connection();
        const auto delta =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - m_start_time)
                .count();
        const auto new_timeout = (timeout - delta) / 1000;
        connection.set_write_timeout(
            details::make_vio_timeout((delta > timeout) ? 0 : new_timeout));
        connection.set_read_timeout(
            details::make_vio_timeout((delta > timeout) ? 0 : new_timeout));
        return Handler_result::Continue;
      });
}

Session_impl::Session_connect_timeout_scope_guard::
    ~Session_connect_timeout_scope_guard() {
  m_parent->get_protocol().remove_send_message_handler(m_handler_id);
  auto &connection = m_parent->get_protocol().get_connection();
  const auto read_timeout =
      m_parent->m_context->m_connection_config.m_timeout_read;
  connection.set_read_timeout(
      details::make_vio_timeout((read_timeout < 0) ? -1 : read_timeout / 1000));
  const auto write_timeout =
      m_parent->m_context->m_connection_config.m_timeout_write;
  connection.set_write_timeout(details::make_vio_timeout(
      (write_timeout < 0) ? -1 : write_timeout / 1000));
}

static void initialize_xmessages() {
  /* Workaround for initialization of protobuf data.
     Call default_instance for first msg from every
     protobuf file.

     This should have be changed to a proper fix.
   */
  PolarXRPC::ServerMessages::default_instance();
  PolarXRPC::Sql::StmtExecute::default_instance();
  PolarXRPC::Session::AuthenticateStart::default_instance();
  PolarXRPC::Resultset::ColumnMetaData::default_instance();
  PolarXRPC::Notice::Warning::default_instance();
  PolarXRPC::Expr::Expr::default_instance();
  PolarXRPC::Expect::Open::default_instance();
  PolarXRPC::Datatypes::Any::default_instance();
  PolarXRPC::Connection::Capabilities::default_instance();
}

std::unique_ptr<XSession> create_session(const char *host, const uint16_t port,
                                         const char *user, const char *pass,
                                         const char *schema,
                                         XError *out_error) {
  initialize_xmessages();

  auto result = create_session();
  auto error = result->connect(host, port, user, pass, schema);

  if (error) {
    if (nullptr != out_error) *out_error = error;
    result.reset();
  }

  return result;
}

std::unique_ptr<XSession> create_session() {
  initialize_xmessages();

  std::unique_ptr<XSession> result{new Session_impl()};

  return result;
}

}  // namespace xcl
