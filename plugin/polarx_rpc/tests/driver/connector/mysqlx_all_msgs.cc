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

#include "mysqlx_all_msgs.h"

#include "../../client/mysqlxclient/xmessage.h"

Message_by_full_name server_msgs_by_full_name;
Message_by_full_name client_msgs_by_full_name;

Message_server_by_name server_msgs_by_name;
Message_client_by_name client_msgs_by_name;

Message_server_by_id server_msgs_by_id;
Message_client_by_id client_msgs_by_id;

static struct init_message_factory {
  template <class C>
  static xcl::XProtocol::Message *create() {
    return new C();
  }

  template <typename T, typename Message_type_id>
  void server_message(Message_type_id id, const std::string &name,
                      const std::string &full_name) {
    server_msgs_by_name[name] = std::make_pair(&create<T>, id);
    server_msgs_by_id[id] = std::make_pair(&create<T>, name);

    if (!full_name.empty()) server_msgs_by_full_name[full_name] = name;
  }

  template <typename T, typename Message_type_id>
  void client_message(Message_type_id id, const std::string &name,
                      const std::string &full_name) {
    client_msgs_by_name[name] = std::make_pair(&create<T>, id);
    client_msgs_by_id[id] = std::make_pair(&create<T>, name);

    if (!full_name.empty()) client_msgs_by_full_name[full_name] = name;
  }

  init_message_factory() {
    server_message<Polarx::Connection::Capabilities>(
        Polarx::ServerMessages::CONN_CAPABILITIES, "CONN_CAPABILITIES",
        "Polarx.Connection.Capabilities");
    server_message<Polarx::Session::AuthenticateContinue>(
        Polarx::ServerMessages::SESS_AUTHENTICATE_CONTINUE,
        "SESS_AUTHENTICATE_CONTINUE", "Polarx.Session.AuthenticateContinue");
    server_message<Polarx::Error>(Polarx::ServerMessages::ERROR, "ERROR",
                                  "Polarx.Error");
    server_message<Polarx::Notice::Frame>(Polarx::ServerMessages::NOTICE,
                                          "NOTICE", "Polarx.Notice.Frame");
    server_message<Polarx::Ok>(Polarx::ServerMessages::OK, "OK", "Polarx.Ok");
    server_message<Polarx::Resultset::ColumnMetaData>(
        Polarx::ServerMessages::RESULTSET_COLUMN_META_DATA,
        "RESULTSET_COLUMN_META_DATA", "Polarx.Resultset.ColumnMetaData");
    server_message<Polarx::Resultset::FetchDone>(
        Polarx::ServerMessages::RESULTSET_FETCH_DONE, "RESULTSET_FETCH_DONE",
        "Polarx.Resultset.FetchDone");
    server_message<Polarx::Resultset::FetchDoneMoreOutParams>(
        Polarx::ServerMessages::RESULTSET_FETCH_DONE_MORE_OUT_PARAMS,
        "RESULTSET_FETCH_DONE_MORE_OUT_PARAMS",
        "Polarx.Resultset.FetchDoneMoreOutParams");
    server_message<Polarx::Resultset::FetchDoneMoreResultsets>(
        Polarx::ServerMessages::RESULTSET_FETCH_DONE_MORE_RESULTSETS,
        "RESULTSET_FETCH_DONE_MORE_RESULTSETS",
        "Polarx.Resultset.FetchDoneMoreResultsets");
    server_message<Polarx::Resultset::Row>(
        Polarx::ServerMessages::RESULTSET_ROW, "RESULTSET_ROW",
        "Polarx.Resultset.Row");
    server_message<Polarx::Session::AuthenticateOk>(
        Polarx::ServerMessages::SESS_AUTHENTICATE_OK, "SESS_AUTHENTICATE_OK",
        "Polarx.Session.AuthenticateOk");
    server_message<Polarx::Sql::StmtExecuteOk>(
        Polarx::ServerMessages::SQL_STMT_EXECUTE_OK, "SQL_STMT_EXECUTE_OK",
        "Polarx.Sql.StmtExecuteOk");

    client_message<Polarx::Connection::CapabilitiesGet>(
        Polarx::ClientMessages::CON_CAPABILITIES_GET, "CON_CAPABILITIES_GET",
        "Polarx.Connection.CapabilitiesGet");
    client_message<Polarx::Connection::CapabilitiesSet>(
        Polarx::ClientMessages::CON_CAPABILITIES_SET, "CON_CAPABILITIES_SET",
        "Polarx.Connection.CapabilitiesSet");
    client_message<Polarx::Connection::Close>(Polarx::ClientMessages::CON_CLOSE,
                                              "CON_CLOSE",
                                              "Polarx.Connection.Close");
    client_message<Polarx::Expect::Close>(Polarx::ClientMessages::EXPECT_CLOSE,
                                          "EXPECT_CLOSE",
                                          "Polarx.Expect.Close");
    client_message<Polarx::Expect::Open>(Polarx::ClientMessages::EXPECT_OPEN,
                                         "EXPECT_OPEN", "Polarx.Expect.Open");
    client_message<Polarx::Session::AuthenticateContinue>(
        Polarx::ClientMessages::SESS_AUTHENTICATE_CONTINUE,
        "SESS_AUTHENTICATE_CONTINUE", "Polarx.Session.AuthenticateContinue");
    client_message<Polarx::Session::AuthenticateStart>(
        Polarx::ClientMessages::SESS_AUTHENTICATE_START,
        "SESS_AUTHENTICATE_START", "Polarx.Session.AuthenticateStart");
    client_message<Polarx::Session::Close>(Polarx::ClientMessages::SESS_CLOSE,
                                           "SESS_CLOSE",
                                           "Polarx.Session.Close");
    client_message<Polarx::Session::Reset>(Polarx::ClientMessages::SESS_RESET,
                                           "SESS_RESET",
                                           "Polarx.Session.Reset");
    client_message<Polarx::Sql::StmtExecute>(
        Polarx::ClientMessages::EXEC_SQL, "EXEC_SQL",
        "Polarx.Sql.StmtExecute");
    client_message<Polarx::ExecPlan::ExecPlan>(
        Polarx::ClientMessages::EXEC_PLAN_READ, "EXEC_PLAN",
        "Polarx.ExecPlan.ExecPlan");
    client_message<Polarx::ExecPlan::GetTSO>(Polarx::ClientMessages::GET_TSO, "GET_TSO",
                                   "Polarx.ExecPlan.GetTSO");
    server_message<Polarx::ExecPlan::ResultTSO>(Polarx::ServerMessages::RESULTSET_TSO,
                                      "RESULT_TSO", "Polarx.ExecPlan.ResultTSO");
    client_message<Polarx::Session::NewSession>(
        Polarx::ClientMessages::SESS_NEW, "SESS_NEW",
        "Polarx.Session.NewSession");
    client_message<Polarx::Session::KillSession>(
        Polarx::ClientMessages::SESS_KILL, "SESS_KILL",
        "Polarx.Session.KillSession");
    client_message<Polarx::Sql::TokenOffer>(Polarx::ClientMessages::TOKEN_OFFER,
                                            "TOKEN_OFFER",
                                            "Polarx.Sql.TokenOffer");
    server_message<Polarx::Resultset::TokenDone>(
        Polarx::ServerMessages::RESULTSET_TOKEN_DONE, "RESULTSET_TOKEN_DONE",
        "Polarx.Resultset.TokenDone");
  }
} init_message_factory;
