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
    server_message<PolarXRPC::Connection::Capabilities>(
        PolarXRPC::ServerMessages::CONN_CAPABILITIES, "CONN_CAPABILITIES",
        "PolarXRPC.Connection.Capabilities");
    server_message<PolarXRPC::Session::AuthenticateContinue>(
        PolarXRPC::ServerMessages::SESS_AUTHENTICATE_CONTINUE,
        "SESS_AUTHENTICATE_CONTINUE", "PolarXRPC.Session.AuthenticateContinue");
    server_message<PolarXRPC::Error>(PolarXRPC::ServerMessages::ERROR, "ERROR",
                                     "PolarXRPC.Error");
    server_message<PolarXRPC::Notice::Frame>(
        PolarXRPC::ServerMessages::NOTICE, "NOTICE", "PolarXRPC.Notice.Frame");
    server_message<PolarXRPC::Ok>(PolarXRPC::ServerMessages::OK, "OK",
                                  "PolarXRPC.Ok");
    server_message<PolarXRPC::Resultset::ColumnMetaData>(
        PolarXRPC::ServerMessages::RESULTSET_COLUMN_META_DATA,
        "RESULTSET_COLUMN_META_DATA", "PolarXRPC.Resultset.ColumnMetaData");
    server_message<PolarXRPC::Resultset::FetchDone>(
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE, "RESULTSET_FETCH_DONE",
        "PolarXRPC.Resultset.FetchDone");
    server_message<PolarXRPC::Resultset::FetchDoneMoreOutParams>(
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_OUT_PARAMS,
        "RESULTSET_FETCH_DONE_MORE_OUT_PARAMS",
        "PolarXRPC.Resultset.FetchDoneMoreOutParams");
    server_message<PolarXRPC::Resultset::FetchDoneMoreResultsets>(
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_RESULTSETS,
        "RESULTSET_FETCH_DONE_MORE_RESULTSETS",
        "PolarXRPC.Resultset.FetchDoneMoreResultsets");
    server_message<PolarXRPC::Resultset::Row>(
        PolarXRPC::ServerMessages::RESULTSET_ROW, "RESULTSET_ROW",
        "PolarXRPC.Resultset.Row");
    server_message<PolarXRPC::Session::AuthenticateOk>(
        PolarXRPC::ServerMessages::SESS_AUTHENTICATE_OK, "SESS_AUTHENTICATE_OK",
        "PolarXRPC.Session.AuthenticateOk");
    server_message<PolarXRPC::Sql::StmtExecuteOk>(
        PolarXRPC::ServerMessages::SQL_STMT_EXECUTE_OK, "SQL_STMT_EXECUTE_OK",
        "PolarXRPC.Sql.StmtExecuteOk");

    client_message<PolarXRPC::Connection::CapabilitiesGet>(
        PolarXRPC::ClientMessages::CON_CAPABILITIES_GET, "CON_CAPABILITIES_GET",
        "PolarXRPC.Connection.CapabilitiesGet");
    client_message<PolarXRPC::Connection::CapabilitiesSet>(
        PolarXRPC::ClientMessages::CON_CAPABILITIES_SET, "CON_CAPABILITIES_SET",
        "PolarXRPC.Connection.CapabilitiesSet");
    client_message<PolarXRPC::Connection::Close>(
        PolarXRPC::ClientMessages::CON_CLOSE, "CON_CLOSE",
        "PolarXRPC.Connection.Close");
    client_message<PolarXRPC::Expect::Close>(
        PolarXRPC::ClientMessages::EXPECT_CLOSE, "EXPECT_CLOSE",
        "PolarXRPC.Expect.Close");
    client_message<PolarXRPC::Expect::Open>(
        PolarXRPC::ClientMessages::EXPECT_OPEN, "EXPECT_OPEN",
        "PolarXRPC.Expect.Open");
    client_message<PolarXRPC::Session::AuthenticateContinue>(
        PolarXRPC::ClientMessages::SESS_AUTHENTICATE_CONTINUE,
        "SESS_AUTHENTICATE_CONTINUE", "PolarXRPC.Session.AuthenticateContinue");
    client_message<PolarXRPC::Session::AuthenticateStart>(
        PolarXRPC::ClientMessages::SESS_AUTHENTICATE_START,
        "SESS_AUTHENTICATE_START", "PolarXRPC.Session.AuthenticateStart");
    client_message<PolarXRPC::Session::Close>(
        PolarXRPC::ClientMessages::SESS_CLOSE, "SESS_CLOSE",
        "PolarXRPC.Session.Close");
    client_message<PolarXRPC::Session::Reset>(
        PolarXRPC::ClientMessages::SESS_RESET, "SESS_RESET",
        "PolarXRPC.Session.Reset");
    client_message<PolarXRPC::Sql::StmtExecute>(
        PolarXRPC::ClientMessages::EXEC_SQL, "EXEC_SQL",
        "PolarXRPC.Sql.StmtExecute");
    client_message<PolarXRPC::ExecPlan::ExecPlan>(
        PolarXRPC::ClientMessages::EXEC_PLAN_READ, "EXEC_PLAN",
        "PolarXRPC.ExecPlan.ExecPlan");
    client_message<PolarXRPC::ExecPlan::GetTSO>(
        PolarXRPC::ClientMessages::GET_TSO, "GET_TSO",
        "PolarXRPC.ExecPlan.GetTSO");
    server_message<PolarXRPC::ExecPlan::ResultTSO>(
        PolarXRPC::ServerMessages::RESULTSET_TSO, "RESULT_TSO",
        "PolarXRPC.ExecPlan.ResultTSO");
    client_message<PolarXRPC::Session::NewSession>(
        PolarXRPC::ClientMessages::SESS_NEW, "SESS_NEW",
        "PolarXRPC.Session.NewSession");
    client_message<PolarXRPC::Session::KillSession>(
        PolarXRPC::ClientMessages::SESS_KILL, "SESS_KILL",
        "PolarXRPC.Session.KillSession");
    client_message<PolarXRPC::Sql::TokenOffer>(
        PolarXRPC::ClientMessages::TOKEN_OFFER, "TOKEN_OFFER",
        "PolarXRPC.Sql.TokenOffer");
    server_message<PolarXRPC::Resultset::TokenDone>(
        PolarXRPC::ServerMessages::RESULTSET_TOKEN_DONE, "RESULTSET_TOKEN_DONE",
        "PolarXRPC.Resultset.TokenDone");
    client_message<PolarXRPC::ExecPlan::AutoSp>(
        PolarXRPC::ClientMessages::AUTO_SP, "AUTO_SP", "PolarXRPC.Sql.AutoSp");

    server_message<PolarXRPC::PhysicalBackfill::GetFileInfoOperator>(
        PolarXRPC::ServerMessages::RESULTSET_GET_FILE_INFO_OK,
        "RESULTSET_GET_FILE_INFO_OK",
        "PolarXRPC.PhysicalBackfill.GetFileInfoOperator");

    server_message<PolarXRPC::PhysicalBackfill::TransferFileDataOperator>(
        PolarXRPC::ServerMessages::RESULTSET_TRANSFER_FILE_DATA_OK,
        "RESULTSET_TRANSFER_FILE_DATA_OK",
        "PolarXRPC.PhysicalBackfill.TransferFileDataOperator");

    server_message<PolarXRPC::PhysicalBackfill::FileManageOperatorResponse>(
        PolarXRPC::ServerMessages::RESULTSET_FILE_MANAGE_OK,
        "RESULTSET_FILE_MANAGE_OK",
        "PolarXRPC.PhysicalBackfill.FileManageOperatorResponse");

    client_message<PolarXRPC::PhysicalBackfill::GetFileInfoOperator>(
        PolarXRPC::ClientMessages::FILE_OPERATION_GET_FILE_INFO,
        "FILE_OPERATION_GET_FILE_INFO",
        "PolarXRPC.PhysicalBackfill.GetFileInfoOperator");
    client_message<PolarXRPC::PhysicalBackfill::FileManageOperator>(
        PolarXRPC::ClientMessages::FILE_OPERATION_FILE_MANAGE,
        "FILE_OPERATION_FILE_MANAGE",
        "PolarXRPC.PhysicalBackfill.FileManageOperator");
    client_message<PolarXRPC::PhysicalBackfill::TransferFileDataOperator>(
        PolarXRPC::ClientMessages::FILE_OPERATION_TRANSFER_FILE_DATA,
        "FILE_OPERATION_TRANSFER_FILE_DATA",
        "PolarXRPC.PhysicalBackfill.TransferFileDataOperator");
  }
} init_message_factory;
