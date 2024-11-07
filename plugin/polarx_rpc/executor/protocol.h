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


#pragma once

#include "sql/protocol_callback.h"

#include "../coders/command_delegate.h"

#include "meta.h"

/*
 * This class is to send MySQL format data to network using X Protocol.
 *
 * X plugin has its own protocol and network code in
 * Streaming_command_delegate, Protocol_encoder and Protocol_flusher, but these
 * code is driven by MySQL server like THD::send_result_metadata and
 * THD::send_result_set_row in a callback way.
 *
 * If we want to override MySQL server, we have to override the network layer.
 *
 */
namespace rpc_executor {
class Protocol {
 public:
  Protocol(polarx_rpc::CcommandDelegate *deleg)
      : xprotocol_(deleg->callbacks(), CS_BINARY_REPRESENTATION, deleg) {}

  int write_metadata(InternalDataSet &dataset);
  int write_row(InternalDataSet &dataset);
  int send_and_flush();

 private:
  // vio and socket in wrapped in xprotocol
  Protocol_callback xprotocol_;
};
}  // namespace rpc_executor
