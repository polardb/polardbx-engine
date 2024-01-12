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
