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

#ifndef PLUGIN_X_NGS_INCLUDE_NGS_PROTOCOL_MESSAGE_H_
#define PLUGIN_X_NGS_INCLUDE_NGS_PROTOCOL_MESSAGE_H_

#include "plugin/x/ngs/include/ngs/memory.h"
#include "plugin/x/ngs/include/ngs/protocol/protocol_protobuf.h"

#include "plugin/x/ngs/include/ngs/galaxy_session.h"

namespace ngs {

#ifdef USE_MYSQLX_FULL_PROTO
typedef ::google::protobuf::Message Message;
#else
typedef ::google::protobuf::MessageLite Message;
#endif

class Message_request {
 public:
  ~Message_request() { free_msg(); }

  void reset(const uint8 message_type = 0, Message *message = nullptr,
             const bool must_be_deleted = false) {
    free_msg();

    m_message = message;
    m_message_type = message_type;
    m_must_be_deleted = must_be_deleted;
  }

  Message *get_message() const { return m_message; }
  uint8 get_message_type() const { return m_message_type; }
  const gx::GRequest &get_galaxy_request() const { return grequest; }
  gx::GRequest &get_galaxy_request() { return grequest; }

  bool has_message() const { return nullptr != m_message; }

  inline ngs::Memory_instrumented<ngs::Message>::Unique_ptr move_out_message() {
    if (m_message != nullptr) {
      if (m_must_be_deleted) {
        ngs::Memory_instrumented<ngs::Message>::Unique_ptr ptr(m_message);
        m_message = nullptr;
        m_must_be_deleted = false;
        return ptr;
      } else if (Mysqlx::ClientMessages::GALAXY_STMT_EXECUTE ==
                 m_message_type) {
        // Copy one.
        ngs::Memory_instrumented<ngs::Message>::Unique_ptr ptr(
            allocate_object<Mysqlx::Sql::GalaxyStmtExecute>(
                static_cast<const Mysqlx::Sql::GalaxyStmtExecute &>(
                    *m_message)));
        return ptr;
      } else if (Mysqlx::ClientMessages::EXPECT_OPEN == m_message_type) {
        // Copy one.
        ngs::Memory_instrumented<ngs::Message>::Unique_ptr ptr(
            allocate_object<Mysqlx::Expect::Open>(
                static_cast<const Mysqlx::Expect::Open &>(*m_message)));
        return ptr;
      }
    }
    return {};
  }

 private:
  void free_msg() {
    if (m_must_be_deleted) {
      if (m_message) free_object(m_message);
      m_must_be_deleted = false;
    }
  }

  Message *m_message = nullptr;
  uint8 m_message_type{0};
  bool m_must_be_deleted{false};

  gx::GRequest grequest;
};

}  // namespace ngs

#endif  // PLUGIN_X_NGS_INCLUDE_NGS_PROTOCOL_MESSAGE_H_
