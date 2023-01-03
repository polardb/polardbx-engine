/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


namespace ngs{

/** Galaxy X-protocol */
bool Protocol_decoder::read_header(gx::GSession_id *gsession_id,
                                   gx::GVersion *gversion,
                                   uint8_t *message_type,
                                   uint32_t *message_size,
                                   xpl::iface::Waiting_for_io *wait_for_io) {

  const uint8_t gheader_size = gx::GSESSION_SIZE + gx::GVERSION_SIZE + 4;
  int header_copied = 0;
  int input_size = 0;
  const uint8_t *input = nullptr;
  uint8_t buffer[gheader_size];

  int copy_from_input = 0;
  const bool needs_idle_check = wait_for_io->has_to_report_idle_waiting();

  const uint64_t io_read_timeout =
      needs_idle_check ? k_on_idle_timeout_value : m_wait_timeout_in_ms;

  m_vio->set_timeout_in_ms(Vio_interface::Direction::k_read, io_read_timeout);

  uint64_t total_timeout = 0;

  m_vio_input_stream.mark_vio_as_idle();

  while (header_copied < gheader_size) {
    if (needs_idle_check) wait_for_io->on_idle_or_before_read();

    if (!m_vio_input_stream.Next((const void **)&input, &input_size)) {
      int out_error_code = 0;
      if (m_vio_input_stream.was_io_error(&out_error_code)) {
        if ((out_error_code == SOCKET_ETIMEDOUT ||
             out_error_code == SOCKET_EAGAIN) &&
            needs_idle_check) {
          total_timeout += k_on_idle_timeout_value;
          if (total_timeout < m_wait_timeout_in_ms) {
            m_vio_input_stream.clear_io_error();

            continue;
          }
        }
      }
      return false;
    }

    copy_from_input = std::min(input_size, gheader_size - header_copied);
    std::copy(input, input + copy_from_input, buffer + header_copied);
    header_copied += copy_from_input;
  }

  google::protobuf::io::CodedInputStream::ReadLittleEndian64FromArray(
      buffer, gsession_id);

  *gversion = buffer[gx::GSESSION_SIZE];

  google::protobuf::io::CodedInputStream::ReadLittleEndian32FromArray(
      buffer + gx::GSESSION_SIZE + gx::GVERSION_SIZE, message_size);

  m_vio_input_stream.mark_vio_as_active();

  if (*message_size > 0) {
    if (input_size == copy_from_input) {
      copy_from_input = 0;
      m_vio->set_timeout_in_ms(Vio_interface::Direction::k_read,
                               m_read_timeout_in_ms);

      if (!m_vio_input_stream.Next((const void **)&input, &input_size)) {
        return false;
      }
    }

    *message_type = input[copy_from_input];
    ++copy_from_input;
  }

  m_vio_input_stream.BackUp(input_size - copy_from_input);

  return true;
}

}  // namespace ngs
