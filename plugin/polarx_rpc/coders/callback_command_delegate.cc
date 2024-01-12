//
// Created by zzy on 2022/8/31.
//

#include <cstddef>
#include <string>

#include "callback_command_delegate.h"

namespace polarx_rpc {

CcallbackCommandDelegate::Field_value::Field_value()
    : is_unsigned(false), is_string(false) {}

CcallbackCommandDelegate::Field_value::Field_value(const Field_value &other)
    : value(other.value),
      is_unsigned(other.is_unsigned),
      is_string(other.is_string) {
  if (other.is_string) value.v_string = new std::string(*other.value.v_string);
}

CcallbackCommandDelegate::Field_value::Field_value(const longlong &num,
                                                   bool unsign) {
  value.v_long = num;
  is_unsigned = unsign;
  is_string = false;
}

CcallbackCommandDelegate::Field_value::Field_value(const double num)
    : is_unsigned(false) {
  value.v_double = num;
  is_string = false;
}

CcallbackCommandDelegate::Field_value::Field_value(const decimal_t &decimal)
    : is_unsigned(false) {
  value.v_decimal = decimal;
  is_string = false;
}

CcallbackCommandDelegate::Field_value::Field_value(const MYSQL_TIME &time)
    : is_unsigned(false) {
  value.v_time = time;
  is_string = false;
}

CcallbackCommandDelegate::Field_value::Field_value(const char *str,
                                                   size_t length)
    : is_unsigned(false) {
  value.v_string = new std::string(str, length);
  is_string = true;
}

CcallbackCommandDelegate::Field_value::~Field_value() {
  if (is_string) delete value.v_string;
}

CcallbackCommandDelegate::Row_data::~Row_data() { clear(); }

void CcallbackCommandDelegate::Row_data::clear() {
  for (auto f : fields) delete f;
  fields.clear();
}

CcallbackCommandDelegate::Row_data::Row_data(const Row_data &other) {
  clone_fields(other);
}

CcallbackCommandDelegate::Row_data &
CcallbackCommandDelegate::Row_data::operator=(const Row_data &other) {
  if (&other != this) {
    clear();
    clone_fields(other);
  }

  return *this;
}

void CcallbackCommandDelegate::Row_data::clone_fields(const Row_data &other) {
  fields.reserve(other.fields.size());

  for (auto f : other.fields)
    this->fields.push_back(f ? new Field_value(*f) : nullptr);
}

CcallbackCommandDelegate::CcallbackCommandDelegate() : m_current_row(nullptr) {}

CcallbackCommandDelegate::CcallbackCommandDelegate(Start_row_callback start_row,
                                                   End_row_callback end_row)
    : m_start_row(start_row), m_end_row(end_row), m_current_row(nullptr) {}

void CcallbackCommandDelegate::set_callbacks(Start_row_callback start_row,
                                             End_row_callback end_row) {
  m_start_row = start_row;
  m_end_row = end_row;
}

void CcallbackCommandDelegate::reset() {
  m_current_row = nullptr;
  CcommandDelegate::reset();
}

int CcallbackCommandDelegate::start_row() {
  if (m_start_row) {
    m_current_row = m_start_row();
    if (!m_current_row) return true;
  } else
    m_current_row = nullptr;
  return false;
}

int CcallbackCommandDelegate::end_row() {
  if (m_end_row && !m_end_row(m_current_row)) return true;
  return false;
}

void CcallbackCommandDelegate::abort_row() {}

ulong CcallbackCommandDelegate::get_client_capabilities() {
  return CLIENT_DEPRECATE_EOF;
}

/****** Getting data ******/
int CcallbackCommandDelegate::get_null() {
  try {
    if (m_current_row) m_current_row->fields.push_back(nullptr);
  } catch (std::exception &ignore) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_integer(longlong value) {
  try {
    if (m_current_row) m_current_row->fields.push_back(new Field_value(value));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_longlong(longlong value,
                                           uint32_t unsigned_flag) {
  try {
    if (m_current_row)
      m_current_row->fields.push_back(new Field_value(value, unsigned_flag));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_decimal(const decimal_t *value) {
  try {
    if (m_current_row) m_current_row->fields.push_back(new Field_value(*value));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_double(double value, uint32) {
  try {
    if (m_current_row) m_current_row->fields.push_back(new Field_value(value));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_date(const MYSQL_TIME *value) {
  try {
    if (m_current_row) m_current_row->fields.push_back(new Field_value(*value));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_time(const MYSQL_TIME *value, uint) {
  try {
    if (m_current_row) m_current_row->fields.push_back(new Field_value(*value));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_datetime(const MYSQL_TIME *value, uint) {
  try {
    if (m_current_row) m_current_row->fields.push_back(new Field_value(*value));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

int CcallbackCommandDelegate::get_string(const char *const value, size_t length,
                                         const CHARSET_INFO *const) {
  try {
    if (m_current_row)
      m_current_row->fields.push_back(new Field_value(value, length));
  } catch (std::exception &e) {
    return true;
  }
  return false;
}

}  // namespace polarx_rpc
