/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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
#ifndef XPAXOS_ERR_H
#define XPAXOS_ERR_H

#include <sstream>

#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/log_shared.h"
#include "mysqld_error.h"

namespace xp {

/** The class logger is the base class of all the error log related classes.
It contains a std::ostringstream object.  The main purpose of this class is
to forward operator<< to the underlying std::ostringstream object.  Do not
use this class directly, instead use one of the derived classes. */
class logger {
 public:
  /** Destructor */
  virtual ~logger();

  /** Format an error message.
  @param[in]    err             Error code from errmsg-*.txt.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  logger &log(int err, Args &&...args) {
    assert(m_err == ER_XP_0);

    m_err = err;

    m_oss << msg(err, std::forward<Args>(args)...);

    return (*this);
  }

  template <typename T>
  logger &operator<<(const T &rhs) {
    m_oss << rhs;
    return (*this);
  }

  /** Write the given buffer to the internal string stream object.
  @param[in]    buf             the buffer contents to log.
  @param[in]    count           the length of the buffer buf.
  @return the output stream into which buffer was written. */
  std::ostream &write(const char *buf, std::streamsize count) {
    return (m_oss.write(buf, count));
  }

  /** Write the given buffer to the internal string stream object.
  @param[in]    buf             the buffer contents to log
  @param[in]    count           the length of the buffer buf.
  @return the output stream into which buffer was written. */
  std::ostream &write(const unsigned char *buf, std::streamsize count) {
    return (m_oss.write(reinterpret_cast<const char *>(buf), count));
  }

 public:
  /** For converting the message into a string. */
  std::ostringstream m_oss;

  /** Error code in errmsg-*.txt */
  int m_err{};

  /** Error logging level. */
  loglevel m_level{INFORMATION_LEVEL};

 protected:
  /** Format an error message.
  @param[in]    err     Error code from errmsg-*.txt.
  @param[in]    args    Variable length argument list */
  template <class... Args>
  static std::string msg(int err, Args &&...args) {
    const char *fmt = error_message_for_error_log(err);

    int ret;
    char buf[LOG_BUFF_MAX];
    ret = snprintf(buf, sizeof(buf), fmt, std::forward<Args>(args)...);

    std::string str;

    if (ret > 0 && (size_t)ret < sizeof(buf)) {
      str.append(buf);
    }

    return (str);
  }

 protected:
  /** Uses LogEvent to report the log entry, using provided message
  @param[in]    msg    message to be logged
  */
  void log_event(std::string msg);

  /** Constructor.
  @param[in]    level           Logging level
  @param[in]    err             Error message code. */
  logger(loglevel level, int err) : m_err(err), m_level(level) {
    /* Note: Dummy argument to avoid the warning:

    "format not a string literal and no format arguments"
    "[-Wformat-security]"

    The warning only kicks in if the call is of the form:

       snprintf(buf, sizeof(buf), str);
    */

    m_oss << msg(err, "");
  }

  /** Constructor.
  @param[in]    level           Logging level
  @param[in]    err             Error message code.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  explicit logger(loglevel level, int err, Args &&...args)
      : m_err(err), m_level(level) {
    m_oss << msg(err, std::forward<Args>(args)...);
  }

  /** Constructor
  @param[in]    level           Log error level */
  explicit logger(loglevel level) : m_err(ER_XP_0), m_level(level) {}
};

/** The class info is used to emit informational log messages.  It is to be
used similar to std::cout.  But the log messages will be emitted only when
the dtor is called.  The preferred usage of this class is to make use of
unnamed temporaries as follows:

info() << "The server started successfully.";

In the above usage, the temporary object will be destroyed at the end of the
statement and hence the log message will be emitted at the end of the
statement.  If a named object is created, then the log message will be emitted
only when it goes out of scope or destroyed. */
class info : public logger {
 public:
  /** Default constructor uses ER_IB_MSG_0 */
  info() : logger(INFORMATION_LEVEL) {}

  /** Constructor.
  @param[in]    err             Error code from errmsg-*.txt.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  explicit info(int err, Args &&...args)
      : logger(INFORMATION_LEVEL, err, std::forward<Args>(args)...) {}
};

/** The class warn is used to emit warnings.  Refer to the documentation of
class info for further details. */
class warn : public logger {
 public:
  /** Default constructor uses ER_IB_MSG_0 */
  warn() : logger(WARNING_LEVEL) {}

  /** Constructor.
  @param[in]    err             Error code from errmsg-*.txt.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  explicit warn(int err, Args &&...args)
      : logger(WARNING_LEVEL, err, std::forward<Args>(args)...) {}
};

/** The class error is used to emit error messages.  Refer to the
documentation of class info for further details. */
class error : public logger {
 public:
  /** Default constructor uses ER_IB_MSG_0 */
  error() : logger(ERROR_LEVEL) {}

  /** Constructor.
  @param[in]    err             Error code from errmsg-*.txt.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  explicit error(int err, Args &&...args)
      : logger(ERROR_LEVEL, err, std::forward<Args>(args)...) {}
};

class system : public logger {
 public:
  /** Default constructor uses */
  system() : logger(SYSTEM_LEVEL) {}

  /** Constructor.
  @param[in]    err             Error code from errmsg-*.txt.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  explicit system(int err, Args &&...args)
      : logger(SYSTEM_LEVEL, err, std::forward<Args>(args)...) {}
};

/** The class fatal is used to emit an error message and stop the server
by crashing it.  Use this class when MySQL server needs to be stopped
immediately.  Refer to the documentation of class info for usage details. */
class fatal : public logger {
 public:
  /** Default constructor uses
  @param[in]    location                Location that creates the fatal message.
*/
  fatal() : logger(ERROR_LEVEL) {}

  /** Constructor.
  @param[in]    location                Location that creates the fatal message.
  @param[in]    err             Error code from errmsg-*.txt.
  @param[in]    args            Variable length argument list */
  template <class... Args>
  explicit fatal(int err, Args &&...args)
      : logger(ERROR_LEVEL, err, std::forward<Args>(args)...) {}

  /** Destructor. */
  ~fatal() override;
};

}  // namespace xp

#endif
