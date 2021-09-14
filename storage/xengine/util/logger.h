/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2015, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#pragma once

#if 0  // GCOV
#include "mysql/components/services/log_builtins.h"
#include <log.h>
#include <sstream>
#include <string>

namespace myx {

class Xdb_logger : public xengine::util::Logger {
public:
  void Logv(const xengine::util::InfoLogLevel log_level, const char *format,
            va_list ap) override {
    DBUG_ASSERT(format != nullptr);

    enum loglevel mysql_log_level;

    if (m_logger) {
      m_logger->Logv(log_level, format, ap);
    }

    if (log_level < GetInfoLogLevel()) {
      return;
    }

    if (log_level >= xengine::util::InfoLogLevel::ERROR_LEVEL) {
      mysql_log_level = ERROR_LEVEL;
    } else if (log_level >= xengine::util::InfoLogLevel::WARN_LEVEL) {
      mysql_log_level = WARNING_LEVEL;
    } else {
      mysql_log_level = INFORMATION_LEVEL;
    }

    // log to MySQL
    //std::string f("LibXEngine:");
    //f.append(format);

        // log to MySQL
    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    LogErr(mysql_log_level, 0, "%s", buffer);
    //abort(); //by beilou to process later
    //error_log_print(mysql_log_level, 0,  ap);
  }

  void Logv(const char *format, va_list ap) override {
    DBUG_ASSERT(format != nullptr);
    // If no level is specified, it is by default at information level
    Logv(xengine::util::InfoLogLevel::INFO_LEVEL, format, ap);
  }

  void SetXENGINELogger(const std::shared_ptr<xengine::util::Logger> logger) {
    m_logger = logger;
  }

  void SetInfoLogLevel(const xengine::util::InfoLogLevel log_level) override {
    xengine::util::Logger::SetInfoLogLevel(log_level);
    if (m_logger != nullptr) {
      m_logger->SetInfoLogLevel(log_level);
    }
  }

private:
  std::shared_ptr<xengine::util::Logger> m_logger;
};

} // namespace myx
#endif  // GCOV

void mysql_set_xengine_info_log_level(ulong);
bool mysql_reinit_xengine_log();
