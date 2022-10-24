//
// Created by zzy on 2022/8/8.
//

#pragma once

#include <cstdio>
#include <cstring>
#include <map>

#include "../common_define.h"

namespace polarx_rpc {

class CcpuInfo final {
  NO_CONSTRUCTOR(CcpuInfo);
  NO_COPY_MOVE(CcpuInfo);

public:
  struct cpu_info_t final {
    int processor;
    int physical_id;
    int core_id;

    inline bool operator<(const cpu_info_t &another) const {
      if (physical_id != another.physical_id)
        return physical_id < another.physical_id;
      if (core_id != another.core_id)
        return core_id < another.core_id;
      return processor < another.processor;
    }

    inline bool operator==(const cpu_info_t &another) const {
      return processor == another.processor && core_id == another.core_id &&
             physical_id == another.physical_id;
    }

    inline bool operator!=(const cpu_info_t &another) const {
      return processor != another.processor || core_id != another.core_id ||
             physical_id != another.physical_id;
    }
  };

  static std::map<int, cpu_info_t> get_cpu_info() {
    FILE *cmd =
        ::popen("grep -E \"processor|physical id|core id\" /proc/cpuinfo", "r");
    if (cmd == nullptr)
      return {};
    std::map<int, cpu_info_t> result;
    char buf[0x100]; /// 256
    auto processor = -1, physical_id = -1, core_id = -1;
    while (::fgets(buf, sizeof(buf) - 1, cmd) != nullptr) {
      buf[sizeof(buf) - 1] = '\0';
      if (0 == ::strncmp("processor", buf, 9)) {
        auto i = ::strchr(buf + 9, ':');
        if (i != nullptr) {
          /// record new one
          processor = ::atoi(i + 1);
          physical_id = -1;
          core_id = -1;
        }
      } else if (processor >= 0 && physical_id < 0 &&
                 0 == ::strncmp("physical id", buf, 11)) {
        auto i = ::strchr(buf + 11, ':');
        if (i != nullptr)
          physical_id = ::atoi(i + 1);
      } else if (processor >= 0 && core_id < 0 &&
                 0 == ::strncmp("core id", buf, 7)) {
        auto i = ::strchr(buf + 7, ':');
        if (i != nullptr)
          core_id = ::atoi(i + 1);
      }

      if (processor >= 0 && physical_id >= 0 && core_id >= 0) {
        result.emplace(std::make_pair(
            processor, cpu_info_t{processor, physical_id, core_id}));
        processor = -1;
        physical_id = -1;
        core_id = -1;
      }
    }
    pclose(cmd);
    return result;
  }
};

} // namespace polarx_rpc
