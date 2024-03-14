//
// Created by zzy on 2022/8/8.
//

#pragma once

#include <cstring>
#include <fstream>
#include <map>

#include "../common_define.h"

namespace polarx_rpc {

class CcpuInfo final {
  NO_CONSTRUCTOR(CcpuInfo)
  NO_COPY_MOVE(CcpuInfo)

 public:
  struct cpu_info_t final {
    int processor;
    int physical_id;
    int core_id;

    inline bool operator<(const cpu_info_t &another) const {
      if (physical_id != another.physical_id)
        return physical_id < another.physical_id;
      if (core_id != another.core_id) return core_id < another.core_id;
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
    /// ::popen("grep -E \"processor|physical id|core id\" /proc/cpuinfo", "r");
    std::ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo.is_open()) return {};
    std::map<int, cpu_info_t> result;
    std::string line;
    auto processor = -1, physical_id = -1, core_id = -1;
    while (std::getline(cpuinfo, line)) {
      /// ignore unnecessary info
      if (line.find("processor") != 0 && line.find("physical id") != 0 &&
          line.find("core id") != 0)
        continue;

      /// build cpu info
      const auto buf = line.c_str();
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
        if (i != nullptr) physical_id = ::atoi(i + 1);
      } else if (processor >= 0 && core_id < 0 &&
                 0 == ::strncmp("core id", buf, 7)) {
        auto i = ::strchr(buf + 7, ':');
        if (i != nullptr) core_id = ::atoi(i + 1);
      }

      if (processor >= 0 && physical_id >= 0 && core_id >= 0) {
        result.emplace(processor, cpu_info_t{processor, physical_id, core_id});
        processor = -1;
        physical_id = -1;
        core_id = -1;
      }
    }
    cpuinfo.close();
    return result;
  }
};

}  // namespace polarx_rpc
