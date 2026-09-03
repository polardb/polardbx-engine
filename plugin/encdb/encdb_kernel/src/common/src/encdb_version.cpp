/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include "encdb_version.h"
#include <string>
#include "encdb_common_exception.h"  // for VersionMismatchException

namespace encdb {

Version::Version(const std::string &version_str) {
  if (3 != sscanf(version_str.c_str(), "%d.%d.%d", &major, &minor, &patch)) {
    throw VersionMismatchException(
        "Wrong client version format: " + version_str + ". Should be x.y.z");
  }
}

bool Version::operator<(const Version &other) const noexcept {
  return major < other.major || minor < other.minor || patch < other.patch;
}

bool Version::operator==(const Version &other) const noexcept {
  return major == other.major && minor == other.minor && patch == other.patch;
}

}  // namespace encdb
