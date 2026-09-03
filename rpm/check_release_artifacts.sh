#!/bin/sh

# Copyright (c) 2026, Alibaba and/or its affiliates. All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
# for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# Reject a staged installation that ships plugins or components declared
# TEST_ONLY. Those expose privileged services, and loading them only needs the
# INSERT privilege on mysql.component or mysql.plugin, which turns them into a
# privilege escalation path (Bug #85439771).
#
# Usage: rpm/check_release_artifacts.sh <staged-install-dir|rpm-file>
#
# The list of forbidden artifacts is derived from the TEST_ONLY declarations in
# the source tree, so it stays correct as plugins are added or removed.

set -u

target="${1:-}"
if [ -z "${target}" ]; then
  echo "usage: $0 <staged-install-dir|rpm-file>" >&2
  exit 2
fi

src_root=$(cd "$(dirname "$0")/.." && pwd)

# .so base names of every TEST_ONLY plugin and component declared in the tree.
# A component named foo builds component_foo.so; a plugin builds
# <MODULE_OUTPUT_NAME>.so, defaulting to the target name. Declarations live all
# over the tree, sql/CMakeLists.txt included, so scan every CMakeLists.txt.
list_test_only_artifacts() {
  grep -rl "TEST_ONLY" --include=CMakeLists.txt "${src_root}" \
    --exclude-dir=bu-RelWithDebInfo --exclude-dir=CMakeFiles 2>/dev/null |
  while read -r cmakefile; do
    awk '
      /MYSQL_ADD_PLUGIN\(/ || /MYSQL_ADD_COMPONENT\(/ {
        in_call = 1
        is_component = /MYSQL_ADD_COMPONENT\(/ ? 1 : 0
        test_only = 0
        out_name = ""
        line = $0
        sub(/.*MYSQL_ADD_(PLUGIN|COMPONENT)\(/, "", line)
        split(line, fields, /[ \t]+/)
        target_name = fields[1]
        gsub(/"/, "", target_name)
      }
      in_call && /TEST_ONLY/ { test_only = 1 }
      in_call && /MODULE_OUTPUT_NAME/ {
        line = $0
        sub(/.*MODULE_OUTPUT_NAME[ \t]*"?/, "", line)
        sub(/".*/, "", line)
        gsub(/[ \t]/, "", line)
        out_name = line
      }
      in_call && /\)/ {
        if (test_only && target_name != "") {
          if (out_name != "")        print out_name ".so"
          else if (is_component)     print "component_" tolower(target_name) ".so"
          else                       print tolower(target_name) ".so"
        }
        in_call = 0
      }
    ' "${cmakefile}"
  done | sort -u
}

case "${target}" in
  *.rpm)
    [ -f "${target}" ] || { echo "no such file: ${target}" >&2; exit 2; }
    shipped=$(rpm -qlp "${target}" 2>/dev/null | sed 's#.*/##' | grep '\.so$' | sort -u)
    ;;
  *)
    [ -d "${target}" ] || { echo "no such directory: ${target}" >&2; exit 2; }
    shipped=$(find "${target}" -name '*.so' | sed 's#.*/##' | sort -u)
    ;;
esac

forbidden=$(list_test_only_artifacts)
if [ -z "${forbidden}" ]; then
  echo "ERROR: found no TEST_ONLY declaration, the check would pass vacuously" >&2
  exit 2
fi

if [ -z "${shipped}" ]; then
  echo "ERROR: no shared library found under ${target}, nothing was checked" >&2
  exit 2
fi

# Match literally, except for names built from a CMake variable, which can only
# be matched as a pattern. Process substitution is avoided on purpose: under
# POSIX sh it is a syntax error, and the check would silently pass.
offenders=$(printf '%s\n@@SEP@@\n%s\n' "${forbidden}" "${shipped}" | awk '
  $0 == "@@SEP@@" { seen_sep = 1; next }
  !seen_sep {
    if ($0 ~ /\$\{/) {
      gsub(/\./, "\\.")
      gsub(/\$\{[^}]*\}/, ".*")
      patterns[++npat] = "^" $0 "$"
    } else {
      literals[$0] = 1
    }
    next
  }
  {
    if ($0 in literals) { print; next }
    for (i = 1; i <= npat; i++) {
      if ($0 ~ patterns[i]) { print; next }
    }
  }
')

if [ -n "${offenders}" ]; then
  echo "ERROR: ${target} ships test-only artifacts (Bug #85439771):" >&2
  echo "${offenders}" | sed 's/^/  /' >&2
  echo "Build with -DWITH_TEST_ONLY_PLUGINS=OFF and make sure TEST_ONLY targets" >&2
  echo "are excluded from install." >&2
  exit 1
fi

echo "OK: ${target} ships no test-only artifacts ($(echo "${forbidden}" | wc -l) checked)"
exit 0
