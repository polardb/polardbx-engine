/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file ut/lizard0ut.cc
 Lizard barious utilities (Included in ut/ut0ut.cc)

 Created 2023-05-04 by Jiyang.zhang
 *******************************************************/

#include "lizard0ut.h"

/** Returns the number of microseconds since 1970/1/1 00:00:00.
 Uses the system clock.
 @return us since epoch or 0 if failed to retrieve */
ib_time_system_us_t ut_time_system_us(void) {
  const auto now = std::chrono::system_clock::now();
  const auto ret = std::chrono::duration_cast<std::chrono::microseconds>(
                       now.time_since_epoch())
                       .count();
  return (ret);
}
