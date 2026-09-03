/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file srv/lizard0srv0cycle.cc
  txn slot cycle service.

 Created 2025-07-14 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0srv0cycle_h
#define lizard0srv0cycle_h

namespace lizard {

#ifdef UNIV_DEBUG
extern bool srv_cycle_run_debug;
#endif

void srv_cycle_wakeup(void);

void srv_wake_cycle_thread_if_not_active(void);

/** Wait for the cycle coordinator thread to start up.*/
extern void srv_start_wait_for_cycle_to_start();

/** Entrance of cycle coordinator thread. */
extern void srv_cycle_coordinator_thread();

}  // namespace lizard

#endif
