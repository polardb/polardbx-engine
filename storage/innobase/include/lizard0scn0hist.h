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


/** @file include/lizard0scn0hist.h
 Lizard INNODB_FLASHBACK_SNAPSHOT system table.

 Created 2020-09-22 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0scn0hist_h
#define lizard0scn0hist_h

#include "univ.i"

#include "db0err.h"
#include "os0event.h"
#include "lizard0scn0types.h"

#ifdef UNIV_PFS_THREAD
extern mysql_pfs_key_t scn_history_thread_key;
#endif

namespace dd {
class Object_table;
}  // namespace dd

/** SCN history table named "INNODB_FLASHBACK_SNAPSHOT" */
#define SCN_HISTORY_TABLE_NAME "innodb_flashback_snapshot"
#define SCN_HISTORY_TABLE_FULL_NAME "mysql/innodb_flashback_snapshot"

/** Transformation state between scn and utc */
enum class SCN_TRANSFORM_STATE { NOT_FOUND, SUCCESS };

/** Transformation result between scn and utc */
struct scn_transform_result_t {
  scn_t scn;
  ulint utc;
  SCN_TRANSFORM_STATE state;
  dberr_t err;

  scn_transform_result_t() {
    scn = 0;
    utc = 0;
    state = SCN_TRANSFORM_STATE::NOT_FOUND;
    err = DB_SUCCESS;
  }
};

namespace lizard {

extern os_event_t scn_history_event;

/** SCN rolling forward interval */
extern ulint srv_scn_history_interval;

/** SCN rolling forward task */
extern bool srv_scn_history_task_enabled;

/** The max time of scn history record keeped */
extern ulint srv_scn_history_keep_days;

/** Create INNODB_FLASHBACK_SNAPSHOT table */
dd::Object_table *create_innodb_scn_hist_table();

/** Start the background thread of scn rolling forward */
extern void srv_scn_history_thread(void);

/** Shutdown the background thread of scn rolling forward */
extern void srv_scn_history_shutdown();

/** Init the background thread attributes */
extern void srv_scn_history_thread_init();

/** Deinit the background thread attributes */
extern void srv_scn_history_thread_deinit();

/**
  Try the best to transform UTC to SCN,
  it will search the first record which is more than the utc.
  Pls confirm the result state to judge whether the tranformation is success,
  and also it can report the error to client user if required through
  result.err.

  @param[in]      utc       Measure by second.
  @retval         result
*/
extern scn_transform_result_t try_scn_transform_by_utc(const ulint utc);

}  // namespace lizard

#endif  // lizard0scn0hist_h
