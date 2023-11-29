/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/partitioning/partition_handler_ext.cc
  PolarX Sample V3

 Created 2023-10-17 by Jiyang.zhang
 *******************************************************/

#include "myisam.h"  // MI_MAX_MSG_BUF
#include "partition_handler.h"
#include "sql/partition_info.h"
#include "sql/sql_class.h"      // THD
#include "sql/sql_partition.h"  // LIST_PART_ENTRY, part_id_range
#include "sql/table.h"

int Partition_helper::ph_sample_init(bool scan) {
  int error;
  uint i = 0;
  uint part_id;
  DBUG_ENTER("Partition_helper::ph_sample_init");

  set_partition_read_set();

  /* Now we see what the index of our first important partition is */
  DBUG_PRINT("info", ("m_part_info->read_partitions: 0x%lx",
                      (long)m_part_info->read_partitions.bitmap));
  part_id = m_part_info->get_first_used_partition();
  DBUG_PRINT("info", ("m_part_spec.start_part %d", part_id));

  if (MY_BIT_NONE == part_id) {
    error = 0;
    goto err1;
  }

  DBUG_PRINT("info", ("sample_init on partition %d", part_id));
  if (scan) {
    /* A scan can be restarted without sample_end() in between! */
    if (m_scan_value == 1 && m_part_spec.start_part != NOT_A_PARTITION_ID) {
      /* End previous scan on partition before restart. */
      if ((error = sample_end_in_part(m_part_spec.start_part, scan))) {
        DBUG_RETURN(error);
      }
    }
    m_scan_value = 1;
    if ((error = sample_init_in_part(part_id, scan))) goto err;
  } else {
    m_scan_value = 0;
    for (i = part_id; i < MY_BIT_NONE;
         i = m_part_info->get_next_used_partition(i)) {
      if ((error = sample_init_in_part(i, scan))) goto err;
    }
  }
  m_part_spec.start_part = part_id;
  m_part_spec.end_part = m_tot_parts - 1;
  DBUG_PRINT("info", ("m_scan_value=%d", m_scan_value));
  DBUG_RETURN(0);

err:
  /* Call sample_end for all previously initialized partitions. */
  for (; part_id < i; part_id = m_part_info->get_next_used_partition(part_id)) {
    sample_end_in_part(part_id, scan);
  }
err1:
  m_scan_value = 2;
  m_part_spec.start_part = NO_CURRENT_PART_ID;
  DBUG_RETURN(error);
}

int Partition_helper::ph_sample_end() {
  int error = 0;
  DBUG_ENTER("Partition_helper::ph_sample_end");
  switch (m_scan_value) {
    case 3:  // Error
      assert(0);
      /* fall through. */
    case 2:  // Error
      break;
    case 1:
      if (NO_CURRENT_PART_ID != m_part_spec.start_part)  // Table scan
      {
        error = sample_end_in_part(m_part_spec.start_part, true);
      }
      break;
    case 0:
      uint i;
      for (i = m_part_info->get_first_used_partition(); i < MY_BIT_NONE;
           i = m_part_info->get_next_used_partition(i)) {
        int part_error;
        part_error = sample_end_in_part(i, false);
        if (part_error && !error) {
          error = part_error;
        }
      }
      break;
  }
  m_scan_value = 3;
  m_part_spec.start_part = NO_CURRENT_PART_ID;
  DBUG_RETURN(error);
}

int Partition_helper::ph_sample_next(uchar *buf) {
  int result = HA_ERR_END_OF_FILE;
  uint part_id = m_part_spec.start_part;
  DBUG_ENTER("Partition_helper::ph_sample_next");

  if (NO_CURRENT_PART_ID == part_id) {
    /*
      The original set of partitions to scan was empty and thus we report
      the result here.
    */
    goto end;
  }

  assert(m_scan_value == 1);

  while (true) {
    result = sample_next_in_part(part_id, buf);
    if (!result) {
      m_last_part = part_id;
      m_part_spec.start_part = part_id;
      m_table->set_row_status_from_handler(0);
      DBUG_RETURN(0);
    }

    /*
      if we get here, then the current partition ha_sample_next returned failure
    */
    if (result == HA_ERR_RECORD_DELETED) continue;  // Probably MyISAM

    if (result != HA_ERR_END_OF_FILE)
      goto end_dont_reset_start_part;  // Return error

    /* End current partition */
    DBUG_PRINT("info", ("sample_end on partition %d", part_id));
    if ((result = sample_end_in_part(part_id, true))) break;

    /* Shift to next partition */
    part_id = m_part_info->get_next_used_partition(part_id);
    if (part_id >= m_tot_parts) {
      result = HA_ERR_END_OF_FILE;
      break;
    }
    m_last_part = part_id;
    m_part_spec.start_part = part_id;
    DBUG_PRINT("info", ("sample_init on partition %d", part_id));
    if ((result = sample_init_in_part(part_id, true))) break;
  }

end:
  m_part_spec.start_part = NO_CURRENT_PART_ID;
end_dont_reset_start_part:
  m_table->set_row_status_from_handler(STATUS_NOT_FOUND);
  DBUG_RETURN(result);
}

int Partition_helper::sample_init_in_part(uint part_id [[maybe_unused]],
                                          bool table_scan [[maybe_unused]]) {
  assert(0);
  return 0;
}

int Partition_helper::sample_next_in_part(uint part_id [[maybe_unused]],
                                          uchar *buf [[maybe_unused]]) {
  assert(0);
  return 0;
}

int Partition_helper::sample_end_in_part(uint part_id [[maybe_unused]],
                                         bool scan [[maybe_unused]]) {
  assert(0);
  return 0;
}
