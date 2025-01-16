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
/** @file sql/pli/license_proc.cc

  License Procedure Implementation

  Created 2025-01-15 by Zefeng.Liu
 *******************************************************/

#include "license_proc.h"
#include "pli/pli.h"

namespace im {

/** All concurrency control system memory usage  */
PSI_memory_key key_memory_license_proc;

/** The uniform schema name for license */
const LEX_CSTRING LICENSE_PROC_SCHEMA{C_STRING_WITH_LEN("dbms_license")};

/** Must be the compatible with the License_error_t with
 * @file: include/pli/pli_common.h */
const LEX_CSTRING LICENSE_STATUS_STR[] = {
    {C_STRING_WITH_LEN(" ACTIVATED ")},
    {C_STRING_WITH_LEN(" NOT EXIST ")},
    {C_STRING_WITH_LEN(" CORRUPTED ")},
    {C_STRING_WITH_LEN(" UNSUPPOTED FORMAT ")},
    {C_STRING_WITH_LEN(" UNKOWN TYPE ")},
    {C_STRING_WITH_LEN(" EXPIRED ")},
    {C_STRING_WITH_LEN(" UNAUTHORIZED ")}};

/** Must be the same as the License_type_t with
 * @file: include/pli/pli_common.h */
const LEX_CSTRING LICENSE_TYPE[] = {{C_STRING_WITH_LEN(" STANDARD ")},
                                    {C_STRING_WITH_LEN(" UNSUPPORT ")}};

static inline const LEX_CSTRING get_license_status(
    xlicense::License_error_t &error) {
  size_t index = 0;

  switch (error) {
    case xlicense::License_error_t::LICENSE_OK:
      index = 0;
      break;
    case xlicense::License_error_t::LICENSE_FILE_NOT_EXIST:
      index = 1;
      break;
    case xlicense::License_error_t::LICENSE_FILE_CORRUPTED:
      index = 2;
      break;
    case xlicense::License_error_t::LICENSE_FORMAT_UNRECOGNIZED:
      index = 3;
      break;
    case xlicense::License_error_t::LICENSE_TYPE_UNSUPPORT:
      index = 4;
      break;
    case xlicense::License_error_t::LICENSE_EXPIRED:
      index = 5;
      break;
    case xlicense::License_error_t::LICENSE_UNAUTHORIZED_MACHINE:
      index = 6;
      break;
    case xlicense::License_error_t::LICENSE_TOO_MUCH_CONNS:
    case xlicense::License_error_t::LICNESE_UNPERMITTED_BP_SIZE:
    default:
      assert(false);
  }
  return LICENSE_STATUS_STR[index];
}

static inline const LEX_CSTRING get_license_type(
    xlicense::License_type_t &type) {
  size_t index = 0;
  switch (type) {
    case xlicense::License_type_t::LICENSE_TYPE_STANDARD:
      index = 0;
      break;
    case xlicense::License_type_t::LICENSE_TYPE_UNSUPPORT:
      index = 1;
      break;
    default:
      assert(false);
  }
  return LICENSE_TYPE[index];
}
/**
 * 1) dbms_license.show_info(...)
 *
 * Show license information.
 */
/** Singleton instance */
Proc *License_proc_show_info::instance() {
  static Proc *proc = new License_proc_show_info(key_memory_license_proc);
  return proc;
}

Sql_cmd *License_proc_show_info::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_license_show_info::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_license_show_info::pc_execute");

  if (!(xlicense::POLARX_LICENSE_CALL(search_license_show_info(m_show_info)))) {
    my_error(ER_LICENSE_PROC_DISABLE, MYF(0));
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

void Sql_cmd_license_show_info::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_license_show_info::send_result");

  Protocol *protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();

  LEX_CSTRING license_status = get_license_status(m_show_info.error);

  LEX_CSTRING license_type = get_license_type(m_show_info.license_type);

  protocol->store_string(m_show_info.license_file_path.c_str(),
                         m_show_info.license_file_path.length(),
                         system_charset_info);

  protocol->store_string(license_status.str, license_status.length,
                         system_charset_info);

  protocol->store_datetime(m_show_info.apply_time, 0);
  protocol->store_datetime(m_show_info.expire_time, 0);

  protocol->store_string(license_type.str, license_type.length,
                         system_charset_info);

  protocol->store_longlong(m_show_info.conns_constrains, true);
  protocol->store_longlong(m_show_info.BP_constrains, true);

  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/**
 * 2) dbms_license.check_license(...)
 *
 * Check License only but not refresh
 */
/** Singleton instance */
Proc *License_proc_check_license::instance() {
  static Proc *proc = new License_proc_check_license(key_memory_license_proc);
  return proc;
}

Sql_cmd *License_proc_check_license::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_license_check_license::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_license_check_license::send_result");

  Protocol *protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();

  LEX_CSTRING license_status = get_license_status(m_show_info.error);

  LEX_CSTRING license_type = get_license_type(m_show_info.license_type);

  protocol->store_string(m_show_info.license_file_path.c_str(),
                         m_show_info.license_file_path.length(),
                         system_charset_info);

  protocol->store_string(license_status.str, license_status.length,
                         system_charset_info);

  protocol->store_datetime(m_show_info.apply_time, 0);
  protocol->store_datetime(m_show_info.expire_time, 0);

  protocol->store_string(license_type.str, license_type.length,
                         system_charset_info);

  protocol->store_longlong(m_show_info.conns_constrains, true);
  protocol->store_longlong(m_show_info.BP_constrains, true);

  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

bool Sql_cmd_license_check_license::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_license_check_license::pc_execute");

  char buf[FN_REFLEN];
  String str(buf, sizeof(buf), system_charset_info);
  String *res = (*m_list)[0]->val_str(&str);
  std::string file_path(res->ptr(), res->length());

  if (!(xlicense::POLARX_LICENSE_CALL(
          check_specified_license(file_path, m_show_info, true)))) {
    my_error(ER_LICENSE_PROC_DISABLE, MYF(0));
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}
}  // namespace im