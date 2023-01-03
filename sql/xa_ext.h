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

#ifndef XA_EXT_INCUDED
#define XA_EXT_INCUDED

/**
  @file

  Extends the XA code for AliSQL
*/

class XID_context {
 public:
  bool increased_prep_xp_xids() { return m_increased_prep_xids; }
  void set_increased_prep_xids() { m_increased_prep_xids = true; }
  void reset_increased_prep_xids() { m_increased_prep_xids = false; }

  bool do_binlog_rotate() { return m_do_binlog_rotate; }
  void set_do_binlog_rotate() { m_do_binlog_rotate = true; }
  void reset_do_binlog_rotate() { m_do_binlog_rotate = false; }

 private:
  /**
    True means the XA transaction has increased m_atomic_prep_xids,
    thus it has to decrease m_atomic_prep_xids after the XA transaction is
    preppared/committed/rolled back to engine.
  */
  bool m_increased_prep_xids = {false};
  /**
    True means rotate is needed the XA transaction has to rotate binlog
    after the XA transaction is preppared/committed/rolled back to engine.
  */
  bool m_do_binlog_rotate = {false};
};

#endif  // XA_EXT_INCUDED