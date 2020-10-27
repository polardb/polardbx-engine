/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0mtr.h
  Lizard mtr wrapper.

 Created 2020-05-28 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0mtr_h
#define lizard0mtr_h

struct mtr_t;
struct trx_t;

namespace lizard {

class Mtr_wrapper {
 public:
  Mtr_wrapper(mtr_t *mtr);
  ~Mtr_wrapper();

  void start();

  lsn_t commit();

 private:
  mtr_t *m_mtr;
  bool m_started;
};

class Trx_rseg_mutex_wrapper {
 public:
  Trx_rseg_mutex_wrapper(trx_t *trx);
  ~Trx_rseg_mutex_wrapper();

  void release_mutex();

  bool txn_rseg_updated() const { return m_txn_rseg_locked; }
  bool redo_rseg_updated() const { return m_redo_rseg_locked; }
  bool temp_rseg_updated() const { return m_temp_rseg_locked; }

 private:
  trx_t *m_trx;
  bool m_txn_rseg_locked;
  bool m_redo_rseg_locked;
  bool m_temp_rseg_locked;
};

}  // namespace lizard
#endif  // lizard0mtr_h
