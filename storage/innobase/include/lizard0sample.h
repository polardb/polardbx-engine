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

/** @file include/lizard0sample.h
  Lizard Sample.

 Created 2023-10-13 by Jiyang.zhang
 *******************************************************/

#ifndef lizard0sample_h
#define lizard0sample_h

#include <stdint.h>
#include <random>

// #include "include/my_global.h"  // uchar
// #include "include/my_dbug.h"    // dbug_print

#include "btr0btr.h"
#include "btr0pcur.h"
#include "db0err.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "mtr0mtr.h"
#include "row0sel.h"
#include "ut0dbg.h"

namespace lizard {

class Sampler {
 public:
  /** Constructor.
  @param[in]  sampling_seed       seed to be used for sampling
  @param[in]  sampling_percentage percentage of sampling that needs to be done
  @param[in]  sampling_method     sampling method to be used for sampling */
  explicit Sampler(int sampling_seed, double sampling_percentage,
                   enum_sampling_method sampling_method);

  /** Destructor. */
  virtual ~Sampler() {}

  /** Initialize the sampler context.
  @param[in]  trx   Transaction used for parallel read.
  @param[in]  index clustered index.
  @param[in]  prebuilt  prebuilt info
  @retval DB_SUCCESS on success. */
  virtual dberr_t init(trx_t *trx, dict_index_t *index,
                       row_prebuilt_t *prebuilt);

  /** Fetch next record and store in **mysql_buf** */
  virtual dberr_t next(uchar *mysql_buf) = 0;

  virtual void end() {
    if (m_inited) {
      m_trx->isolation_level = m_backup_isolation_level;

      if (m_n_sampled != 0) {
#ifdef UNIV_DEBUG
        ib::info() << "PolarX Sample V3 (" << get_sample_mode() << ") sampled "
                   << get_n_smapled_records() << " records";
#endif
        m_n_sampled = 0;
      }
      m_inited = false;
    }
  }

 protected:
  /** Check if the processing of the record needs to be skipped.
  In case of record belonging to non-leaf page, we decide if the child page
  pertaining to the record needs to be skipped.
  In case of record belonging to leaf page, we read the page regardless.
  @return true if it needs to be skipped, else false. */
  bool skip();

  /** Open at the left side of the index. */
  virtual void open_at_index_left_side() = 0;

  /** Used for printing. */
  virtual const std::string get_sample_mode() = 0;

  size_t get_n_smapled_records() { return m_n_sampled; }

 private:
  /** Random generator engine used to provide us random uniformly distributed
  values required to decide if the row in question needs to be sampled or
  not. */
  std::mt19937 m_random_generator;

  /** Uniform distribution used by the random generator. */
  static std::uniform_real_distribution<double> m_distribution;

  /** Sampling method to be used for sampling. */
  enum_sampling_method m_sampling_method{enum_sampling_method::NONE};

  /** Sampling percentage to be used for sampling */
  double m_sampling_percentage{};

  /** Sampling seed to be used for sampling */
  int m_sampling_seed{};

  /** Backup the isolation level because only TRX_ISO_READ_UNCOMMITTED will be
  used. */
  trx_t::isolation_level_t m_backup_isolation_level;

 protected:
  /** The index to be sampled. */
  dict_index_t *m_index;

  /** For MVCC. */
  trx_t *m_trx;

  /** prebuilt struct. */
  row_prebuilt_t *m_prebuilt;

  /** check if inited. */
  bool m_inited;

  /** The number of records obtained in this sampling. */
  size_t m_n_sampled;
};

extern Sampler *create_sampler(int sampling_seed, double sampling_percentage,
                               dict_index_t *index);
}  // namespace lizard

#endif
