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

/** @file include/lizard0gcs0utc.h
 Lizard utc number implementation.

 Created 2025-06-24 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0gcs0utc_h
#define lizard0gcs0utc_h

#include "lizard0gcs0service.h"
#include "ut0dbg.h"

struct trx_t;

namespace lizard {

/** Represent commit utc time and state which are persisted into txn slot. */
/**------------------------------------------------------------------------*/
/** Format in UTC */
/*
 *	-------		---------
 *	1  bit		csr
 *	63 bit		time (us)
 */
/**------------------------------------------------------------------------*/
class UTC {
 private:
  /** Bits for time of UTC. */
  constexpr static uint64_t UTC_POS_US = 0;
  constexpr static uint64_t UTC_WIDTH_US = 63;
  constexpr static uint64_t UTC_MASK_US =
      ((~(~0ULL << UTC_WIDTH_US)) << UTC_POS_US);

  /** Bits for commit source of UTC. */
  constexpr static uint64_t UTC_POS_CSR = (UTC_POS_US + UTC_WIDTH_US);
  constexpr static uint64_t UTC_WIDTH_CSR = 1;
  constexpr static uint64_t UTC_MASK_CSR =
      ((~(~0ULL << UTC_WIDTH_CSR)) << UTC_POS_CSR);

 public:
  explicit UTC() : m_us(0), m_csr(CSR_AUTOMATIC) {}

  explicit UTC(utc_t us, csr_t csr) : m_us(us), m_csr(csr) {}

  virtual ~UTC() {}

  utc_t encode() const {
    ut_ad(interpet_csr(m_us) == 0);
    static_assert(CSR_AUTOMATIC == 0, "csr automatic != 0");
    static_assert(CSR_ASSIGNED == 1, "csr assigned != 1");

    utc_t value = static_cast<utc_t>(m_csr);
    return m_us | (value << UTC_POS_CSR);
  }

  void decode(const utc_t utc) {
    ut_ad(m_us == 0 && m_csr == 0);
    m_us = interpret_us(utc);
    m_csr = interpet_csr(utc);
  }

  static csr_t interpet_csr(utc_t value) {
    return static_cast<csr_t>(((value & UTC_MASK_CSR) >> UTC_POS_CSR));
  }

  static utc_t interpret_us(utc_t value) {
    return ((value & UTC_MASK_US) >> UTC_POS_US);
  }

  std::pair<utc_t, csr_t> get() const { return {m_us, m_csr}; }

  /** Generate a now utc for transaction commit.
   *
   * @param[in/out]	trx	rw transaction.
   *
   * @retval	utc */
  utc_t new_us(trx_t *trx) const;

 private:
  /** utc time(micro second)*/
  utc_t m_us;
  /** Commit source. */
  csr_t m_csr;
};

}  // namespace lizard

const utc_t UTC_NULL = lizard::UTC(US_NULL, CSR_AUTOMATIC).encode();

#endif
