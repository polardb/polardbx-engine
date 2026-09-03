/* Copyright (c) 2025, Alibaba and/or its affiliates. All Rights Reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/* Unit tests for lizard undo block address (UBA) encode/decode.

   These tests cover the extra-flag (state / csr / is_slave) encode->decode
   round-trip of undo_addr_t, exercising BOTH decode entry points that were
   fixed to right-shift the flag before casting:
     1) the header constructor  undo_addr_t(undo_ptr_t)
     2) the out-of-line method  undo_addr_t::decode(undo_ptr_t)

   The previously latent bug decoded the csr flag as
       static_cast<csr_t>(undo_ptr & UBA_MASK_CSR)
   (no ">> UBA_POS_CSR"). Because csr is an enum (not bool) the cast does not
   normalize, so a set csr bit produced a value that is neither CSR_ASSIGNED(1)
   nor CSR_AUTOMATIC(0). These tests assert the decoded csr equals exactly the
   original enumerator.

   All cases stay on the "fake space" slot address path (space_id == 0) so the
   round-trip is fully self-contained and needs no InnoDB runtime state
   (no undo tablespace / rseg-id translation). */

#include <gtest/gtest.h>

#include "storage/innobase/include/lizard0txn0slot0uba0types.h"

namespace lizard_uba_unittest {

/* A fake-space slot address: space_id == SLOT_SPACE_ID_FAKE (0) keeps
   encode()/decode() away from trx_rseg_id_to_space_id()/undo::id2num(), while
   page_no/offset still round-trip through the address bit fields.

   NOTE: for a fake-space address, undo_addr_t::decode() asserts the offset is
   one of the reserved special offsets (offset >= SLOT_OFFSET_LIMIT, i.e.
   >= 0xFFFF - 5). 0xFFFF == SLOT_OFFSET_TEMP_TAB_REC. The assertion only checks
   offset, so an arbitrary page_no is fine. */
static constexpr space_id_t kFakeSpaceId = 0;
static constexpr page_no_t kPageNo = 0x12345678;
static constexpr ulint kOffset = 0xFFFF;

/* Bit layout constants mirrored from slot_addr_t/undo_addr_t (kept local so the
   test also documents the expected geometry independently of the private
   definitions under test). */
static constexpr uint64_t kPosIsSlave = 61;
static constexpr uint64_t kPosCsr = 62;
static constexpr uint64_t kPosState = 63;
static constexpr uint64_t kPosUnused = 55;
static constexpr uint64_t kUnusedWidth = 6;

/* Build a valid, non-null fake-space slot address. */
static slot_addr_t make_slot_addr() {
  return slot_addr_t(kFakeSpaceId, kPageNo, kOffset);
}

/* Verify that a decoded undo_addr_t matches the expected fields. */
static void expect_fields(const undo_addr_t &d, bool state, csr_t csr,
                          bool is_slave) {
  EXPECT_EQ(d.state, state);
  EXPECT_EQ(d.is_slave, is_slave);
  /* The heart of the regression: csr must decode to the exact enumerator,
     not a shifted/truncated value. Compare both as enum and as integer. */
  EXPECT_EQ(d.csr, csr);
  EXPECT_EQ(static_cast<int>(d.csr), static_cast<int>(csr));

  /* Address bits round-trip unchanged. */
  EXPECT_EQ(d.space_id, kFakeSpaceId);
  EXPECT_EQ(d.page_no, kPageNo);
  EXPECT_EQ(d.offset, kOffset);
}

/* Exhaustively round-trip every combination of the three extra flags through
   both decode paths and every static single-flag decoder. */
TEST(lizard_uba, encode_decode_roundtrip_all_flag_combinations) {
  const csr_t csrs[] = {CSR_AUTOMATIC, CSR_ASSIGNED};

  for (bool state : {false, true}) {
    for (csr_t csr : csrs) {
      for (bool is_slave : {false, true}) {
        undo_addr_t src(make_slot_addr(), state, csr, is_slave);
        const undo_ptr_t p = src.encode();

        /* Path 1: header constructor undo_addr_t(undo_ptr_t). */
        {
          undo_addr_t d(p);
          expect_fields(d, state, csr, is_slave);
        }

        /* Path 2: out-of-line undo_addr_t::decode(undo_ptr_t). */
        {
          undo_addr_t d(slot_addr_t::null());
          d.decode(p);
          expect_fields(d, state, csr, is_slave);
        }

        /* Static single-flag decoders must agree. */
        EXPECT_EQ(undo_addr_t::decode_csr(p), csr);
        EXPECT_EQ(static_cast<int>(undo_addr_t::decode_csr(p)),
                  static_cast<int>(csr));
        EXPECT_EQ(undo_addr_t::decode_slave(p), is_slave);
        /* decode_active is the logical negation of the persisted state bit. */
        EXPECT_EQ(undo_addr_t::decode_active(p), !state);
      }
    }
  }
}

/* Focused regression: with the csr bit set, the decoded csr must be exactly
   CSR_ASSIGNED (== 1). Before the fix this yielded a non-normalized enum value
   (neither CSR_ASSIGNED nor CSR_AUTOMATIC). */
TEST(lizard_uba, csr_assigned_bit_decodes_to_exactly_one) {
  ASSERT_EQ(static_cast<int>(CSR_ASSIGNED), 1);
  ASSERT_EQ(static_cast<int>(CSR_AUTOMATIC), 0);

  undo_addr_t src(make_slot_addr(), /*state=*/true, CSR_ASSIGNED,
                  /*is_slave=*/false);
  const undo_ptr_t p = src.encode();

  /* The csr bit (bit 62) is the only one that distinguishes the enum. */
  EXPECT_NE((p >> kPosCsr) & 1ULL, 0ULL);

  undo_addr_t d(p);
  EXPECT_EQ(d.csr, CSR_ASSIGNED);
  EXPECT_EQ(static_cast<int>(d.csr), 1);
  EXPECT_TRUE(d.csr == CSR_ASSIGNED);
  EXPECT_FALSE(d.csr == CSR_AUTOMATIC);
}

/* set_commit() must stamp state/csr/is_slave so the static and object decoders
   all observe the committed values. */
TEST(lizard_uba, set_commit_stamps_flags) {
  /* Start from an active (state=0), automatic, non-slave address. */
  undo_addr_t active(make_slot_addr(), /*state=*/false, CSR_AUTOMATIC,
                     /*is_slave=*/false);
  undo_ptr_t p = active.encode();
  EXPECT_TRUE(undo_addr_t::decode_active(p));

  undo_addr_t::set_commit(&p, CSR_ASSIGNED, /*is_slave=*/true);

  EXPECT_FALSE(undo_addr_t::decode_active(p));
  EXPECT_EQ(undo_addr_t::decode_csr(p), CSR_ASSIGNED);
  EXPECT_TRUE(undo_addr_t::decode_slave(p));

  undo_addr_t d(p);
  expect_fields(d, /*state=*/true, CSR_ASSIGNED, /*is_slave=*/true);
}

/* A well-formed undo_ptr must leave the 6 reserved bits [55,60] zero. This is
   exactly the invariant guarded by the (now effective) reserved-bit assertion
   inside slot_addr_t::decode(). */
TEST(lizard_uba, encoded_reserved_bits_are_zero) {
  const uint64_t unused_mask = (~(~0ULL << kUnusedWidth)) << kPosUnused;
  const csr_t csrs[] = {CSR_AUTOMATIC, CSR_ASSIGNED};

  for (bool state : {false, true}) {
    for (csr_t csr : csrs) {
      for (bool is_slave : {false, true}) {
        undo_addr_t src(make_slot_addr(), state, csr, is_slave);
        const undo_ptr_t p = src.encode();
        EXPECT_EQ(p & unused_mask, 0ULL);
        /* Only the intended flag bits may be set above the address region. */
        EXPECT_EQ((p >> kPosState) & 1ULL, state ? 1ULL : 0ULL);
        EXPECT_EQ((p >> kPosCsr) & 1ULL,
                  (csr == CSR_ASSIGNED) ? 1ULL : 0ULL);
        EXPECT_EQ((p >> kPosIsSlave) & 1ULL, is_slave ? 1ULL : 0ULL);
      }
    }
  }
}

}  // namespace lizard_uba_unittest
