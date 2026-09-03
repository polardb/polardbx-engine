/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: wrapped bytes: 1) ref to existing bytes without malloc, 2) wrap malloc
 * bytes with std::shared_ptr
 */
#pragma once
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include "defs.h"
#include "memory.hpp"

namespace encdb {

using std::uint32_t;
using std::uint8_t;

class bytes_t : public EncdbBaseObj {
 public:
  bytes_t(const bytes_t &) = delete;
  bytes_t(bytes_t &&other) noexcept;
  explicit bytes_t(uint32_t nlen);
  explicit bytes_t(const uint8_t *ndata, uint32_t nlen, bool isRef = true);
  bytes_t() noexcept;
  bytes_t &operator=(const bytes_t &) = delete;
  bytes_t &operator=(bytes_t &&other) noexcept;
  ~bytes_t();

  /**
   * @brief set bytes_t to given address
   * @param [in]bRef  true - \c ndata can be used as an reference
   *                  false - \c ndata should be used as copy since it may be
   * freed during using
   */
  void set(const uint8_t *ndata, uint32_t nlen, bool bRef);

  /**
   * @brief whether bytes_t contains data
   */
  explicit operator bool() const noexcept;
  uint8_t *data() noexcept;
  const uint8_t *data() const noexcept;
  uint32_t size() const noexcept;
  void resize(uint32_t nlen);
  void fill(const uint8_t val = 0);

  bool operator==(const bytes_t &other) const;
  bool operator!=(const bytes_t &other) const;

  const std::string to_string() const noexcept;

  void swap(bytes_t &rhs);

 protected:
  std::shared_ptr<uint8_t> dataPtr;
  uint8_t *dataRef;
  uint32_t len;
  bool isRef;
};

// update BYTES_T_MAX_LENGTH in type/include/defs.h when sizeof(bytes_t) changes
// static_assert(sizeof(bytes_t) <= BYTES_T_MAX_LENGTH);

void swap(bytes_t &lhs, bytes_t &rhs);

}  // namespace encdb

// custom specialization of std::hash can be injected in namespace std
namespace std {

template <>
struct hash<encdb::bytes_t> {
  std::size_t operator()(encdb::bytes_t const &bytes) const noexcept;
};

}  // namespace std
