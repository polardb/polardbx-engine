/*****************************************************************************
Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file include/lizard0cmmt0cache0types.h
  Lizard tcn cache types

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0cmmt0cache0types_h
#define lizard0cmmt0cache0types_h

#include "univ.i"
#include "ut0cpu_cache.h"

namespace lizard {

/** Cache interface.
 *
 * Abstract key/value cache which only requires insert and search
 * operations, both of them are best-effort and allowed to give up. */
template <typename K, typename V>
class Cache_iface {
 public:
  Cache_iface() {}
  virtual ~Cache_iface() {}

  /** Insert value.
   *
   * @param[in]	value	value to cache, the target slot is decided by its key
   *
   * @retval	true if the value was discarded, false if it was cached */
  virtual bool insert(const V &value) = 0;

  /** Search value by key.
   *
   * @param[in]	key	search key
   *
   * @retval	cached value, or a default-constructed value on miss */
  virtual V search(const K &key) = 0;
};

/** Linear array.
 *
 * Fixed-size array cache which maps a key to the slot [key % size], the
 * new value directly overwrites the old one on conflict. The slot
 * latching is left to the subclass through the pair of try_latch()
 * and unlatch() hooks. */
template <typename K, typename V>
class Array_iface : public Cache_iface<K, V> {
 public:
  /** Constructor.
   *
   * @param[in]	size		array slot number
   * @param[in]	psi_mem_key	PSI memory key for the array allocation */
  Array_iface(size_t size, PSI_memory_key psi_mem_key) : m_size(size) {
    m_elements = ut::new_arr_withkey<V>(ut::make_psi_memory_key(psi_mem_key),
                                        ut::Count{m_size});
  }

  virtual ~Array_iface() { ut::delete_arr(m_elements); }

  /** Try to latch the slot before reading or writing it.
   *
   * @param[in]	pos	slot position
   *
   * @retval	true if the latch is held and the slot is safe to access,
   * false if the operation must be skipped */
  virtual bool try_latch(size_t pos) = 0;

  /** Release the slot latch after the operation.
   *
   * @param[in]	latched	whether the latch was acquired by try_latch()
   * and needs to be released
   * @param[in]	pos	slot position */
  virtual void unlatch(bool latched, size_t pos) = 0;

  /** Insert value.
   *
   * Overwrite the mapped slot under the slot latch, and give up
   * without any retry if the latch is not acquired.
   *
   * @param[in]	value	value to cache into the slot [value.key() % size]
   *
   * @retval	true if the value was discarded, false if it was cached */
  virtual bool insert(const V &value) override {
    size_t pos = value.key() % m_size;

    bool latched = try_latch(pos);
    if (latched) {
      m_elements[pos] = value;
    }
    unlatch(latched, pos);

    return !latched;
  }

  /** Search value by key.
   *
   * Read the mapped slot under the slot latch.
   *
   * NOTE: the returned value may belong to another key mapped to the
   * same slot, so the caller must confirm the key by itself.
   *
   * @param[in]	key	search key
   *
   * @retval	value stored in the slot [key % size], or a
   * default-constructed value if the latch was not acquired */
  virtual V search(const K &key) override {
    size_t pos = key % m_size;
    bool latched = try_latch(pos);
    V value;
    if (latched) {
      value = m_elements[pos];
    }
    unlatch(latched, pos);
    return value;
  }

 protected:
  /** Plain read of the key stored in the slot which the given key maps to.
   *
   * NOTE: the read is racy without any protection, so the result is only
   * a hint, and must be confirmed again under the slot latch.
   *
   * @param[in]	key	search key
   *
   * @retval	key currently stored in the mapped slot */
  K peek_key(const K &key) const { return m_elements[key % m_size].key(); }

 private:
  /** Array slot number. */
  size_t m_size;
  /** Value array whose length is m_size. */
  V *m_elements;
};

/** Linear array without any protection.
 *
 * The operations never fail, so it's only safe when the concurrent
 * access is impossible or is serialized by the caller. */
template <typename K, typename V>
class Plain_array : public Array_iface<K, V> {
 public:
  Plain_array(size_t size, PSI_memory_key psi) : Array_iface<K, V>(size, psi) {}

  virtual ~Plain_array() {}

  /** No latch at all, the operation is always allowed. */
  virtual bool try_latch(size_t pos) override { return true; }

  /** Nothing to release. */
  virtual void unlatch(bool latched, size_t pos) override { return; }
};

/** Linear array which is protected by atomic operation.
 *
 * Each slot is protected by an atomic flag shared by the slots mapped
 * to the same flag through [pos % ATOMIC_ARRAY_SIZE], which is acquired
 * by CAS spinning with a bounded retry, so the operation gives up
 * rather than blocks under heavy contention. */
template <typename K, typename V>
class Atomic_array : public Array_iface<K, V> {
 private:
  /** Retry max times. */
  constexpr static size_t RETRY_MAX_TIMES = 10;
  /** Atomic array size */
  constexpr static size_t ATOMIC_ARRAY_SIZE = 512 * 64;

 public:
  Atomic_array(size_t size, PSI_memory_key psi) : Array_iface<K, V>(size, psi) {
    for (size_t i = 0; i < ATOMIC_ARRAY_SIZE; i++) {
      m_used[i].store(false);
    }
  }

  virtual ~Atomic_array() {}

  /** Search value by key.
   *
   * Pre-filter by a plain read of the slot key, so the miss case pays
   * no atomic operation at all. A torn or stale value is benign here:
   * a false match is confirmed again under the slot latch within the
   * base search, and a false mismatch during a concurrent overwrite is
   * an acceptable miss for the best-effort cache.
   *
   * @param[in]	key	search key
   *
   * @retval	value stored in the slot [key % size], or a
   * default-constructed value on pre-filter mismatch or lock failure */
  virtual V search(const K &key) override {
    if (this->peek_key(key) != key) {
      return V();
    }

    return Array_iface<K, V>::search(key);
  }

  /** Acquire the slot latch by CAS spinning.
   *
   * @param[in]	pos	slot position
   *
   * @retval	true if the latch is held, false if it was not acquired
   * within RETRY_MAX_TIMES and the operation must be skipped */
  virtual bool try_latch(size_t pos) override {
    uint loop = 0;
    bool expected;
    uint used_index = pos % ATOMIC_ARRAY_SIZE;
  retry:
    if (loop++ > RETRY_MAX_TIMES) return false;

    expected = false;
    if (m_used[used_index].compare_exchange_strong(expected, true)) {
      return true;
    } else {
      goto retry;
    }
  }

  /** Release the slot latch if it was acquired.
   *
   * @param[in]	latched	whether the latch is held and to be released
   * @param[in]	pos	slot position */
  virtual void unlatch(bool latched, size_t pos) override {
    if (latched) {
      ut_ad(m_used[pos % ATOMIC_ARRAY_SIZE].load() == true);
      m_used[pos % ATOMIC_ARRAY_SIZE].store(false);
    }
  }

 private:
  /** Slot latch flags. Each flag is padded to a separate cache line to
   * avoid false sharing, since tids are allocated consecutively so the
   * neighbour slot latches are hammered by concurrent threads. The cost is
   * about 2MB in total, negligible compared with the tcn entry array. */
  ut::Cacheline_padded<std::atomic<bool>> m_used[ATOMIC_ARRAY_SIZE];
};

}  // namespace lizard
#endif
