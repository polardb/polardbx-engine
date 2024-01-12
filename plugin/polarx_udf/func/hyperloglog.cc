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

#include "../service/udf.h"

#include <math.h>
#include <stdint.h>
#include <cstring>
#include <string>
#include <vector>

#define HLL_P 14           /* The greater is P, the smaller the error. */
#define HLL_Q (64 - HLL_P) /* The number of bits of the hash value used for */
                           /*  determining the number of leading zeros. */
#define HLL_REGISTERS (1 << HLL_P)     /* With P=14, 16384 registers. */
#define HLL_P_MASK (HLL_REGISTERS - 1) /* Mask to index register. */
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1 << HLL_BITS) - 1)
#define HLL_ALPHA_INF 0.721347520444481703680 /* constant for 0.5/ln(2) */

template <class HashT>
class HyperLogLog {
  class Registers {
   public:
    Registers() {
      /* Allocate 1 byte more explicitly to avoid the out-of-bounds arrayaccess
       */
      m_regs.resize((HLL_REGISTERS * HLL_BITS / 8) + 1, 0);
    }

    inline size_t size() const { return m_regs.size(); }

    inline void reset() { memset(m_regs.data(), 0, size()); }

    inline int init(uint8_t *buf, size_t len) {
      if (len != size()) return -1;
      uint8_t *p = m_regs.data();
      memcpy(p, buf, len);
      return 0;
    }

    inline uint8_t get(uint16_t pos) const {
      uint32_t byte = pos * HLL_BITS / 8;
      uint32_t bit = (pos * HLL_BITS) & 7;
      return ((m_regs[byte] >> bit) | (m_regs[byte + 1] << (8 - bit))) &
             HLL_REGISTER_MAX;
    }

    inline void set(uint16_t pos, uint8_t val) {
      uint32_t byte = pos * HLL_BITS / 8;
      uint32_t bit = (pos * HLL_BITS) & 7;
      m_regs[byte] &= ~(HLL_REGISTER_MAX << bit);
      m_regs[byte] |= (val << bit);
      m_regs[byte + 1] &= ~(HLL_REGISTER_MAX >> (8 - bit));
      m_regs[byte + 1] |= (val >> (8 - bit));
    }

    inline void ReplaceIfGreater(uint16_t pos, uint8_t val) {
      if (get(pos) < val) set(pos, val);
    }

    void merge(Registers &regs) {
      for (uint16_t j = 0; j < HLL_REGISTERS; j++) {
        uint8_t val = regs.get(j);
        if (val > get(j)) {
          set(j, val);
        }
      }
    }

    void GetHistogram(std::vector<int> &reghisto) const {
      if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        const uint8_t *r = m_regs.data();
        uint8_t r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14,
            r15;
        for (int j = 0; j < 1024; j++) {
          /* Handle 16 registers per iteration. */
          r0 = r[0] & 63;
          r1 = (r[0] >> 6 | r[1] << 2) & 63;
          r2 = (r[1] >> 4 | r[2] << 4) & 63;
          r3 = (r[2] >> 2) & 63;
          r4 = r[3] & 63;
          r5 = (r[3] >> 6 | r[4] << 2) & 63;
          r6 = (r[4] >> 4 | r[5] << 4) & 63;
          r7 = (r[5] >> 2) & 63;
          r8 = r[6] & 63;
          r9 = (r[6] >> 6 | r[7] << 2) & 63;
          r10 = (r[7] >> 4 | r[8] << 4) & 63;
          r11 = (r[8] >> 2) & 63;
          r12 = r[9] & 63;
          r13 = (r[9] >> 6 | r[10] << 2) & 63;
          r14 = (r[10] >> 4 | r[11] << 4) & 63;
          r15 = (r[11] >> 2) & 63;

          reghisto[r0]++;
          reghisto[r1]++;
          reghisto[r2]++;
          reghisto[r3]++;
          reghisto[r4]++;
          reghisto[r5]++;
          reghisto[r6]++;
          reghisto[r7]++;
          reghisto[r8]++;
          reghisto[r9]++;
          reghisto[r10]++;
          reghisto[r11]++;
          reghisto[r12]++;
          reghisto[r13]++;
          reghisto[r14]++;
          reghisto[r15]++;

          r += 12;
        }
      } else {
        for (uint16_t j = 0; j < HLL_REGISTERS; j++) {
          reghisto[get(j)]++;
        }
      }
    }

    const uint8_t *c_ptr(size_t *len = NULL) const {
      if (len) *len = this->size();
      return m_regs.data();
    }

   private:
    std::vector<uint8_t> m_regs;
  };

 public:
  void add(uint8_t *p, size_t len) {
    uint64_t hash = m_hash(p, len);
    m_registers.ReplaceIfGreater(position(hash), ZeroRunLength(hash));
  }

  size_t count() const {
    double m = HLL_REGISTERS;
    double E;

    std::vector<int> reghisto(64, 0);
    m_registers.GetHistogram(reghisto);
    double z = m * tau((m - reghisto[HLL_Q + 1]) / (double)m);
    for (int j = HLL_Q; j >= 1; --j) {
      z += reghisto[j];
      z *= 0.5;
    }
    z += m * sigma(reghisto[0] / (double)m);
    E = llroundl(HLL_ALPHA_INF * m * m / z);

    return (size_t)E;
  }

  void merge(HyperLogLog<HashT> &hll) { m_registers.merge(hll.m_registers); }

  void inline reset() { m_registers.reset(); }

  /* The reg data size should be 1 byte less than its real size. */
  inline size_t GetRegSize() const { return m_registers.size() - 1; }

  int InitRegs(uint8_t *buf, size_t len) {
    if (len < GetRegSize()) return -1;
    len = std::min<size_t>(len, GetRegSize());
    m_registers.init(buf, len);
    return 0;
  }

  int DumpRegs(char *buf, size_t *len) const {
    if (!buf || !len || !*len) return -1;
    size_t reg_size = 0;
    const uint8_t *regs = m_registers.c_ptr(&reg_size);
    *len = std::min<size_t>(reg_size, *len);
    memcpy(buf, regs, *len);
    return 0;
  }

 private:
  /* Helper function sigma as defined in
   * "New cardinality estimation algorithms for HyperLogLog sketches"
   * Otmar Ertl, arXiv:1702.01284 */
  static double sigma(double x) {
    if (x == 1.) return INFINITY;
    double zPrime;
    double y = 1;
    double z = x;
    do {
      x *= x;
      zPrime = z;
      z += x * y;
      y += y;
    } while (zPrime != z);
    return z;
  }

  /* Helper function tau as defined in
   * "New cardinality estimation algorithms for HyperLogLog sketches"
   * Otmar Ertl, arXiv:1702.01284 */
  static double tau(double x) {
    if (x == 0. || x == 1.) return 0.;
    double zPrime;
    double y = 1.0;
    double z = 1 - x;
    do {
      x = sqrt(x);
      zPrime = z;
      y *= 0.5;
      z -= pow(1 - x, 2) * y;
    } while (zPrime != z);
    return z / 3;
  }

  static inline uint16_t position(uint16_t hash) { return (hash & HLL_P_MASK); }

  static uint8_t ZeroRunLength(uint64_t hash) {
    uint8_t rl = 1;
    hash >>= HLL_P;
    hash |= ((uint64_t)1 << HLL_Q);
    while (!(hash & (uint64_t)1)) {
      rl++;
      hash >>= 1;
    }
    return rl;
  }

 private:
  HashT m_hash;
  Registers m_registers;
};

class MurmurHash {
 public:
  MurmurHash(uint64_t seed = 0xadc83b19ULL) : m_seed(seed) {}
  ~MurmurHash() {}

  uint64_t operator()(uint8_t *p, size_t len) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = m_seed ^ (len * m);
    const uint8_t *data = (const uint8_t *)p;
    const uint8_t *end = data + (len - (len & 7));

    while (data != end) {
      uint64_t k;

#ifdef WORDS_BIGENDIAN
      k = (uint64_t)data[0];
      k |= (uint64_t)data[1] << 8;
      k |= (uint64_t)data[2] << 16;
      k |= (uint64_t)data[3] << 24;
      k |= (uint64_t)data[4] << 32;
      k |= (uint64_t)data[5] << 40;
      k |= (uint64_t)data[6] << 48;
      k |= (uint64_t)data[7] << 56;
#else
      k = *((const uint64_t *)data);
#endif

      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
      data += 8;
    }

    switch (len & 7) {
      case 7:
        h ^= (uint64_t)data[6] << 48; /* fall-thru */
        __attribute__((fallthrough));
      case 6:
        h ^= (uint64_t)data[5] << 40; /* fall-thru */
        __attribute__((fallthrough));
      case 5:
        h ^= (uint64_t)data[4] << 32; /* fall-thru */
        __attribute__((fallthrough));
      case 4:
        h ^= (uint64_t)data[3] << 24; /* fall-thru */
        __attribute__((fallthrough));
      case 3:
        h ^= (uint64_t)data[2] << 16; /* fall-thru */
        __attribute__((fallthrough));
      case 2:
        h ^= (uint64_t)data[1] << 8; /* fall-thru */
        __attribute__((fallthrough));
      case 1:
        h ^= (uint64_t)data[0];
        h *= m; /* fall-thru */
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
  }

 private:
  uint64_t m_seed;
};

/*
bool hyperloglog_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void hyperloglog_deinit(UDF_INIT *initid);
void hyperloglog_clear(UDF_INIT *initid, char *is_null, char *error);
void hyperloglog_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char
*error); char *hyperloglog(UDF_INIT *initid, UDF_ARGS *args, char *result,
                      unsigned long *length, char *is_null, char *error);
bool hllndv_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void hllndv_deinit(UDF_INIT *initid);
void hllndv_clear(UDF_INIT *initid, char *is_null, char *error);
void hllndv_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
longlong hllndv(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
*/

bool hyperloglog_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    strcpy(message, "HyperLogLog accepts only one argument");
    return 1;
  }

  switch (args->arg_type[0]) {
    case ROW_RESULT:
      strcpy(message, "HyperLogLog cannot accept row type arguent");
      return 1;
    default:
      break;
  }

  HyperLogLog<MurmurHash> *hll = new (std::nothrow) HyperLogLog<MurmurHash>();
  if (!hll) return 1;
  initid->const_item = 0;
  initid->maybe_null = 0;
  initid->max_length = hll->GetRegSize();
  initid->ptr = (char *)hll;
  return 0;
}

void hyperloglog_deinit(UDF_INIT *initid) {
  if (initid->ptr) {
    HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
    initid->ptr = NULL;
    delete hll;
  }
}

void hyperloglog_clear(UDF_INIT *initid, char *is_null MY_ATTRIBUTE((unused)),
                       char *error MY_ATTRIBUTE((unused))) {
  HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
  hll->reset();
}

void hyperloglog_add(UDF_INIT *initid, UDF_ARGS *args,
                     char *is_null MY_ATTRIBUTE((unused)),
                     char *error MY_ATTRIBUTE((unused))) {
  HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
  if (!args->args[0]) {
    hll->add(NULL, 0);
    return;
  }

  switch (args->arg_type[0]) {
    case STRING_RESULT:
    case DECIMAL_RESULT:
      hll->add((uint8_t *)args->args[0], args->lengths[0]);
      break;
    case INT_RESULT:
      hll->add((uint8_t *)args->args[0], sizeof(longlong));
      break;
    case REAL_RESULT:
      hll->add((uint8_t *)args->args[0], sizeof(double));
      break;
    default:
      break;
  }
}

char *hyperloglog(UDF_INIT *initid MY_ATTRIBUTE((unused)),
                  UDF_ARGS *args MY_ATTRIBUTE((unused)), char *result,
                  unsigned long *length, char *is_null MY_ATTRIBUTE((unused)),
                  char *error) {
  HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
  if (*length < hll->GetRegSize()) {
    *error = 1;
    *length = hll->GetRegSize();
    return NULL;
  }

  gs::udf::udf_counter.hyperloglog_counter++;

  if (hll->DumpRegs(result, length) != 0) {
    *error = 1;
    return NULL;
  }

  return result;
}

void hyperloglog_udf(gs::udf::Udf_definition *def) {
  def->m_name = const_cast<char *>("hyperloglog");
  def->m_result = STRING_RESULT;
  def->m_type = UDFTYPE_AGGREGATE;
  def->m_func_init = (Udf_func_init)hyperloglog_init;
  def->m_func_deinit = (Udf_func_deinit)hyperloglog_deinit;
  def->m_func_add = (Udf_func_add)hyperloglog_add;
  def->m_func_clear = (Udf_func_clear)hyperloglog_clear;
  def->m_func = (Udf_func_any)hyperloglog;
}

bool hllndv_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    strcpy(message, "HLLNDV accepts only one argument");
    return 1;
  }

  switch (args->arg_type[0]) {
    case ROW_RESULT:
      strcpy(message, "HLLNDV cannot accept row type arguent");
      return 1;
    default:
      break;
  }

  HyperLogLog<MurmurHash> *hll = new (std::nothrow) HyperLogLog<MurmurHash>();
  if (!hll) return 1;
  initid->const_item = 0;
  initid->maybe_null = 0;
  initid->max_length = hll->GetRegSize();
  initid->ptr = (char *)hll;
  return 0;
}

void hllndv_deinit(UDF_INIT *initid) {
  if (initid->ptr) {
    HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
    initid->ptr = NULL;
    delete hll;
  }
}

void hllndv_clear(UDF_INIT *initid, char *is_null MY_ATTRIBUTE((unused)),
                  char *error MY_ATTRIBUTE((unused))) {
  HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
  hll->reset();
}

void hllndv_add(UDF_INIT *initid, UDF_ARGS *args,
                char *is_null MY_ATTRIBUTE((unused)),
                char *error MY_ATTRIBUTE((unused))) {
  HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;
  if (!args->args[0]) {
    hll->add(NULL, 0);
    return;
  }

  switch (args->arg_type[0]) {
    case STRING_RESULT:
    case DECIMAL_RESULT:
      hll->add((uint8_t *)args->args[0], args->lengths[0]);
      break;
    case INT_RESULT:
      hll->add((uint8_t *)args->args[0], sizeof(longlong));
      break;
    case REAL_RESULT:
      hll->add((uint8_t *)args->args[0], sizeof(double));
      break;
    default:
      break;
  }
}

longlong hllndv(UDF_INIT *initid MY_ATTRIBUTE((unused)),
                UDF_ARGS *args MY_ATTRIBUTE((unused)),
                char *is_null MY_ATTRIBUTE((unused)),
                char *error MY_ATTRIBUTE((unused))) {
  HyperLogLog<MurmurHash> *hll = (HyperLogLog<MurmurHash> *)initid->ptr;

  gs::udf::udf_counter.hllndv_counter++;

  return (longlong)hll->count();
}

void hllndv_udf(gs::udf::Udf_definition *def) {
  def->m_name = const_cast<char *>("hllndv");
  def->m_result = INT_RESULT;
  def->m_type = UDFTYPE_AGGREGATE;
  def->m_func_init = (Udf_func_init)hllndv_init;
  def->m_func_deinit = (Udf_func_deinit)hllndv_deinit;
  def->m_func_add = (Udf_func_add)hllndv_add;
  def->m_func_clear = (Udf_func_clear)hllndv_clear;
  def->m_func = (Udf_func_any)hllndv;
}
