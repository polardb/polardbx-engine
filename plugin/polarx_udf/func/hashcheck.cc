#include <string.h>
#include <boost/crc.hpp>
#include <cstdlib>
#include <string>

#include "../service/udf.h"

class HashCheckCaculator {
 public:
  /**
   * use ECMA-182 CRC Polynomial
   */
  using Crc = boost::crc_optimal<64, 0x42F0E1EBA9EA3693, 0xffffffffffffffff,
                                 0xffffffffffffffff, false, false>;

  HashCheckCaculator() : crc64(), hashVal(0), isFirstCacl(true) {}

  /**
   * this function process every row element include NULL element.
   * if element is NULL, it will feed a null_tag(0xfe) to crc64.
   * this function also attach a separator_tag(0xff) to each element.
   * */
  void rowElementUpdate(const char *buffer, size_t size) {
    if (buffer == nullptr) {
      crc64.process_byte(null_tag);
    } else {
      crc64.process_bytes(buffer, size);
    }
    crc64.process_byte(separator_tag);
  }

  /**
   * get the row digest and feed to myhash.
   * */
  void rowUpdate() {
    Crc::value_type crcResult = crc64.checksum();
    if (!isFirstCacl) {
      hashVal = myhash(static_cast<uint64_t>(crcResult));
    } else {
      isFirstCacl = false;
      hashVal = crcResult;
    }
    crc64.reset();
  }

  uint64_t getDigest() const { return hashVal; }

  void reset() {
    crc64.reset();
    isFirstCacl = true;
    hashVal = 0;
  }

  bool hasData() const { return !isFirstCacl; }

 private:
  inline uint64_t myhash(uint64_t data) const {
    return fact_p + fact_q * (hashVal + data) + fact_r * hashVal * data;
  }

  Crc crc64;

  uint64_t hashVal;
  bool isFirstCacl;

  const static uint64_t fact_p;
  const static uint64_t fact_q;
  const static uint64_t fact_r;
  const static char separator_tag;
  const static char null_tag;
};

const uint64_t HashCheckCaculator::fact_p = 3860031;
const uint64_t HashCheckCaculator::fact_q = 2779;
const uint64_t HashCheckCaculator::fact_r = 2;
const char HashCheckCaculator::separator_tag = 0xff;
const char HashCheckCaculator::null_tag = 0xfe;

bool hashcheck_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void hashcheck_deinit(UDF_INIT *initid);
void hashcheck_reset(UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null,
                     unsigned char *error);
void hashcheck_clear(UDF_INIT *initid, unsigned char *is_null,
                     unsigned char *error);
void hashcheck_add(UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null,
                   unsigned char *error);
longlong hashcheck(UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null,
                   unsigned char *error);
void hashcheck_udf(gs::udf::Udf_definition *def);

bool hashcheck_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count == 0) {
    strcpy(message,
           "wrong number of arguments: hashcheck() requires at least one "
           "argument");
    return 1;
  }

  HashCheckCaculator *caculator = new (std::nothrow) HashCheckCaculator;
  if (caculator == nullptr) {
    strcpy(message, "memory allocate error");
    return 1;
  }

  initid->ptr = reinterpret_cast<char *>(caculator);
  initid->maybe_null = 1;
  return 0;
}

void hashcheck_reset(UDF_INIT *initid, UDF_ARGS *, unsigned char *,
                     unsigned char *) {
  HashCheckCaculator *caculator =
      reinterpret_cast<HashCheckCaculator *>(initid->ptr);
  caculator->reset();
}

void hashcheck_clear(UDF_INIT *initid, unsigned char *, unsigned char *) {
  HashCheckCaculator *caculator =
      reinterpret_cast<HashCheckCaculator *>(initid->ptr);
  caculator->reset();
}

void hashcheck_add(UDF_INIT *initid, UDF_ARGS *args, unsigned char *,
                   unsigned char *) {
  HashCheckCaculator *caculator =
      reinterpret_cast<HashCheckCaculator *>(initid->ptr);

  for (unsigned int i = 0; i < args->arg_count; i++) {
    if (args->args[i] == nullptr) {
      caculator->rowElementUpdate(nullptr, 0);
    } else {
      switch (args->arg_type[i]) {
        case STRING_RESULT: {
          caculator->rowElementUpdate(args->args[i], args->lengths[i]);
          break;
        }
        case REAL_RESULT: {
          caculator->rowElementUpdate(args->args[i], sizeof(double));
          break;
        }
        case INT_RESULT: {
          caculator->rowElementUpdate(args->args[i], sizeof(long long));
          break;
        }
        case DECIMAL_RESULT: {
          caculator->rowElementUpdate(args->args[i], args->lengths[i]);
          break;
        }
        default:;
      }
    }
  }

  caculator->rowUpdate();
}

longlong hashcheck(UDF_INIT *initid, UDF_ARGS *, unsigned char *is_null,
                   unsigned char *) {
  gs::udf::udf_counter.hashcheck_counter++;
  HashCheckCaculator *caculator =
      reinterpret_cast<HashCheckCaculator *>(initid->ptr);
  if (!caculator->hasData()) {
    *is_null = 1;
    return 0;
  }
  return caculator->getDigest();
}

void hashcheck_deinit(UDF_INIT *initid) {
  HashCheckCaculator *caculator =
      reinterpret_cast<HashCheckCaculator *>(initid->ptr);
  delete caculator;
  initid->ptr = nullptr;
}

void hashcheck_udf(gs::udf::Udf_definition *def) {
  def->m_name = const_cast<char *>("hashcheck");
  def->m_result = INT_RESULT;
  def->m_type = UDFTYPE_AGGREGATE;
  def->m_func_init = (Udf_func_init)hashcheck_init;
  def->m_func_deinit = (Udf_func_deinit)hashcheck_deinit;
  def->m_func_add = (Udf_func_add)hashcheck_add;
  def->m_func_clear = (Udf_func_clear)hashcheck_clear;
  def->m_func = (Udf_func_any)hashcheck;
}