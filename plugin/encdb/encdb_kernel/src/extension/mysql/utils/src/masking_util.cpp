#include "masking_util.h"
#include "../../include/encdb_encode.h"
#include "mysql.hh"

namespace encdb {

CQRResultsetValue encode_value_convert(ENCODE_VALUE &v) {
  if (v.is_binary_result) {
    return CQRResultsetValue((enum_field_types)v.type, v.flag, v.pData);

  } else {
    return CQRTextResultsetValue((enum_field_types)v.type, v.flag, v.pData);
  }
}

}  // namespace encdb
