/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "serialization.h"
#include <stdint.h>
#include <string.h>
#include "autovector.h"
#include "testharness.h"
#include "storage/change_info.h"
#include "storage/storage_manager.h"
#include "db/dbformat.h"
#include "util/to_string.h"

using namespace xengine::common;

namespace xengine {
namespace util {

TEST(Serialize, fixed_integral_type) {
  const int64_t bufsiz = 64;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;
  int8_t i8v = 1;
  int ret = encode_fixed_int8(buffer, bufsiz, encode_pos, i8v);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, sizeof(int8_t));

  int8_t ri8v = 0;
  ret = decode_fixed_int8(buffer, bufsiz, decode_pos, ri8v);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(decode_pos, sizeof(int8_t));
  ASSERT_EQ(i8v, ri8v);
}

TEST(Serialize, variable_integral_type) {
  const int64_t bufsiz = 64;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;
  int64_t i64v = 64;
  int ret = encode_var_int64(buffer, bufsiz, encode_pos, i64v);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, encoded_length_var_int64(i64v));

  int64_t ri64v = 0;
  ret = decode_var_int64(buffer, bufsiz, decode_pos, ri64v);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(decode_pos, encoded_length_var_int64(i64v));
  ASSERT_EQ(i64v, ri64v);
}

struct POD {
  int64_t i64_;
  int32_t i32_;
  int16_t i16_;
  uint8_t ui8_;
  char str_[20];
  POD() : POD(0, 0, 0, 0, nullptr) {}
  POD(int64_t i64, int32_t i32, int16_t i16, uint8_t ui8, const char *str)
      : i64_(i64), i32_(i32), i16_(i16), ui8_(ui8) {
    if (nullptr != str)
      strcpy(str_, str);
    else
      memset(str_, 0, sizeof(str_));
  }
  POD(const POD &pod) : POD(pod.i64_, pod.i32_, pod.i16_, pod.ui8_, nullptr) {
    memcpy(str_, pod.str_, 20);
  }

  bool equal(const POD &pod) {
    return i64_ == pod.i64_ && i32_ == pod.i32_ && i16_ == pod.i16_ &&
           ui8_ == pod.ui8_ && 0 == strcmp(str_, pod.str_);
  }

  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();
};

DEFINE_SERIALIZATION(POD, i64_, i32_, i16_, ui8_, str_);
DEFINE_TO_STRING(POD, KV_(i64), KV_(i32), KV_(i16), KV_(ui8), KV_(str));

struct Compound {
  int64_t ic64_;
  int32_t ic32_;
  POD pod_;
  Compound() : Compound(0, 0, POD()) {}
  Compound(int64_t i64, int32_t i32, const POD &pod)
      : ic64_(i64), ic32_(i32), pod_(pod) {}
  Compound(const Compound &comp)
      : Compound(comp.ic64_, comp.ic32_, comp.pod_) {}

  bool equal(const Compound &comp) {
    return ic64_ == comp.ic64_ && ic32_ == comp.ic32_ && pod_.equal(comp.pod_);
  }

  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();
};
DEFINE_SERIALIZATION(Compound, ic64_, ic32_, pod_);
DEFINE_TO_STRING(Compound, KV_(ic64), KV_(ic32), KV_(pod));

TEST(Serialize, serialize_struct) {
  const int64_t bufsiz = 64;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  POD pod(64, 32, 16, 8, "xegine_pod");
  int ret = pod.serialize(buffer, bufsiz, encode_pos);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, pod.get_serialize_size());

  POD rpod;
  ret = rpod.deserialize(buffer, bufsiz, decode_pos);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(decode_pos, encode_pos);
  ASSERT_TRUE(pod.equal(rpod));
}

TEST(Serialize, ExtentMeta) {
  util::autovector<storage::ExtentMeta> metas;
  storage::ExtentMeta meta;
  meta.largest_key_.Set("largest", 1, db::kTypeValue);
  meta.smallest_key_.Set("smallest", 2, db::kTypeValue);
  meta.extent_id_ = 0x1234;
  metas.push_back(meta);

  storage::ExtentMeta meta2;
  meta2.extent_id_ = 0x1111;
  metas.push_back(meta2);
  char buf[4096];
  int64_t pos = 0;
  util::serialize_v(buf, 4096, pos, metas);
  pos = 0;
  util::autovector<storage::ExtentMeta> dmetas;
  int ret = util::deserialize_v(buf, 4096, pos, dmetas);

  printf("ret: %d\n", ret);
  for (auto& m : dmetas) {
    printf("%lx\n", m.extent_id_.id());
    printf("%s\n", m.largest_key_.rep()->c_str());
  }
}

TEST(Serialize, serialize_list) {
  const int64_t bufsiz = 64;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t i64 = 64;
  int32_t i32 = 32;
  int8_t i8 = 8;
  const char *str = "xengine";

  int64_t encode_pos = 0;
  int ret = serialize(buffer, bufsiz, encode_pos, i64, i32, i8, str);
  ASSERT_EQ(ret, 0);
  int64_t size = get_serialize_size(i64, i32, i8, str);
  ASSERT_EQ(encode_pos, size);

  int64_t ri64 = 0;
  int32_t ri32 = 0;
  int8_t ri8 = 0;
  char rstr[32] = {0};
  int64_t decode_pos = 0;
  ret = deserialize(buffer, bufsiz, decode_pos, ri64, ri32, ri8, rstr);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, decode_pos);
  ASSERT_EQ(ri64, i64);
  ASSERT_EQ(ri32, i32);
  ASSERT_EQ(ri8, i8);
  ASSERT_EQ(0, strcmp(str, rstr));

  encode_pos = 0;
  POD pod(6400, 3200, 160, 8, "pod_struct");
  ret = serialize(buffer, bufsiz, encode_pos, i64, i32, pod, i8, str);
  ASSERT_EQ(ret, 0);
  size = get_serialize_size(i64, i32, pod, i8, str);
  ASSERT_EQ(encode_pos, size);

  POD rpod;
  decode_pos = 0;
  ret = deserialize(buffer, bufsiz, decode_pos, ri64, ri32, rpod, ri8, rstr);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, decode_pos);
  ASSERT_EQ(ri64, i64);
  ASSERT_EQ(ri32, i32);
  ASSERT_EQ(ri8, i8);
  ASSERT_EQ(0, strcmp(str, rstr));
  ASSERT_TRUE(pod.equal(rpod));
}

TEST(Serialize, to_string) {
  const int64_t bufsiz = 256;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  POD pod(64, 32, 16, 8, "xegine_pod");
  pod.to_string(buffer, bufsiz);
  const char *result =
      "{{\"i64\":64},{\"i32\":32},{\"i16\":16},{\"ui8\":8},{\"str\":\"xegine_"
      "pod\"}}";
  ASSERT_EQ(0, strcmp(result, buffer));
}

TEST(Serialize, serialize_compound) {
  const int64_t bufsiz = 128;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  POD pod(64, 32, 16, 8, "xegine_pod");
  Compound compound(644, 322, pod);
  int ret = compound.serialize(buffer, bufsiz, encode_pos);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, compound.get_serialize_size());

  Compound rcompound;
  ret = rcompound.deserialize(buffer, bufsiz, decode_pos);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(decode_pos, encode_pos);
  ASSERT_TRUE(rcompound.equal(compound));

  rcompound.to_string(buffer, bufsiz);
  const char *result =
      "{{\"ic64\":644},{\"ic32\":322},{\"pod\":{{\"i64\":64},{\"i32\":32},{"
      "\"i16\":16},{\"ui8\":8},{\"str\":\"xegine_pod\"}}}}";
  ASSERT_EQ(0, strcmp(result, buffer));
}

struct CompoundWithArray {
  int64_t *array_;
  int64_t num_;
  Compound compound_;
  CompoundWithArray() : CompoundWithArray(nullptr, 0, Compound()) {}
  CompoundWithArray(int64_t *array, int64_t num, const Compound &compound)
      : array_(array), num_(num), compound_(compound) {}

  bool equal(const CompoundWithArray &comp) {
    return 0 == memcmp(array_, comp.array_, num_ * sizeof(int64_t)) &&
           compound_.equal(comp.compound_);
  }

  DECLARE_TO_STRING();
};

DEFINE_TO_STRING(CompoundWithArray, "array", util::ArrayWrap<int64_t>(array_, num_),
                 KV_(compound));

TEST(Serialize, to_string_compound_array) {
  const int64_t bufsiz = 256;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  POD pod(64, 32, 16, 8, "xegine_pod");
  Compound compound(644, 322, pod);
  int64_t items[] = {1, 3, 6, 99};
  CompoundWithArray car1(items, sizeof(items) / sizeof(int64_t), compound);

  car1.to_string(buffer, bufsiz);
  const char *result =
      "{{\"array\":[1, 3, 6, "
      "99]},{\"compound\":{{\"ic64\":644},{\"ic32\":322},{\"pod\":{{\"i64\":64}"
      ",{\"i32\":32},{\"i16\":16},{\"ui8\":8},{\"str\":\"xegine_pod\"}}}}}}";
  ASSERT_EQ(0, strcmp(result, buffer));
}

TEST(Serialize, serialize_vector) {
  const int64_t bufsiz = 64;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  std::vector<int32_t> av;
  for (int32_t i = 0; i < 10; ++i) {
    av.push_back(i + 10);
  }
  int ret = util::serialize_v(buffer, bufsiz, encode_pos, av);
  ASSERT_EQ(ret, 0);

  std::vector<int32_t> rv;
  ret = util::deserialize_v(buffer, bufsiz, decode_pos, rv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, decode_pos);
  ASSERT_EQ(av, rv);

  int64_t size = get_serialize_v_size(av);
  ASSERT_EQ(encode_pos, size);
}

TEST(Serialize, serialize_auto_vector) {
  const int64_t bufsiz = 64;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  autovector<int64_t> av;
  for (int64_t i = 0; i < 20; ++i) {
    av.emplace_back(i * 17974);
  }
  int ret = util::serialize_v(buffer, bufsiz, encode_pos, av);
  ASSERT_EQ(ret, 0);

  autovector<int64_t> rv;
  ret = util::deserialize_v(buffer, bufsiz, decode_pos, rv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, decode_pos);
  for (int64_t i = 0; i < (int64_t)av.size(); ++i) {
    ASSERT_EQ(av[i], rv[i]);
  }

  int64_t size = get_serialize_v_size(av);
  ASSERT_EQ(encode_pos, size);
}

TEST(Serialize, serialize_vector_pod) {
  const int64_t bufsiz = 256;
  char buffer[bufsiz];
  memset(buffer, 0, bufsiz);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  std::vector<POD> av;
  for (int64_t i = 0; i < 10; ++i) {
    POD pod(i * 2, i, i, i, "xegine_pod");
    av.push_back(pod);
  }
  int ret = util::serialize_v(buffer, bufsiz, encode_pos, av);
  ASSERT_EQ(ret, 0);

  std::vector<POD> rv;
  ret = util::deserialize_v(buffer, bufsiz, decode_pos, rv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(encode_pos, decode_pos);
  // ASSERT_EQ(av, rv);
  for (int64_t i = 0; i < (int64_t)av.size(); ++i) {
    ASSERT_TRUE(av[i].equal(rv[i]));
  }

  int64_t size = get_serialize_v_size(av);
  ASSERT_EQ(encode_pos, size);
}


TEST(PrintUtil, slice_to_string) {
  char key_buffer[1024];
  
  const char *mins = "abcabcxxxxxabcabcxxxxxabcabcxxxxx";
  common::Slice akey(mins);
  akey.to_string(key_buffer, 1024);
  fprintf(stderr, "akey:%s\n", key_buffer);
  const char *maxs = "aaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbb";
  common::Slice bkey(maxs);
  bkey.to_string(key_buffer, 1024);
  fprintf(stderr, "bkey:%s\n", key_buffer);
}

TEST(Serialize_x, single_pod)
{
  int ret = common::Status::kOk;
  const int64_t buf_len = 256;
  char buf[buf_len];
  memset(buf, 0, buf_len);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  int32_t pod_int1 = 32;
  ret = util::serialize_x(buf, buf_len, encode_pos, pod_int1);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(encode_pos, util::get_serialize_size_x(pod_int1));
  pod_int1 = 0;
  ret = util::deserialize_x(buf, buf_len, decode_pos, pod_int1);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(32, pod_int1);
}

TEST(Serialize_x, multi_pods)
{
  int ret = common::Status::kOk;
  const int64_t buf_len = 256;
  char buf[buf_len];
  memset(buf, 0, buf_len);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  int8_t pod_int1 = 29;
  int16_t pod_int2 = 43;
  int32_t pod_int3 = 56;
  int64_t pod_int4 = 78;

  ret = util::serialize_x(buf, buf_len, encode_pos, pod_int1, pod_int2, pod_int3, pod_int4);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(encode_pos, util::get_serialize_size_x(pod_int1, pod_int2, pod_int3, pod_int4));
  pod_int1 = 0;
  pod_int2 = 0;
  pod_int3 = 0;
  pod_int4 = 0;
  ret = util::deserialize_x(buf, buf_len, decode_pos, pod_int1, pod_int2, pod_int3, pod_int4);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(29, pod_int1);
  ASSERT_EQ(43, pod_int2);
  ASSERT_EQ(56, pod_int3);
  ASSERT_EQ(78, pod_int4);
}

struct MultiDataType
{
  static const int64_t VERSION = 1;
  int32_t int_type_;
  POD pod_type_;
  std::vector<int64_t> vector_type_;
  util::autovector<POD> autovector_type_;

  MultiDataType() : int_type_(0), pod_type_(), vector_type_(), autovector_type_() {}
  ~MultiDataType() {}
  bool equal(const MultiDataType &obj)
  {
    bool is_equal = true;
    if (int_type_ != obj.int_type_) {
      is_equal = false;
    } else if (!pod_type_.equal(obj.pod_type_)) {
      is_equal = false;
    } else if (vector_type_.size() != obj.vector_type_.size()){
      is_equal = false;
    } else {
      for (uint64_t i = 0; i < vector_type_.size(); ++i) {
        if (vector_type_[i] != obj.vector_type_[i]) {
          is_equal = false;
          break;
        }
      }
      if (is_equal && autovector_type_.size() == obj.autovector_type_.size()) {
        for (uint64_t i = 0; i < autovector_type_.size(); ++i) {
          if (!autovector_type_[i].equal(obj.autovector_type_[i])) {
            is_equal = false;
            break;
          }
        }
      }
    }
    return is_equal;
  }

  DECLARE_COMPACTIPLE_SERIALIZATION(VERSION);
};

DEFINE_COMPACTIPLE_SERIALIZATION(MultiDataType, int_type_, pod_type_, vector_type_, autovector_type_);

TEST(Serialize_x, multi_data_types)
{
  int ret = common::Status::kOk;
  const int64_t buf_len = 256;
  char buf[buf_len];
  memset(buf, 0, buf_len);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  MultiDataType obj;
  MultiDataType obj_tmp;
  obj.int_type_ = 4689;
  obj.pod_type_.i64_ = 2468890;
  obj.pod_type_.i32_ = 347954;
  obj.pod_type_.i16_ = 23;
  obj.pod_type_.ui8_ = 54;
  strcpy(obj.pod_type_.str_, "xengine");
  obj.vector_type_.push_back(56);
  obj.vector_type_.push_back(78);
  obj.vector_type_.push_back(90);
  obj.autovector_type_.push_back(POD(12, 23, 34, 56, "xengine"));
  obj.autovector_type_.push_back(POD(56, 34, 23, 12, "xengine"));

  ret = obj.serialize(buf, buf_len, encode_pos);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(encode_pos, obj.get_serialize_size());
  ret = obj_tmp.deserialize(buf, buf_len, decode_pos);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(true, obj.equal(obj_tmp));
}

struct BaseStruct
{
  static const int64_t VERSION = 1;
  int32_t i32_;
  int64_t i64_;
  POD pod_;

  BaseStruct() : i32_(0), i64_(0), pod_() {}
  ~BaseStruct() {}

  DECLARE_COMPACTIPLE_SERIALIZATION(VERSION);
};
DEFINE_COMPACTIPLE_SERIALIZATION(BaseStruct, i32_, i64_, pod_);

struct BaseStructV2
{
  static const int64_t VERSION = 1;
  int32_t i32_;
  int64_t i64_;
  POD pod_;
  int16_t i16_;

  BaseStructV2() : i32_(0), i64_(0), pod_(), i16_(0) {}
  ~BaseStructV2() {}

  DECLARE_COMPACTIPLE_SERIALIZATION(VERSION);
};
DEFINE_COMPACTIPLE_SERIALIZATION(BaseStructV2, i32_, i64_, pod_, i16_);

TEST(Serialize_x, compactiple)
{
  int ret = common::Status::kOk;
  const int64_t buf_len = 256;
  char buf[buf_len];
  memset(buf, 0, buf_len);

  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  BaseStruct base_struct;
  BaseStructV2 base_struct_v2;
  base_struct.i32_ = 5698;
  base_struct.i64_ = 34875;
  base_struct.pod_.i64_ = 4567;
  base_struct.pod_.i32_ = 345;
  base_struct.pod_.i16_ = 235;
  base_struct.pod_.ui8_ = 56;
  strcpy(base_struct.pod_.str_, "xengine");

  ret = base_struct.serialize(buf, buf_len, encode_pos);
  ASSERT_EQ(common::Status::kOk, ret);
  ret = base_struct_v2.deserialize(buf, buf_len, decode_pos);
  ASSERT_EQ(common::Status::kOk, ret);
  ASSERT_EQ(5698, base_struct_v2.i32_);
  ASSERT_EQ(34875, base_struct_v2.i64_);
  ASSERT_EQ(true, base_struct.pod_.equal(base_struct_v2.pod_));
  ASSERT_EQ(0, base_struct_v2.i16_);

}

TEST(serialization_b, ChangeInfo)
{
  int ret = Status::kOk;
  const int64_t buf_len = 1024;
  char buf[buf_len];
  memset(buf, 0, buf_len);
  int64_t encode_pos = 0;
  int64_t decode_pos = 0;

  storage::ChangeInfo change_info;
  storage::ExtentId extent_id(1, 1);
  storage::LayerPosition layer_position(0, 0);
  change_info.delete_extent(layer_position, extent_id);
  layer_position.level_ = 1;
  change_info.add_extent(layer_position, extent_id);
  
  ret = change_info.serialize(buf, buf_len, encode_pos);
  ASSERT_EQ(common::Status::kOk, ret);

  storage::ChangeInfo change_info_des;
  change_info_des.deserialize(buf, buf_len, decode_pos);
  ASSERT_EQ(common::Status::kOk, ret);

  int64_t i = 0;
  for (auto iter = change_info_des.extent_change_info_.begin(); iter != change_info_des.extent_change_info_.end(); ++iter, ++i) {
    ASSERT_EQ(i, iter->first);
    ASSERT_EQ(1, iter->second.size());
    for (uint32_t j = 0; j < iter->second.size(); ++j) {
      storage::ExtentChange extent_change = iter->second.at(j);
      ASSERT_EQ(1, extent_change.extent_id_.file_number);
      ASSERT_EQ(1, extent_change.extent_id_.offset);
      if (0 == i) {
        ASSERT_TRUE(extent_change.is_delete());
      } else if (1 == i) {
        ASSERT_TRUE(extent_change.is_add());
      } else {
        abort();
      }
    }
  }
}

}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
