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

#ifndef XENGINE_INCLUDE_TEMPLATE_UTIL_H_
#define XENGINE_INCLUDE_TEMPLATE_UTIL_H_
#include <vector>
#include <list>
#include <unordered_set>
#include <unordered_map>
#include "autovector.h"

namespace xengine
{
namespace util
{
//macros used by HAS_MEMBER, for judge whether function(named member) exists or not?
#define DEFINE_HAS_MEMBER(member) \
  template <typename T> \
  struct has_##member \
  { \
    typedef char yes[1]; \
    typedef char no[2]; \
    \
    template <typename C> \
    static yes &test(decltype(&C::member)); \
    \
    template <typename C> \
    static no &test(...); \
    \
    static bool const value = sizeof(test<T>(0)) == sizeof(yes); \
  };
   
DEFINE_HAS_MEMBER(serialize)
DEFINE_HAS_MEMBER(deserialize)
DEFINE_HAS_MEMBER(get_serialize_size)

//macros used by IS_CONTAINER, for judge where type is container type
struct true_type
{
  static const bool value = true;
};
struct false_type
{
  static const bool value = false;
};

template <typename T>
struct is_container : false_type
{
};

#define DEFINE_CONTAINER_TYPE(type) \
  template<typename T> \
  struct is_container<type<T> > : public true_type \
  { \
  }

template <typename K, typename V>
struct is_kv_container : false_type
{
};

#define DEFINE_KV_CONTAINER_TYPE(type) \
  template <typename K, typename V> \
  struct is_kv_container<type<K, V>, void> : public true_type \
  { \
  }

DEFINE_CONTAINER_TYPE(std::vector);
DEFINE_CONTAINER_TYPE(std::list);
DEFINE_CONTAINER_TYPE(util::autovector);
DEFINE_CONTAINER_TYPE(std::unordered_set);
DEFINE_KV_CONTAINER_TYPE(std::unordered_map);

} //namespace util
} //namespace xengine

#define HAS_MEMBER(type, member) xengine::util::has_##member<type>::value

#define IS_CONTAINER(type) ((xengine::util::is_container<type>::value) || (xengine::util::is_kv_container<type, void>::value))
  
#endif //XENGINE_INCLUDE_TEMPLATE_UTIL_H_  
