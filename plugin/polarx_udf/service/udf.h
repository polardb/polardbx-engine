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

#ifndef PLUGIN_GALAXY_UDF_UDF_H
#define PLUGIN_GALAXY_UDF_UDF_H

#include <functional>

#include "my_compiler.h"
#include "my_inttypes.h"

#include "mysql/udf_registration_types.h"

/** All the udf initialization function declaration */

namespace gs {
namespace udf {

/** All counter for galaxy udf */
struct st_udf_counter {
  unsigned long long bloomfilter_counter = 0;
  unsigned long long hyperloglog_counter = 0;
  unsigned long long hllndv_counter = 0;
  unsigned long long hashcheck_counter = 0;
};

extern struct st_udf_counter udf_counter;

/**
  The definition of user defined function within galaxy plugin.
*/
struct Udf_definition {
 public:
  const char *m_name;
  Item_result m_result;
  Item_udftype m_type;
  Udf_func_any m_func;
  Udf_func_init m_func_init;
  Udf_func_deinit m_func_deinit;
  Udf_func_add m_func_add;
  Udf_func_clear m_func_clear;

  Udf_definition() { reset(); }
  void reset() {
    m_name = nullptr;
    m_result = INVALID_RESULT;
    m_type = UDFTYPE_FUNCTION;
    m_func = nullptr;
    m_func_init = nullptr;
    m_func_deinit = nullptr;
    m_func_add = nullptr;
    m_func_clear = nullptr;
  }
};

/** The common function was used to construct udf definition */
typedef std::function<void(Udf_definition *)> Udf_function;

class UDF {
 public:
  explicit UDF(Udf_function func) : m_def(), m_func(func) { m_func(&m_def); }

  Udf_definition def() { return m_def; }

 private:
  Udf_definition m_def;
  Udf_function m_func;
};

}  // namespace udf

}  // namespace gs

/** All the udf initialization function declaration */
void bloomfilter_udf(gs::udf::Udf_definition *def);
void hllndv_udf(gs::udf::Udf_definition *def);
void hyperloglog_udf(gs::udf::Udf_definition *def);
void hashcheck_udf(gs::udf::Udf_definition *def);

#endif
