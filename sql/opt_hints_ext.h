/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/*
  Parse tree node classes for extended hint syntax
*/

#ifndef OPT_HINTS_EXT_INCLUDED
#define OPT_HINTS_EXT_INCLUDED

#include "opt_hints.h"

class Sample_percentage_hint {
  double pct;

 public:
  Sample_percentage_hint(double _pct) : pct(_pct) {}

  double sample_pct() const { return pct; }

  void print(const THD *thd, String *str);
};

extern bool check_sample_semantic(LEX *lex, const TABLE *table = nullptr);

#ifdef POLARX_SAMPLE_TEST
extern void turn_regular_query_to_sample(LEX *lex, const TABLE *table);
#endif

extern bool is_polarx_sample_query(LEX *lex,
                                   const TABLE *table [[maybe_unused]]);

#endif
