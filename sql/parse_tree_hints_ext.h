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

#ifndef PARSE_TREE_HINTS_EXT_INCLUDED
#define PARSE_TREE_HINTS_EXT_INCLUDED

#include "parse_tree_hints.h"

class PT_hint_sample_percentage : public PT_hint {
  const LEX_CSTRING sample_pct;

  typedef PT_hint super;

 public:
  explicit PT_hint_sample_percentage(const LEX_CSTRING sample)
      : PT_hint(SAMPLE_PERCENTAGE_HINT_ENUM, true), sample_pct(sample) {}

  /**
    Function initializes SAMPLE_PERCENTAGE hint

    @param pc   Pointer to Parse_context object

    @return  true in case of error,
             false otherwise
  */
  virtual bool contextualize(Parse_context *pc) override;
};

#endif
