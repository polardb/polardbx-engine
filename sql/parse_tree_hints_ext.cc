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

#include "parse_tree_hints_ext.h"
#include "opt_hints_ext.h"

bool PT_hint_sample_percentage::contextualize(Parse_context *pc) {
  if (super::contextualize(pc)) return true;

  do {
    auto lex = pc->thd->lex;

    // the expected sql style:
    //   select /*+ sample_percentage(10.0) */ cols from t;

    if (lex->sql_command != SQLCOM_SELECT) break;

    if (check_sample_semantic(lex)) {
      my_error(ER_SAMPLE_WRONG_SEMANTIC, MYF(0), "");
      return true;
    }

    int error;
    auto end = sample_pct.str + sample_pct.length;
    double pct = my_strtod(sample_pct.str, &end, &error);
    if (error) break;

    if (pct < 0.0)
      pct = 0.0;
    else if (pct > 100.0)
      pct = 100.0;

    Opt_hints_global *global_hint = get_global_hints(pc);
    if (global_hint->is_specified(type())) {
      print_warn(pc->thd, ER_WARN_CONFLICTING_HINT, NULL, NULL, NULL, this);
      return false;
    }
    pc->thd->lex->sample_percentage = pct;
    assert(pc->thd->lex->hint_polarx_sample == false);
    pc->thd->lex->hint_polarx_sample = true;

    global_hint->sample_hint =
        new (pc->thd->mem_root) Sample_percentage_hint(pct);
    global_hint->set_switch(switch_on(), type(), false);
    return false;
  } while (0);

  my_error(ER_SAMPLE_WRONG_SEMANTIC, MYF(0), "");
  return true;
}
