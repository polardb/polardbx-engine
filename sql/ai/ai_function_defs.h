/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef SQL_AI_AI_FUNCTION_DEFS_INCLUDED
#define SQL_AI_AI_FUNCTION_DEFS_INCLUDED

/**
  @file sql/ai/ai_function_defs.h

  AI function metadata used by dbms_ai.update_function() and
  dbms_ai.show_ai_function().  The kernel initializes one metadata row per
  supported AI function without a default model; model rows and default model
  bindings are registered through the dbms_ai interfaces by the control plane.
*/

#include <cstddef>

namespace im {

struct Ai_function_def {
  const char *function_name;
  const char *description;
};

static const Ai_function_def ai_function_defs[] = {
    {"AI_PROMPT", "AI prompt function"},
    {"AI_EMBEDDING", "AI embedding function"},
    {"AI_CLASSIFY", "AI classify function"},
    {"AI_RANK", "AI rank function"},
    {"AI_SIMILARITY", "AI similarity function"},
    {"AI_EXTRACT", "AI extract function"},
    {"AI_SUMMARIZE", "AI summarize function"},
    {"AI_PARSE_DOCUMENT", "AI parse document function"},
    {"AI_VL_EMBEDDING", "AI vl embedding function"},
    {"AI_TEXT2SQL", "AI text2sql function"},
};

static const size_t ai_function_defs_count =
    sizeof(ai_function_defs) / sizeof(ai_function_defs[0]);

} /* namespace im */

#endif /* SQL_AI_AI_FUNCTION_DEFS_INCLUDED */
