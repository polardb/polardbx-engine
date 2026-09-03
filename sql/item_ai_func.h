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
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef ITEM_AI_FUNC_H
#define ITEM_AI_FUNC_H

/**
  @file sql/item_ai_func.h
  AI built-in function declarations (AI_PROMPT, etc.).
*/

#include "sql/item_strfunc.h"

/**
  AI_PROMPT(prompt [, model [, options_json]])

  Invokes a Large Language Model (LLM) through the model registered in
  mysql.ai_model_config and returns the generated text.

  Parameters:
    - prompt (TEXT, required) - The input prompt / question
    - model (VARCHAR, optional) - Model config name; uses the AI_PROMPT
                                  default configured by dbms_ai.update_function
                                  if omitted
    - options_json (JSON, optional) - Override parameters as JSON:
        temperature, max_tokens, top_p, system_prompt

  API key lookup:
    - model api_key, if configured through dbms_ai.register_model/update_model
    - DASHSCOPE_API_KEY environment variable, if non-empty

  Examples:
    SELECT AI_PROMPT('What is PolarDB-X?');
    SELECT AI_PROMPT('Write a poem', 'qwen_plus');
    SELECT AI_PROMPT('Translate', 'gpt-4o', '{"temperature": 0.9}');
*/
class Item_func_ai_prompt final : public Item_str_func {
  String tmp_value;

 public:
  /* 1 arg: prompt only */
  Item_func_ai_prompt(const POS &pos, Item *prompt)
      : Item_str_func(pos, prompt) {}
  /* 2 args: prompt + model */
  Item_func_ai_prompt(const POS &pos, Item *prompt, Item *model)
      : Item_str_func(pos, prompt, model) {}
  /* 3 args: prompt + model + options_json */
  Item_func_ai_prompt(const POS &pos, Item *prompt, Item *model,
                      Item *options)
      : Item_str_func(pos, prompt, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_prompt"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_EMBEDDING(text [, model [, options_json]])

  Converts input text into a vector representation (embedding) by calling an
  OpenAI-compatible Embedding API. Returns a JSON array of floating-point
  numbers, e.g. [0.123, -0.456, ...].

  Parameters:
    - text   (TEXT, required)    - The input text to embed
    - model  (VARCHAR, optional) - Model config name; uses default EMBEDDING
                                   model if omitted
    - options_json (JSON, optional) - Override parameters as JSON

  Examples:
    SELECT AI_EMBEDDING('What is PolarDB-X?');
    SELECT AI_EMBEDDING('hello', 'text_embedding_v3');
*/
class Item_func_ai_embedding final : public Item_str_func {
  String tmp_value;

 public:
  /* 1 arg: text only */
  Item_func_ai_embedding(const POS &pos, Item *text)
      : Item_str_func(pos, text) {}
  /* 2 args: text + model */
  Item_func_ai_embedding(const POS &pos, Item *text, Item *model)
      : Item_str_func(pos, text, model) {}
  /* 3 args: text + model + options_json */
  Item_func_ai_embedding(const POS &pos, Item *text, Item *model,
                         Item *options)
      : Item_str_func(pos, text, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_embedding"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_CLASSIFY(text, categories_json [, model [, options]])

  Classifies input text into one of the given candidate categories by
  instructing the LLM via a system prompt.

  Parameters:
    - text       (TEXT, required)    - The text to classify
    - categories (JSON, required)   - JSON array of candidate categories
    - model      (VARCHAR, optional) - Model config name
    - options    (JSON, optional)    - Extra options

  Returns:
    VARCHAR — the selected category label.
*/
class Item_func_ai_classify final : public Item_str_func {
  String tmp_value;

 public:
  Item_func_ai_classify(const POS &pos, Item *text, Item *categories)
      : Item_str_func(pos, text, categories) {}
  Item_func_ai_classify(const POS &pos, Item *text, Item *categories,
                        Item *model)
      : Item_str_func(pos, text, categories, model) {}
  Item_func_ai_classify(const POS &pos, Item *text, Item *categories,
                        Item *model, Item *options)
      : Item_str_func(pos, text, categories, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_classify"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_EXTRACT(text, schema_json [, model [, options]])

  Extracts structured information from text according to the given schema.
  Returns a JSON object with keys defined in schema_json.
*/
class Item_func_ai_extract final : public Item_str_func {
  String tmp_value;

 public:
  Item_func_ai_extract(const POS &pos, Item *text, Item *schema)
      : Item_str_func(pos, text, schema) {}
  Item_func_ai_extract(const POS &pos, Item *text, Item *schema, Item *model)
      : Item_str_func(pos, text, schema, model) {}
  Item_func_ai_extract(const POS &pos, Item *text, Item *schema, Item *model,
                       Item *options)
      : Item_str_func(pos, text, schema, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_extract"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_SUMMARIZE(text [, max_length [, model [, options]]])

  Generates a summary of the input text, optionally limited to max_length chars.
*/
class Item_func_ai_summarize final : public Item_str_func {
  String tmp_value;

 public:
  Item_func_ai_summarize(const POS &pos, Item *text)
      : Item_str_func(pos, text) {}
  Item_func_ai_summarize(const POS &pos, Item *text, Item *max_len)
      : Item_str_func(pos, text, max_len) {}
  Item_func_ai_summarize(const POS &pos, Item *text, Item *max_len,
                         Item *model)
      : Item_str_func(pos, text, max_len, model) {}
  Item_func_ai_summarize(const POS &pos, Item *text, Item *max_len,
                         Item *model, Item *options)
      : Item_str_func(pos, text, max_len, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_summarize"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_SIMILARITY(input1, input2 [, similarity_type [, model_name]])

  Computes similarity between two vectors or texts.
  - If arguments are JSON arrays, computes directly (no API call).
  - If arguments are plain text, embeds them via AI_EMBEDDING first.
  similarity_type: 'cosine' (default), 'euclidean', 'dot'
  Returns DOUBLE.
*/
class Item_func_ai_similarity final : public Item_func {
 public:
  Item_func_ai_similarity(const POS &pos, Item *v1, Item *v2)
      : Item_func(pos, v1, v2) {}
  Item_func_ai_similarity(const POS &pos, Item *v1, Item *v2,
                          Item *sim_type)
      : Item_func(pos, v1, v2, sim_type) {}
  Item_func_ai_similarity(const POS &pos, Item *v1, Item *v2,
                          Item *sim_type, Item *model)
      : Item_func(pos, v1, v2, sim_type, model) {}

  double val_real() override;
  longlong val_int() override {
    return llrint_with_overflow_check(val_real());
  }
  String *val_str(String *str) override {
    double nr = val_real();
    if (null_value) return nullptr;
    str->set_real(nr, decimals, collation.collation);
    return str;
  }
  my_decimal *val_decimal(my_decimal *decimal_value) override {
    double nr = val_real();
    if (null_value) return nullptr;
    double2my_decimal(E_DEC_FATAL_ERROR, nr, decimal_value);
    return decimal_value;
  }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_real(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_real(ltime);
  }
  const char *func_name() const override { return "ai_similarity"; }
  enum Item_result result_type() const override { return REAL_RESULT; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_RANK(query, candidate [, model [, options]])

  Computes the relevance score between a query and a candidate text by
  calling a Rerank API (e.g. DashScope gte-rerank-v2).
  Returns DOUBLE in [0, 1].
*/
class Item_func_ai_rank final : public Item_func {
 public:
  Item_func_ai_rank(const POS &pos, Item *query, Item *candidate)
      : Item_func(pos, query, candidate) {}
  Item_func_ai_rank(const POS &pos, Item *query, Item *candidate, Item *model)
      : Item_func(pos, query, candidate, model) {}
  Item_func_ai_rank(const POS &pos, Item *query, Item *candidate, Item *model,
                    Item *options)
      : Item_func(pos, query, candidate, model, options) {}

  double val_real() override;
  longlong val_int() override {
    return llrint_with_overflow_check(val_real());
  }
  String *val_str(String *str) override {
    double nr = val_real();
    if (null_value) return nullptr;
    str->set_real(nr, decimals, collation.collation);
    return str;
  }
  my_decimal *val_decimal(my_decimal *decimal_value) override {
    double nr = val_real();
    if (null_value) return nullptr;
    double2my_decimal(E_DEC_FATAL_ERROR, nr, decimal_value);
    return decimal_value;
  }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_real(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_real(ltime);
  }
  const char *func_name() const override { return "ai_rank"; }
  enum Item_result result_type() const override { return REAL_RESULT; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_PARSE_DOCUMENT(url [, input_format [, model [, options_json]]])

  Parses a public document URL using a Document Parse model
  (e.g. qwen-doc-turbo) and returns the extracted plain text.
  input_format: 'auto' (default), 'text_only', 'text_and_images',
                or file-type aliases (pdf, word, image, etc.)
*/
class Item_func_ai_parse_document final : public Item_str_func {
  String tmp_value;

 public:
  Item_func_ai_parse_document(const POS &pos, Item *url)
      : Item_str_func(pos, url) {}
  Item_func_ai_parse_document(const POS &pos, Item *url, Item *format)
      : Item_str_func(pos, url, format) {}
  Item_func_ai_parse_document(const POS &pos, Item *url, Item *format,
                              Item *model)
      : Item_str_func(pos, url, format, model) {}
  Item_func_ai_parse_document(const POS &pos, Item *url, Item *format,
                              Item *model, Item *options)
      : Item_str_func(pos, url, format, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_parse_document"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_VL_EMBEDDING(content [, model_name [, options_json]])

  Generates a multimodal embedding vector (JSON array) for text, image URL,
  or video URL using a VL_EMBEDDING model (e.g. qwen3-vl-embedding).
  Auto-detects content type: http(s):// URLs → image/video, others → text.
  Options: {"dimension": 512, "content_type": "image", "fps": 2.0}
*/
class Item_func_ai_vl_embedding final : public Item_str_func {
  String tmp_value;

 public:
  Item_func_ai_vl_embedding(const POS &pos, Item *content)
      : Item_str_func(pos, content) {}
  Item_func_ai_vl_embedding(const POS &pos, Item *content, Item *model)
      : Item_str_func(pos, content, model) {}
  Item_func_ai_vl_embedding(const POS &pos, Item *content, Item *model,
                            Item *options)
      : Item_str_func(pos, content, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_vl_embedding"; }
  bool resolve_type(THD *thd) override;
};

/**
  AI_TEXT2SQL(prompt [, model [, options_json]])

  Converts a natural language question into a SQL statement using the
  table structures of the current database as context.

  The function:
    1. Collects the visible base tables of the current database from the
       data dictionary.  A table is included only when the user holds
       SELECT on it at some level (global, database, table or column);
       with a column-level grant only the granted columns are rendered,
       and indexes covering unauthorized columns are omitted.
    2. When the database holds more than 10 tables, asks the LLM to pick
       the relevant ones first (falls back to the first 10 on failure).
    3. Sends the question together with CREATE TABLE style definitions of
       the selected tables to the LLM and returns the generated SQL text.

  Contract: the result is generated text, not validated SQL.  The system
  prompt instructs the model to produce a single read-only SELECT, but
  the function performs no parse or execution; the caller must treat the
  output as untrusted text before running it (same contract as the CN
  AiText2SqlFunction).

  Parameters:
    - prompt (TEXT, required) - Natural language description of the query
    - model (VARCHAR, optional) - Model config name; uses the AI_TEXT2SQL
                                  default configured by
                                  dbms_ai.update_function if omitted
    - options_json (JSON, optional) - Override parameters as JSON:
        temperature, max_tokens

  Examples:
    SELECT AI_TEXT2SQL('Find all users older than 30');
    SELECT AI_TEXT2SQL('Total revenue by category', 'qwen_plus');
*/
class Item_func_ai_text2sql final : public Item_str_func {
  String tmp_value;

 public:
  /* 1 arg: prompt only */
  Item_func_ai_text2sql(const POS &pos, Item *prompt)
      : Item_str_func(pos, prompt) {}
  /* 2 args: prompt + model */
  Item_func_ai_text2sql(const POS &pos, Item *prompt, Item *model)
      : Item_str_func(pos, prompt, model) {}
  /* 3 args: prompt + model + options_json */
  Item_func_ai_text2sql(const POS &pos, Item *prompt, Item *model,
                        Item *options)
      : Item_str_func(pos, prompt, model, options) {}

  String *val_str(String *str) override;
  const char *func_name() const override { return "ai_text2sql"; }
  bool resolve_type(THD *thd) override;
};

#endif /* ITEM_AI_FUNC_H */
