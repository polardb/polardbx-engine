/* Copyright (c) 2024, 2026, Alibaba and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <memory>
#include <vector>
#include "sql/dd/impl/properties_impl.h"
#include "sql/dd/string_type.h"
#include "sql/field.h"
#include "sql/key.h"
#include "sql/table.h"
#include "vidx/vidx_index.h"

namespace vidx {

/* -------------------- dd_table_get_hlindex_count -------------------- */

uint dd_table_get_hlindex_count(const dd::Table *dd_table) {
  if (!dd_table->options().exists("__hlindexes__")) return 0;

  dd::String_type raw;
  dd_table->options().get("__hlindexes__", &raw);
  std::unique_ptr<dd::Properties> p(dd::Properties::parse_properties(raw));
  if (p == nullptr || !p->exists("count") ||
      (!p->exists("next_id") && !p->exists("next")))
    return 1;
  uint count = 0;
  if (p->get("count", &count) || count == 0) return 1;
  return count;
}

/* -------------------- decode_hlindex_meta -------------------- */

bool decode_hlindex_meta(
    const dd::Table *dd_table, std::vector<hlindex_meta_entry> *entries,
    uint *next_id) {
  assert(entries != nullptr);
  entries->clear();

  dd::String_type raw;
  dd_table->options().get("__hlindexes__", &raw);

  /* Try structured format: parseable as dd::Properties with both
     "count" and "next_id" (or legacy "next") keys.  Requiring two keys
     prevents a backtick-quoted index name containing '=' from being
     misinterpreted. */
  std::unique_ptr<dd::Properties> p(dd::Properties::parse_properties(raw));
  if (p != nullptr && p->exists("count") &&
      (p->exists("next_id") || p->exists("next"))) {
    uint count = 0;
    if (p->get("count", &count)) return true;
    if (next_id != nullptr) {
      *next_id = count;  /* safe default */
      if (p->exists("next_id"))
        p->get("next_id", next_id);
      else if (p->exists("next"))
        p->get("next", next_id);
    }

    char kbuf[32];
    for (uint i = 0; i < count; i++) {
      hlindex_meta_entry e;
      snprintf(kbuf, sizeof(kbuf), "name_%u", i);
      if (!p->exists(dd::String_type(kbuf)) ||
          p->get(dd::String_type(kbuf), &e.name))
        return true;
      snprintf(kbuf, sizeof(kbuf), "id_%u", i);
      if (!p->exists(dd::String_type(kbuf)) ||
          p->get(dd::String_type(kbuf), &e.id))
        return true;
      /* Read optional enriched fields (present during CREATE only). */
      snprintf(kbuf, sizeof(kbuf), "m_%u", i);
      if (p->exists(dd::String_type(kbuf))) {
        p->get(dd::String_type(kbuf), &e.m);
        snprintf(kbuf, sizeof(kbuf), "dist_%u", i);
        p->get(dd::String_type(kbuf), &e.dist);
        snprintf(kbuf, sizeof(kbuf), "ef_%u", i);
        p->get(dd::String_type(kbuf), &e.ef);
        snprintf(kbuf, sizeof(kbuf), "col_%u", i);
        p->get(dd::String_type(kbuf), &e.col);
        e.enriched = true;
      }
      entries->push_back(e);
    }
    return false;
  }

  /* Bare string: legacy single-index layout. */
  entries->push_back({raw, 0});
  if (next_id != nullptr) *next_id = 1;
  return false;
}

/* -------------------- prepare_hlindex_meta -------------------- */

void prepare_hlindex_meta(dd::Table *table_def,
                               const KEY *key_info, uint key_count) {
  uint nvec = 0;
  for (const KEY *k = key_info, *end = k + key_count; k < end; ++k)
    if (k->flags & HA_VECTOR) nvec++;
  if (nvec == 0) return;

  const KEY *vec_keys = key_info + key_count - nvec;
  dd::Properties_impl p;
  p.set("count", dd::String_type(std::to_string(nvec)));
  p.set("next_id", dd::String_type(std::to_string(nvec)));

  char kbuf[32];
  for (uint i = 0; i < nvec; i++) {
    const KEY &k = vec_keys[i];
    snprintf(kbuf, sizeof(kbuf), "name_%u", i);
    p.set(dd::String_type(kbuf), dd::String_type(k.name));
    snprintf(kbuf, sizeof(kbuf), "id_%u", i);
    p.set(dd::String_type(kbuf), dd::String_type(std::to_string(i)));
    snprintf(kbuf, sizeof(kbuf), "m_%u", i);
    p.set(dd::String_type(kbuf), dd::String_type(std::to_string(k.vector_m)));
    snprintf(kbuf, sizeof(kbuf), "dist_%u", i);
    p.set(dd::String_type(kbuf),
          dd::String_type(std::to_string(k.vector_distance)));
    snprintf(kbuf, sizeof(kbuf), "ef_%u", i);
    p.set(dd::String_type(kbuf),
          dd::String_type(std::to_string(k.vector_ef_construction)));
    snprintf(kbuf, sizeof(kbuf), "col_%u", i);
    p.set(dd::String_type(kbuf),
          dd::String_type(std::to_string(k.key_part->fieldnr + 1)));
  }
  table_def->options().set("__hlindexes__", p.raw_string());
}

/* -------------------- finalize_hlindex_meta -------------------- */

bool finalize_hlindex_meta(dd::Table *dd_table, const KEY *key_info,
                           uint keys, uint total_keys) {
  assert(total_keys >= keys);
  const uint nvec = total_keys - keys;
  if (nvec == 0) return false;

  if (nvec == 1) {
    /* Single-index: store bare key name for backward compat. */
    dd_table->options().set("__hlindexes__",
                            dd::String_type(key_info[keys].name));
    return false;
  }

  /* Multi-index: structured format. */
  dd::Properties_impl p;
  p.set("count", dd::String_type(std::to_string(nvec)));
  p.set("next_id", dd::String_type(std::to_string(nvec)));

  char kbuf[32];
  for (uint i = 0; i < nvec; i++) {
    const KEY &k = key_info[keys + i];
    assert(k.flags & HA_VECTOR);
    snprintf(kbuf, sizeof(kbuf), "name_%u", i);
    p.set(dd::String_type(kbuf), dd::String_type(k.name));
    snprintf(kbuf, sizeof(kbuf), "id_%u", i);
    p.set(dd::String_type(kbuf),
          dd::String_type(std::to_string(k.vector_index_id)));
  }
  dd_table->options().set("__hlindexes__", p.raw_string());
  return false;
}

/* -------------------- create_all_hlindexes -------------------- */

int create_all_hlindexes(THD *thd, TABLE_SHARE *share, TABLE *table,
                         dd::Table *table_def, const char *db_name,
                         uint64_t old_table_id) {
  const uint nvec = share->hlindexes();
  if (nvec == 0) return 0;

  KEY *vec_keys = share->key_info + share->keys;
  for (KEY *k = vec_keys, *end = k + nvec; k < end; ++k) {
    k->vector_index_id = static_cast<uint>(k - vec_keys);
    table->hlindex_active_slot = static_cast<uint>(k - vec_keys);
    if (create_table(thd, k, table_def, table, db_name, old_table_id))
      return 1;
  }
  return 0;
}

/* -------------------- fill_key_from_enriched -------------------- */

void fill_key_from_enriched(TABLE_SHARE *share, uint nr,
                            const hlindex_meta_entry &entry,
                            const dd::String_type &key_name) {
  KEY *vec_key = &(share->key_info[nr]);
  KEY_PART_INFO *key_part = vec_key->key_part;
  MEM_ROOT *mem_root = &(share->mem_root);

  vec_key->flags = HA_VECTOR;
  vec_key->name =
      strmake_root(mem_root, key_name.c_str(), key_name.length() + 1);
  vec_key->vector_index_id = entry.id;
  vec_key->algorithm = HA_KEY_ALG_BTREE;
  vec_key->is_algorithm_explicit = false;
  vec_key->is_visible = true;
  vec_key->user_defined_key_parts = 1;
  vec_key->parser = nullptr;
  vec_key->engine_attribute = {nullptr, 0};
  vec_key->secondary_engine_attribute = {nullptr, 0};
  vec_key->comment = {nullptr, 0};
  vec_key->vector_m = entry.m;
  vec_key->vector_distance = entry.dist;
  vec_key->vector_ef_construction = entry.ef;

  key_part->fieldnr = entry.col;
  Field *field = key_part->field = share->field[key_part->fieldnr - 1];
  key_part->key_part_flag = 0;
  key_part->length = field->key_length();
  key_part->offset = field->offset(share->default_values);
  key_part->type = field->key_type();
  key_part->bin_cmp = ((field->real_type() != MYSQL_TYPE_VARCHAR &&
                        field->real_type() != MYSQL_TYPE_STRING) ||
                       (field->charset()->state & MY_CS_BINSORT));
  vec_key->key_length = key_part->length;
  share->keynames.type_names[nr] = vec_key->name;
  share->keys_in_use.set_bit(nr);
  share->visible_indexes.set_bit(nr);
}

}  // namespace vidx
