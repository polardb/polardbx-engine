/* Copyright (c) 2024, Alibaba and/or its affiliates.

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

#include "sql/dd/impl/system_views/vector_indexes.h"

namespace dd {
namespace system_views {

const Vector_indexes &Vector_indexes::instance() {
  static Vector_indexes *s_instance = new Vector_indexes();
  return *s_instance;
}

/*
  Build the VECTOR_INDEXES view definition.

  Data flow:  mysql.tables (base, with __hlindexes__ option)
           -> JOIN mysql.tables (hlidx = vidx_<se_private_id>_0)
           -> JOIN mysql.columns (vcol at __vector_column__ ordinal)

  All vector index parameters are extracted from DD options of the
  auxiliary table (distance, M, ef_construction) and the base table
  (index name).  No in-memory cache or open-table path is involved.
*/
Vector_indexes::Vector_indexes() {
  m_target_def.set_view_name(view_name());

  m_target_def.add_field(FIELD_TABLE_CATALOG, "TABLE_CATALOG", "'def'");
  m_target_def.add_field(FIELD_TABLE_SCHEMA, "TABLE_SCHEMA",
                         "sch.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_TABLE_NAME, "TABLE_NAME",
                         "base.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(
      FIELD_INDEX_NAME, "INDEX_NAME",
      "GET_DD_PROPERTY_KEY_VALUE(hlidx.options,'__vector_index_name__')");
  // Auxiliary table name (vidx_<se_private_id>_0). Useful for DBA diagnostics:
  // correlating with innodb_table_stats, locating tablespace files, etc.
  m_target_def.add_field(FIELD_HLINDEX_TABLE_NAME, "HLINDEX_TABLE_NAME",
                         "hlidx.name");
  m_target_def.add_field(FIELD_COLUMN_NAME, "COLUMN_NAME", "vcol.name");
  m_target_def.add_field(FIELD_ALGORITHM, "ALGORITHM", "'HNSW'");
  // __vector_distance__ stores the distance_kind enum value (EUCLIDEAN=0,
  // COSINE=1, INNER_PRODUCT=2; see vidx_func.h). Map it back to the name used
  // in distance_names[] (vidx_common.h).
  m_target_def.add_field(
      FIELD_METRIC_TYPE, "METRIC_TYPE",
      "CASE GET_DD_PROPERTY_KEY_VALUE(hlidx.options,'__vector_distance__') "
      "WHEN '0' THEN 'EUCLIDEAN' "
      "WHEN '1' THEN 'COSINE' "
      "WHEN '2' THEN 'INNER_PRODUCT' "
      "ELSE 'UNKNOWN' END");
  // The vector column byte length divided by sizeof(float)=4 yields the
  // dimension; use integer division to avoid a fractional result.
  m_target_def.add_field(FIELD_DIMENSION, "DIMENSION",
                         "vcol.char_length DIV 4");
  m_target_def.add_field(
      FIELD_M, "M", "GET_DD_PROPERTY_KEY_VALUE(hlidx.options,'__vector_m__')");
  m_target_def.add_field(
      FIELD_EF_CONSTRUCTION, "EF_CONSTRUCTION",
      "GET_DD_PROPERTY_KEY_VALUE(hlidx.options,'__vector_ef_construction__')");
  m_target_def.add_field(FIELD_QUANTIZE_TYPE, "QUANTIZE_TYPE", "'SQ16'");

  m_target_def.add_from("mysql.tables base");
  m_target_def.add_from("JOIN mysql.schemata sch ON base.schema_id = sch.id");
  /*
    The auxiliary table is named vidx_<se_private_id>_<n>, so the underscores
    of the pattern have to be escaped: without ESCAPE they are LIKE wildcards
    and the pattern of a table whose id is a prefix of another one matches the
    auxiliary tables of that other table as well (vidx_1_% eats vidx_12_0).
  */
  m_target_def.add_from(
      "JOIN mysql.tables hlidx "
      "  ON hlidx.name LIKE CONCAT('vidx|_', base.se_private_id, '|_%') "
      "     ESCAPE '|' "
      " AND hlidx.schema_id = base.schema_id "
      " AND hlidx.engine = 'InnoDB'");
  m_target_def.add_from(
      "JOIN mysql.columns vcol "
      "  ON vcol.table_id = base.id "
      " AND vcol.ordinal_position = "
      "     GET_DD_PROPERTY_KEY_VALUE(hlidx.options,'__vector_column__')");

  m_target_def.add_where(
      "GET_DD_PROPERTY_KEY_VALUE(base.options,'__hlindexes__') IS NOT NULL");
  m_target_def.add_where("AND CAN_ACCESS_TABLE(sch.name, base.name)");
  m_target_def.add_where("AND IS_VISIBLE_DD_OBJECT(base.hidden)");
}

}  // namespace system_views
}  // namespace dd
