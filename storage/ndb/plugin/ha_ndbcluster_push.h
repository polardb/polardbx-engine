/*
   Copyright (c) 2011, 2019, Oracle and/or its affiliates. All rights reserved.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "my_dbug.h"
#include "sql/sql_bitmap.h"
#include "storage/ndb/include/ndbapi/NdbDictionary.hpp"
#include "storage/ndb/plugin/ha_ndbcluster.h"

class NdbTransaction;
class NdbQueryBuilder;
class NdbQueryOperand;
class NdbQueryOperationDef;
class NdbQueryOptions;
class ndb_pushed_builder_ctx;
struct NdbError;

namespace AQP {
class Join_plan;
class Table_access;
}  // namespace AQP

void ndbcluster_build_key_map(const NdbDictionary::Table *table,
                              const NDB_INDEX_DATA &index, const KEY *key_def,
                              uint ix_map[]);

/**
 * This type is used in conjunction with AQP::Join_plan and represents a set
 * of the table access operations of the join plan.
 * Had to subclass Bitmap as the default Bitmap<64> c'tor didn't initialize its
 * map.
 */
typedef Bitmap<(MAX_TABLES > 64 ? MAX_TABLES : 64)> table_bitmap;

class ndb_table_access_map : public table_bitmap {
 public:
  explicit ndb_table_access_map() : table_bitmap() {}

  void add(const ndb_table_access_map
               &table_map) {  // Require const_cast as signature of class
                              // Bitmap::merge is not const correct
    merge(table_map);
  }
  void add(uint table_no) { set_bit(table_no); }

  bool contain(const ndb_table_access_map &table_map) const {
    return table_map.is_subset(*this);
  }
  bool contain(uint table_no) const { return is_set(table_no); }

  uint first_table(uint start = 0) const;
  uint last_table(uint start = MAX_TABLES) const;

};  // class ndb_table_access_map

/** This class represents a prepared pushed (N-way) join operation.
 *
 *  It might be instantiated multiple times whenever the query,
 *  or this subpart of the query, is being (re-)executed by
 *  ::createQuery() or it's wrapper method
 *  ha_ndbcluster::create_pushed_join().
 */
class ndb_pushed_join {
 public:
  explicit ndb_pushed_join(const ndb_pushed_builder_ctx &builder_ctx,
                           const NdbQueryDef *query_def);

  ~ndb_pushed_join();

  /**
   * Check that this prepared pushed query matches the type
   * of operation specified by the arguments.
   */
  bool match_definition(int type,  // NdbQueryOperationDef::Type,
                        const NDB_INDEX_DATA *idx) const;

  /** Create an executable instance of this defined query. */
  NdbQuery *make_query_instance(NdbTransaction *trans,
                                const NdbQueryParamValue *keyFieldParams,
                                uint paramCnt) const;

  /** Get the number of pushed table access operations.*/
  uint get_operation_count() const { return m_operation_count; }

  /**
   * In a pushed join, fields in lookup keys and scan bounds may refer to
   * result fields of table access operation that execute prior to the pushed
   * join. This method returns the number of such references.
   */
  uint get_field_referrences_count() const { return m_field_count; }

  const NdbQueryDef &get_query_def() const { return *m_query_def; }

  /** Get the table that is accessed by the i'th table access operation.*/
  TABLE *get_table(uint i) const {
    DBUG_ASSERT(i < m_operation_count);
    return m_tables[i];
  }

  /**
   * This is the maximal number of fields in the key of any pushed table
   * access operation.
   */
  static const uint MAX_KEY_PART = MAX_KEY;
  /**
   * In a pushed join, fields in lookup keys and scan bounds may refer to
   * result fields of table access operation that execute prior to the pushed
   * join. This constant specifies the maximal number of such references for
   * a query.
   */
  static const uint MAX_REFERRED_FIELDS = 16;
  /**
   * For each table access operation in a pushed join, this is the maximal
   * number of key fields that may refer to the fields of the parent operation.
   */
  static const uint MAX_LINKED_KEYS = MAX_KEY;
  /**
   * This is the maximal number of table access operations there can be in a
   * single pushed join.
   */
  static const uint MAX_PUSHED_OPERATIONS = MAX_TABLES;

 private:
  const NdbQueryDef *const m_query_def;  // Definition of pushed join query

  /** This is the number of table access operations in the pushed join.*/
  uint m_operation_count;

  /** This is the tables that are accessed by the pushed join.*/
  TABLE *m_tables[MAX_PUSHED_OPERATIONS];

  /**
   * This is the number of referred fields of table access operation that
   * execute prior to the pushed join.
   */
  const uint m_field_count;

  /**
   * These are the referred fields of table access operation that execute
   * prior to the pushed join.
   */
  Field *m_referred_fields[MAX_REFERRED_FIELDS];
};  // class ndb_pushed_join

/**
 * Contains the context and helper methods used during ::make_pushed_join().
 *
 * Interacts with the AQP which provides interface to the query prepared by
 * the mysqld optimizer.
 *
 * Execution plans built for pushed joins are stored inside this builder
 * context.
 */
class ndb_pushed_builder_ctx {
  friend ndb_pushed_join::ndb_pushed_join(
      const ndb_pushed_builder_ctx &builder_ctx, const NdbQueryDef *query_def);

 public:
  ndb_pushed_builder_ctx(AQP::Table_access *table);
  ~ndb_pushed_builder_ctx();

  /**
   * Build the pushed query identified with 'is_pushable_with_root()'.
   * Returns:
   *   = 0: A NdbQueryDef has successfully been prepared for execution.
   *   > 0: Returned value is the error code.
   *   < 0: There is a pending NdbError to be retrieved with getNdbError()
   */
  int make_pushed_join(const ndb_pushed_join *&pushed_join);

  const NdbError &getNdbError() const;

 private:
  // 'pushability' is stored in AQP::Table_access::set_table_properties()
  enum join_pushability {
    PUSHABILITY_UNKNOWN = 0x00,  // Initial 'unknown' value, calculate it
    PUSHABILITY_KNOWN = 0x10,
    PUSHABLE_AS_PARENT = 0x01,
    PUSHABLE_AS_CHILD = 0x02
  };

  bool maybe_pushable(AQP::Table_access *table, join_pushability check);

  /**
   * Collect all tables which may be pushed together with 'root'.
   * Returns 'true' if anything is pushable.
   */
  bool is_pushable_with_root();

  bool is_pushable_as_child(AQP::Table_access *table);

  bool is_const_item_pushable(const Item *key_item,
                              const KEY_PART_INFO *key_part);

  bool is_field_item_pushable(AQP::Table_access *table, const Item *key_item,
                              const KEY_PART_INFO *key_part,
                              ndb_table_access_map &parents);

  void validate_join_nest(uint first_inner, ndb_table_access_map inner_nest);

  void remove_pushable(const AQP::Table_access *table);

  int optimize_query_plan();

  int build_query();

  void collect_key_refs(const AQP::Table_access *table,
                        const Item *key_refs[]) const;

  int build_key(const AQP::Table_access *table, const NdbQueryOperand *op_key[],
                NdbQueryOptions *key_options);

  uint get_table_no(const Item *key_item) const;

  ndb_table_access_map get_table_map(table_map external_map) const;

 private:
  const AQP::Join_plan &m_plan;
  AQP::Table_access *m_join_root;

  // Scope of tables covered by this pushed join
  ndb_table_access_map m_join_scope;

  // Scope of tables evaluated prior to 'm_join_root'
  // These are effectively const or params wrt. the pushed join
  ndb_table_access_map m_const_scope;

  // Set of tables in join scope requiring (index-)scan access
  ndb_table_access_map m_scan_operations;

  // Tables in this join-scope having remaining conditions not being pushed
  ndb_table_access_map m_has_pending_cond;

  // Number of internal operations used so far (unique lookups count as two).
  uint m_internal_op_count;

  uint m_fld_refs;
  Field *m_referred_fields[ndb_pushed_join::MAX_REFERRED_FIELDS];

  // Handle to the NdbQuery factory.
  // Possibly reused if multiple NdbQuery's are pushed.
  NdbQueryBuilder *m_builder;

  struct pushed_tables {
    pushed_tables()
        : m_inner_nest(),
          m_upper_nests(),
          m_outer_nest(),
          m_first_inner(0),
          m_last_inner(0),
          m_first_upper(-1),
          m_sj_nest(),
          m_key_parents(nullptr),
          m_ancestors(),
          m_parent(MAX_TABLES),
          m_op(nullptr) {}

    /**
     * As part of analyzing the pushability of each table, the 'join-nest'
     * structure is collected for the tables. The 'map' represent the id
     * of each table in the 'nests':
     *
     * - The inner-nest contain the set of all *preceding* table which
     *   this table has some INNER JOIN relation with. Either by the table(s)
     *   being directly referred by a inner-join condition on this table,
     *   or indirectly by being inner joined with one of the referred table(s).
     *
     *   Note that no result rows from any of the tables in the inner-nest can
     *   be produced if there is not a match on all join conditions between the
     *   tables in an inner-nest. (Except NULL complimented rows for
     *   entire inner-nest if the nest itself is outer joined.
     *   (-> has an upper-nest - see below.))
     *
     * - The upper-nest(s) are the set of tables which the tables in the
     *   inner-nest are outer joined with. There might be multiple levels
     *   of upper nesting (or outer joining), where m_upper_nests contain the
     *   aggregate of these nests as seen from this table.
     *
     * In the optimizer code, and this SPJ handler integration code, we may use
     * a parentized form to express the nest structures, like:
     * t1, (t2,t3,(t4)), which means:
     *
     * - t2 & t3 has the upper nest [t1], thus t2,t3 are outer joined with t1
     * - t2 & t3 is in same inner nest, thus they are inner joined with each
     * other.
     * - t4 has the upper nest [t2,t3], the aggregated upper nests
     * (m_upper_nests) for t4 will contain [t1,t2,t3]. (and outer joins with
     * these).
     *
     * eg. t3 will have the m_upper_nests=[t1], and m_inner_nest=[t2],
     * ('self' not included in inner_nest).
     *
     * Note that a table will be present in the m_inner_nest and/or m_upper_nest
     * even if the table is not join-pushed. 'Self' is not represented in
     * 'm_inner_nest'.
     */
    ndb_table_access_map m_inner_nest;
    ndb_table_access_map m_upper_nests;

    /**
     * The sum of the inner- and upper-nests is the 'embedding nests'
     * for the table. The join semantic for the tables in the
     * embedding nest is such that no result row can be created from
     * a row from this table without having a set of rows from all preceding
     * tables in the embedding nest. (Fulfilling any join conditions)
     *
     * The 'embedding nests' plays an important role when analyzing a
     * table for join pushability:
     *
     *  - Assume the previous nest structure: t1, (t2,t3,(t4))
     *  - t2, t3 both refer only t1 in their join conditions.
     *  - t4 refers both t2 and t3: 't4.a = t2.x and t4.b = t3.y'.
     *
     * Thus, the query has the dependency topology:
     *
     *                t1
     *               /  \
     *              t2  t3  (Not directly pushable, see below)
     *               \  /
     *                t4
     *
     * The SPJ implementation require the dependency topology for
     * a SPJ query to be a plain tree topology (implementation legacy).
     * Thus the query above is not directly pushable.
     *
     * We have two mechanisms for helping us in making such queries pushable:
     *  1) SPJ allows us to refer values from any ancestor tables.
     *     (grand-(grand-...)parents).
     *  2) A table is implicit dependend on any table in the embedding nests,
     *     even if no join condition is refering that table.
     *
     * For the query above we may use this to add an extra dependency from
     * t3 on t2. Furthermore t3's join condition on t1 is made a grand-parent
     * reference to t1, via t2:
     *
     *                          t1
     *                         //
     *                        t2|  <- t1 values passed via t2
     *                         \\  <- Added extra dependency on t2
     *    t2 values, via t3 -> |t3
     *                         //
     *                        t4   (Has a join condition on t2 & t3)
     *
     * Thus we have transformed the query into a pushable query.
     * The SPJ handler integration, in combination with the SPJ API,
     * will add such extra parent dependencies where required, and
     * allowed by checking that references are from within the
     * embedding_nests().
     */
    ndb_table_access_map embedding_nests() const {
      ndb_table_access_map nests(m_inner_nest);
      nests.add(m_upper_nests);
      return nests;
    }

    /**
     * In additions to the above inner- and upper-nest dependencies, there
     * may be dependencies on tables outside of the embedding nests.
     * Such dependencies are caused by explicit references to non-embedded
     * tables from the join conditions.
     * One such case is the nest structure:  t1,(t2),(t3),(t4), where t4 has the
     * same t2,t3 depending join condition as above: 't4.a = t2.x and t4.b =
     * t3.y'. (Referring the outer joined t2 and t3, which are not in the
     * embedding_nests() of t4.)
     *
     * Note that this is a perfectly legal join condition, possibly a bit
     * unusual though.
     *
     * If we employed the same rewrite as above (Adding a dependency
     * on (t2) from (t3)) we would effectively also add the extra condition
     * 't2 IS NOT NULL, which would have changed the semantic of the query.
     * Thus, pushing this query could have resulted in an incorrect result
     * set being returned.
     *
     *                 t1
     *                /  \
     *              (t2)(t3)  --> Can't be made join pushable!
     *                \  /
     *                (t4)
     *
     * An exception exists to the above: What if t3 already has a join
     * dependency on (the outer joined) t2?, like the join condition:
     * 'on t3.a = t1.x and t3.b = t2.y'. That would introduce an explicit
     * dependency between t2 and t3, similar to the one we added in an
     * example further up. For t3 it also implies ''t2 IS NOT NULL'.
     *   -> Query becomes pushable.
     *
     * The outer_nest map for each table is used to keep track of known
     * dependencies to tables outside of the embedding nests.
     * In the case above the 'outer' references to t2 from t3 will result in
     * t3.m_outer_nest = [t2] being set. When analyzing t4 pushability,
     * we will find that the parent table t3 already has the required
     * dependency on t2. Thus t4 becomes pushable.
     *
     * Note that m_outer_nest is only set on the first_inner table in the nest.
     * All outer references made from any tables in the nest is aggregated at
     * the first_inner table.
     */
    ndb_table_access_map m_outer_nest;

    // Return all inner-, upper- and outer-tables available as 'parents'
    ndb_table_access_map parent_nests() const {
      ndb_table_access_map nests(m_inner_nest);
      nests.add(m_upper_nests);
      nests.add(m_outer_nest);
      return nests;
    }
    bool isOuterJoined(pushed_tables &parent) const {
      return m_first_inner > parent.m_first_inner;
    }
    bool isInnerJoined(pushed_tables &parent) const {
      // return !isOuterJoined(parent);
      return m_first_inner <= parent.m_first_inner;
    }

    /**
     * Some additional join_nest structure for navigating the nest hiararcy:
     */
    uint m_first_inner;  // The first table represented in current m_inner_nest
    uint m_last_inner;   // The last table in current m_inner_nest
    int m_first_upper;   // The first table in the upper_nest of this table.

    /**
     * A similar, but simpler, nest topology exists for the semi-joins.
     * Tables in the semi-join nest are semi joined with any tables outside
     * the sj_nest.
     */
    ndb_table_access_map m_sj_nest;

    /**
     * For each KEY_PART referred in the join conditions, we find the set of
     * possible parent tables for each non-const_item KEY_PART.
     * In addition to the parent table directly referred by the join condition,
     * any tables *in same join nest*, available by usage of
     * equality sets, are also added as a possible parent.
     *
     * The set of 'key_parents[]' are collected when analyzing query for
     * join pushability, and saved for later usage by ::optimize_query_plan(),
     * which will select the actuall m_parent to be used for each table.
     */
    ndb_table_access_map *m_key_parents;

    /**
     * The m_ancestor map serves two slightly different purposes:
     *
     * 1) During ::optimize_query_plan() we may enforce parent
     *    dependencies on the ancestor tables by setting the ancestors.
     *    Such enforcement means that no rows from this table will be
     *    requested until result row(s) are available from all ancestors.
     *    Normally any parent tables referred by the join conditions are
     *    added to m_ancestors at this stage.
     *
     * 2) After ::optimize_query_plan() m_ancestors will contain all
     *    ancestor tables reachable through the m_parent chain
     */
    ndb_table_access_map m_ancestors;

    /**
     * The actual parent as choosen by ::optimize_query_plan()
     */
    uint m_parent;

    // The NdbQueryOperationDef produced when pushing this table
    const NdbQueryOperationDef *m_op;

  } m_tables[MAX_TABLES];

};  // class ndb_pushed_builder_ctx
