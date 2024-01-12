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

#include "sql/outline/outline.h"
#include "sql/sql_class.h"

namespace im {

/**
  Convert the outline type to category.

  @param[in]    type        Outline type
  @retval       category    Outline category in
                             outline cache container
*/
static Outline_category to_category(Outline_type type) {
  DBUG_ENTER("to_category");
  switch (type) {
    case Outline_type::INDEX_IGNORE:
    case Outline_type::INDEX_USE:
    case Outline_type::INDEX_FORCE:
      DBUG_RETURN(Outline_category::CATEGORY_INDEX);
    case Outline_type::OPTIMIZER:
      DBUG_RETURN(Outline_category::CATEGORY_OPTIMIZER);
    case Outline_type::UNKNOWN:
      assert(0);
  }
  assert(0);
  DBUG_RETURN(Outline_category::CATEGORY_COUNT);
}
/**
  Compare the outline according to position

  @param[in]    first     Outline
  @param[in]    second    Outline

  @retval       true      Less than
  @retval       false     More than
*/
static bool compare_outline(const Outline *first, const Outline *second) {
  return first->get_pos() < second->get_pos();
}

/**
  Build the index array by hint column in table.
*/
void Outline::build_index_hint() {
  DBUG_ENTER("Outline::build_index_hint");
  if (m_type != Outline_type::OPTIMIZER) {
    split<String_outline, Index_array, true>(m_hint.c_str(),
                                             INDEX_HINT_SEPARATOR, &m_indexes);
    assert(m_indexes.size() > 0);
  } else {
    assert(m_indexes.size() == 0);
  }

  DBUG_VOID_RETURN;
}

/**
  Evoke index hint instance, and add into list.

  @param[in]        mem_root    memory allocator
  @param[in/out]    list        container
*/
void Outline::evoke_index_hint(MEM_ROOT *mem_root, List<Index_hint> *list) {
  DBUG_ENTER("Outline::revoke_index_hint");
  assert(m_type != Outline_type::OPTIMIZER);

  for (uint i = 0; i < m_indexes.size(); i++) {
    String_outline &index = m_indexes[i];
    Index_hint *hint = new (mem_root) Index_hint(
        strmake_root(mem_root, index.c_str(), index.length()), index.length());
    hint->type = static_cast<enum index_hint_type>(m_type);
    hint->clause = m_scope.mask;
    list->push_back(hint);
  }
  inc_hit();
  DBUG_VOID_RETURN;
}
/**
  Evoke optimizer hint instance, and add into list.

  @param[in]        mem_root    memory allocator
  @param[in/out]    list        container
*/
void Outline::evoke_optimizer_hint(MEM_ROOT *mem_root,
                                   List<Lex_optimizer_hint> *list) {
  DBUG_ENTER("Outline::revoke_optimizer_hint");
  assert(m_type == Outline_type::OPTIMIZER);

  size_t len = m_hint.length();
  char *str = strmake_root(mem_root, m_hint.c_str(), len);
  Lex_optimizer_hint *hint = new (mem_root) Lex_optimizer_hint(m_pos, str, len);
  list->push_back(hint);
  inc_hit();
  DBUG_VOID_RETURN;
}
/**
  Insert outline into statement_outline.

  @param[in]      outline       outline
*/
void Statement_outline::insert(Outline *outline) {
  DBUG_ENTER("Statement_outline::insert");
  m_outlines.push_back(outline);
  std::sort(m_outlines.begin(), m_outlines.end(), compare_outline);
  DBUG_VOID_RETURN;
}

/**
  Delete the outline from array by id;

  @param[in]        outline id
  @retval           affected rows
*/
size_t Statement_outline::delete_by_id(ulonglong id) {
  size_t num = 0;
  for (auto it = m_outlines.begin(); it != m_outlines.end();) {
    Outline *outline = *it;
    if (outline->get_id() == id) {
      destroy_object<Outline>(outline);
      it = m_outlines.erase(it);
      num++;
    } else {
      it++;
    }
  }
  std::sort(m_outlines.begin(), m_outlines.end(), compare_outline);
  return num;
}

/* Destroy all the outlines in arraylist */
Statement_outline::~Statement_outline() {
  for (Outline_vector::iterator it = m_outlines.begin(); it != m_outlines.end();
       it++) {
    destroy_object(*it);
  }
  m_outlines.clear();
}

/**
  Contructor of System_outline

  Init all the statement outline maps, sizes, locks.
*/
System_outline::System_outline(size_t partition)
    : m_partition(partition),
      m_elements(partition * static_cast<size_t>(Category::CATEGORY_COUNT)),
      m_maps(key_memory_outline),
      m_sizes(key_memory_outline),
      m_locks(key_memory_outline) {
  for (size_t i = 0; i < m_elements; i++) {
    /* Init statement outline map array */
    Statement_outline_map *elem_map =
        allocate_outline_object<Statement_outline_map>(key_memory_outline);
    m_maps.push_back(elem_map);

    /* Init size array */
    std::atomic_ullong *elem_atomic =
        allocate_outline_object<std::atomic_ullong>();
    m_sizes.push_back(elem_atomic);

    /* Init locks array */
    mysql_rwlock_t *lock = allocate_outline_object<mysql_rwlock_t>();
    mysql_rwlock_init(key_rwlock_outline, lock);

    m_locks.push_back(lock);
  }
}

/**
  Destroy all the container elements.
*/
System_outline::~System_outline() {
  for (size_t i = 0; i < m_elements; i++) {
    for (Statement_outline_map::const_iterator it = m_maps[i]->cbegin();
         it != m_maps[i]->cend(); it++) {
      destroy_object<Statement_outline>(
          const_cast<Statement_outline *>(it->second));
    }
    destroy_object(m_maps[i]);
    destroy_object(m_sizes[i]);
    mysql_rwlock_destroy(m_locks[i]);
    destroy_object(m_locks[i]);
  }
  m_maps.clear();
  m_sizes.clear();
  m_locks.clear();
}

/**
  Fill the outlines into maps, clear all if force_clean is true.

  @param[in]      records       outline records container.
  @param[in]      force_clean   Whether clear all maps.
*/
void System_outline::fill_records(Conf_records *records, bool force_clean) {
  size_t begin;
  Outline_group group;
  group.classify_hint(records);
  /* fill index hint */
  begin = get_begin(Category::CATEGORY_INDEX);
  for (size_t i = begin; i < m_partition + begin; i++) {
    Lock_helper lock(m_locks[i], true);
    assert(lock.effect());
    if (force_clean) clear(i);
    for (auto it : group.index_outlines) {
      add_outline(i, it);
    }
  }

  /* fill optimizer hint */
  begin = get_begin(Category::CATEGORY_OPTIMIZER);
  for (size_t i = begin; i < m_partition + begin; i++) {
    Lock_helper lock(m_locks[i], true);
    assert(lock.effect());
    if (force_clean) clear(i);
    for (auto it : group.optimizer_outlines) {
      add_outline(i, it);
    }
  }
}
/**
  Clear the statement outline objects from maps at pos.

  @param[in]      pos       partition position
*/
void System_outline::clear(size_t pos) {
  Statement_outline_map *map = m_maps[pos];
  for (Statement_outline_map::const_iterator it = map->cbegin();
       it != map->cend(); it++) {
    destroy_object<Statement_outline>(
        const_cast<Statement_outline *>(it->second));
  }
  map->clear();
}

/**
  Aggregate the outlines that has the same outline id within
  all partition maps.

  @param[out]   container       Result container
*/
void System_outline::aggregate_outline(
    Outline_show_result_container *container) {
  typename Outline_show_result_container::const_iterator orc_it;
  for (size_t i = 0; i < m_elements; i++) {
    /* Loop all the partition */
    Lock_helper lock(m_locks[i], false);
    assert(lock.effect());
    for (auto it = m_maps[i]->begin(); it != m_maps[i]->end(); it++) {
      /* Loop all the statement outline in map */
      Statement_outline *stmt_outline =
          const_cast<Statement_outline *>(it->second);
      for (auto outline_ptr : stmt_outline->outlines()) {
        /* Loop all the outline in a statement outline */
        orc_it = container->find(outline_ptr->get_id());
        if (orc_it == container->end()) {
          /* IF not found */
          Outline_show_result result(outline_ptr);
          container->insert(std::pair<ulonglong, Outline_show_result>(
              outline_ptr->get_id(), result));
        } else {
          /* IF found */
          Outline_show_result &result =
              const_cast<Outline_show_result &>(orc_it->second);
          result.aggregate(outline_ptr);
        }
      }
    }
  }
}
/**
  Delete the outline from all maps by id

  @param[in]        outline id

  @retval           affected rows
*/
size_t System_outline::delete_outline(ulonglong id) {
  size_t num = 0;
  for (size_t i = 0; i < m_elements; i++) {
    Lock_helper lock(m_locks[i], true);
    assert(lock.effect());
    for (auto it = m_maps[i]->begin(); it != m_maps[i]->end();) {
      Statement_outline *stmt_outline =
          const_cast<Statement_outline *>(it->second);
      num += stmt_outline->delete_by_id(id);
      /* Erase the hash element if statement outline all has been deleted. */
      if (stmt_outline->outlines().size() == 0) {
        destroy_object<Statement_outline>(stmt_outline);
        it = m_maps[i]->erase(it);
      } else {
        it++;
      }
    }
    lock.unlock();
  }

  return num;
}

/**
  Add the outling into m_maps at pos.

  @param[in]      pos       partition position
  @param[in]      outline   outline object

  @retval         true      Failure
  @retval         false     Success
*/
bool System_outline::add_outline(size_t pos, const Outline *outline) {
  Statement_outline *stmt_outline;
  DBUG_ENTER("System_outline::add_outline");
  if (!(stmt_outline =
            find(pos, outline->get_schema(), outline->get_digest()))) {
    stmt_outline = allocate_outline_object<Statement_outline>(
        outline->get_schema().c_str(), outline->get_digest().c_str());
    m_maps[pos]->insert(Statement_outline_map::value_type(
        Statement_outline_map::key_type(outline->get_schema(),
                                        outline->get_digest()),
        stmt_outline));
  }

  Outline *clone = allocate_outline_object<Outline>(*outline);
  stmt_outline->insert(clone);
  DBUG_RETURN(false);
}

void prepare_hints(THD *thd, Table_ref *table, Outline *outline) {
  if (!table->index_hints)
    table->index_hints = new (thd->mem_root) List<Index_hint>();
  outline->evoke_index_hint(thd->mem_root, table->index_hints);
}

void prepare_hints(THD *thd, Query_block *select_lex, Outline *outline) {
  if (!select_lex->outline_optimizer_list)
    select_lex->outline_optimizer_list =
        new (thd->mem_root) List<Lex_optimizer_hint>();

  outline->evoke_optimizer_hint(thd->mem_root,
                                select_lex->outline_optimizer_list);
}

/**
  Iterate the next Table_ref from global table list.

  @param[in]      table_list
  @retval         table_list
*/
static Table_ref *iterate_next(Table_ref *table_list) {
  return table_list->next_global;
}

/**
  Iterate the next Query_block from global SELECT LEX list.

  @param[in]      Query_block
  @retval         Query_block
*/
static Query_block *iterate_next(Query_block *select_lex) {
  return select_lex->next_qb;
}
/**
  Find the outlines by schema and digest, and create hint
  add into Table_ref or Query_block if found.

  @param[in]      thd           thread context
  @param[in/out]  T             Table_ref or Query_block list
  @param[in]      schema        schema name
  @param[in]      digest        statement digest
*/
template <typename T, Outline_category outline_category>
void System_outline::find_and_fill_hints(THD *thd, T *lex_objects,
                                         String_outline &schema,
                                         String_outline &digest) {
  size_t pos;
  Statement_outline *stmt_outline;
  T *element;
  DBUG_ENTER("System_outline::find_and_fill_hints");
  pos = get_pos<outline_category>(thd->thread_id());
  assert(pos < m_elements);

  Lock_helper lock(m_locks[pos], false);
  assert(lock.effect());
  if ((stmt_outline = find(pos, schema, digest))) {
    const Statement_outline_vector &outlines = stmt_outline->outlines();
    /* position in Table_ref/Query_block global list */
    uint element_nr = 1;
    uint outline_nr = 0;
    element = lex_objects;
    uint outline_size = outlines.size();
    /**
      Loop the all elements list and statement outline array
      that is order at the same time.
    */
    while (element && (outline_nr < outline_size)) {
      /* Index Hint position on table in statement */
      ulonglong hint_pos = outlines[outline_nr]->get_pos();
      /* Table position in table_list equal the hint position in statement */
      if (element_nr == hint_pos) {
        prepare_hints(thd, element, outlines[outline_nr]);
        outline_nr++;
      } else if (element_nr < hint_pos) {
        element = iterate_next(element);
        element_nr++;
      } else if (element_nr > hint_pos) {
        outline_nr++;
      }
    }
    /* Accumulated the overflow hint which is not in table_list */
    while (outline_nr < outline_size) {
      outlines[outline_nr]->inc_overflow();
      outline_nr++;
    }
  }
  DBUG_VOID_RETURN;
}

/* Template specialization */
template void System_outline::find_and_fill_hints<
    Table_ref, Outline_category::CATEGORY_INDEX>(THD *thd,
                                                 Table_ref *lex_objects,
                                                 String_outline &schema,
                                                 String_outline &digest);

template void System_outline::find_and_fill_hints<
    Query_block, Outline_category::CATEGORY_OPTIMIZER>(THD *thd,
                                                       Query_block *lex_objects,
                                                       String_outline &schema,
                                                       String_outline &digest);
/**
  Classify the different hint type, and fill into vector.

  1) One is index hint.
  2) The other is optimizer hint.

  @param[in]    records     All kinds of outline
 */
void Outline_group::classify_hint(Conf_records *records) {
  DBUG_ENTER("Outline_group::classify_hint");
  for (Conf_records::const_iterator it = records->cbegin();
       it != records->cend(); it++) {
    Outline_record *r = dynamic_cast<Outline_record *>(*it);
    Outline_category c = to_category(r->type);
    if (c == Outline_category::CATEGORY_INDEX)
      index_outlines.push_back(allocate_outline_object<Outline>(r));
    else if (c == Outline_category::CATEGORY_OPTIMIZER)
      optimizer_outlines.push_back(allocate_outline_object<Outline>(r));
    else {
      assert(0);
    }
  }
  DBUG_VOID_RETURN;
}

/**
  Destructor of two container

  Should destroy the outlines from allocate_outline_object();
*/
Outline_group::~Outline_group() {
  for (auto it : index_outlines) {
    destroy_object<Outline>(it);
  }
  index_outlines.clear();
  for (auto it : optimizer_outlines) {
    destroy_object<Outline>(it);
  }
  optimizer_outlines.clear();
}

} /*namespace im */
