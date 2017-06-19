/* Copyright (c) 2016, 2017, Oracle and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

/** @file storage/temptable/include/temptable/cursor.h
TempTable index cursor. */

#ifndef TEMPTABLE_CURSOR_H
#define TEMPTABLE_CURSOR_H

#include "temptable/column.h"        /* temptable::Columns */
#include "temptable/containers.h"    /* temptable::*container */
#include "temptable/indexed_cells.h" /* temptable::Indexed_cells */
#include "temptable/row.h"           /* temptable::Row */
#include "temptable/storage.h"       /* temptable::Storage::Element */
#include "my_dbug.h"              /* DBUG_ASSERT() */

namespace temptable {

/** A cursor for iterating over an `Index`. */
class Cursor {
 public:
  /** Constructor. */
  Cursor();

  /** Constructor from `Hash_duplicates` iterator. */
  explicit Cursor(
      /** [in] Iterator for cursor initial position. */
      const Hash_duplicates_container::const_iterator& iterator);

  /** Constructor from `Tree` iterator. */
  explicit Cursor(
      /** [in] Iterator for cursor initial position. */
      const Tree_container::const_iterator& iterator);

  /** Check if the cursor is positioned.
   * @return true if positioned */
  bool is_positioned() const;

  /** Unposition the cursor. */
  void unposition();

  /** Get the indexed cells of the current cursor position.
   * @return indexed cells */
  const Indexed_cells& indexed_cells() const;

  /** Get a pointer to the row of the current cursor position.
   * @return a pointer to a row */
  Storage::Element* row() const;

  /** Export the row that is pointed to by this cursor in mysql write_row()
   * format. */
  void export_row_to_mysql(
      /** [in] Metadata for the columns that constitute the row. */
      const Columns& columns,
      /** [out] Destination buffer to write the row to. */
      unsigned char* mysql_row,
      /** [in] Presumed length of the mysql row in bytes. */
      size_t mysql_row_length) const;

  /** Get the underlying hash iterator. The cursor must be on a hash index.
   * @return iterator */
  const Hash_duplicates_container::const_iterator& hash_iterator() const;

  /** Get the underlying tree iterator. The cursor must be on a tree index.
   * @return iterator */
  const Tree_container::const_iterator& tree_iterator() const;

  /** Copy-assign from another cursor.
   * @return *this */
  Cursor& operator=(
      /** [in] Source cursor to assign from. */
      const Cursor& rhs);

  /** Advance the cursor forward.
   * @return *this */
  Cursor& operator++();

  /** Recede the cursor backwards.
   * @return *this */
  Cursor& operator--();

  /** Check if equal to another cursor.
   * @return true if equal */
  bool operator==(
      /** [in] Cursor to compare with. */
      const Cursor& other) const;

  /** Check if not equal to another cursor.
   * @return true if not equal */
  bool operator!=(
      /** [in] Cursor to compare with. */
      const Cursor& other) const;

 private:
  /** Type of the index the cursor iterates over. */
  enum class Type : uint8_t {
    /** Hash index. */
    HASH,
    /** Tree index. */
    TREE,
  };

  /** Type of the index the cursor iterates over. */
  Type m_type;

  /** Indicate whether the cursor is positioned. */
  bool m_is_positioned;

  /** Iterator that is used if `m_type == Type::HASH`. */
  Hash_duplicates_container::const_iterator m_hash_iterator;

  /** Iterator that is used if `m_type == Type::TREE`. */
  Tree_container::const_iterator m_tree_iterator;
};

/* Implementation of inlined methods. */

inline Cursor::Cursor() : m_is_positioned(false) {}

inline Cursor::Cursor(const Hash_duplicates_container::const_iterator& iterator)
    : m_type(Type::HASH), m_is_positioned(true), m_hash_iterator(iterator) {}

inline Cursor::Cursor(const Tree_container::const_iterator& iterator)
    : m_type(Type::TREE), m_is_positioned(true), m_tree_iterator(iterator) {}

inline bool Cursor::is_positioned() const { return m_is_positioned; }

inline void Cursor::unposition() { m_is_positioned = false; }

inline const Indexed_cells& Cursor::indexed_cells() const {
  DBUG_ASSERT(m_is_positioned);

  if (m_type == Type::HASH) {
    return *m_hash_iterator;
  }

  DBUG_ASSERT(m_type == Type::TREE);
  return *m_tree_iterator;
}

inline Storage::Element* Cursor::row() const {
  DBUG_ASSERT(m_is_positioned);

  if (m_type == Type::HASH) {
    return m_hash_iterator->row();
  }

  DBUG_ASSERT(m_type == Type::TREE);
  return m_tree_iterator->row();
}

inline void Cursor::export_row_to_mysql(const Columns& columns,
                                        unsigned char* mysql_row,
                                        size_t mysql_row_length) const {
  DBUG_ASSERT(m_is_positioned);

  if (m_type == Type::HASH) {
    return m_hash_iterator->export_row_to_mysql(columns, mysql_row,
                                                mysql_row_length);
  }

  DBUG_ASSERT(m_type == Type::TREE);
  return m_tree_iterator->export_row_to_mysql(columns, mysql_row,
                                              mysql_row_length);
}

inline const Hash_duplicates_container::const_iterator& Cursor::hash_iterator()
    const {
  DBUG_ASSERT(m_type == Type::HASH);
  return m_hash_iterator;
}

inline const Tree_container::const_iterator& Cursor::tree_iterator() const {
  DBUG_ASSERT(m_type == Type::TREE);
  return m_tree_iterator;
}

inline Cursor& Cursor::operator=(const Cursor& rhs) {
  m_is_positioned = rhs.m_is_positioned;

  m_type = rhs.m_type;

  if (m_is_positioned) {
    if (m_type == Type::HASH) {
      m_hash_iterator = rhs.m_hash_iterator;
    } else {
      DBUG_ASSERT(m_type == Type::TREE);
      m_tree_iterator = rhs.m_tree_iterator;
    }
  }

  return *this;
}

inline Cursor& Cursor::operator++() {
  DBUG_ASSERT(m_is_positioned);

  if (m_type == Type::HASH) {
    ++m_hash_iterator;
  } else {
    DBUG_ASSERT(m_type == Type::TREE);
    ++m_tree_iterator;
  }

  return *this;
}

inline Cursor& Cursor::operator--() {
  DBUG_ASSERT(m_is_positioned);

  if (m_type == Type::HASH) {
    /* We don't support decrement on a hash and it shouldn't be called. */
    abort();
  } else {
    DBUG_ASSERT(m_type == Type::TREE);
    --m_tree_iterator;
  }

  return *this;
}

inline bool Cursor::operator==(const Cursor& other) const {
  DBUG_ASSERT(m_is_positioned);

  if (m_type == Type::HASH) {
    return m_hash_iterator == other.m_hash_iterator;
  }

  DBUG_ASSERT(m_type == Type::TREE);
  return m_tree_iterator == other.m_tree_iterator;
}

inline bool Cursor::operator!=(const Cursor& other) const {
  return !(*this == other);
}

} /* namespace temptable */

#endif /* TEMPTABLE_CURSOR_H */
