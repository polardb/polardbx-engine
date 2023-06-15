/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/lizard0handler.h

  Transaction coordinator recovery xa specification.

  Created 2023-06-14 by Jianwei.zhao
 *******************************************************/

#ifndef SQL_LIZARD0HANDLER_H
#define SQL_LIZARD0HANDLER_H

#include "sql/mem_root_allocator.h"
#include "sql/xa.h"

#include "sql/xa_specification.h"

class XA_spec_list {
 public:
  /** Internal XA spec structure. */
  using Commit_pair = std::pair<const my_xid, XA_specification *>;
  using Commit_map = std::map<my_xid, XA_specification *, std::less<my_xid>,
                              Mem_root_allocator<Commit_pair>>;

  /** External XA spec structure. */
  using State_pair = std::pair<const XID, XA_specification *>;
  using State_map = std::map<XID, XA_specification *, std::less<XID>,
                             Mem_root_allocator<State_pair>>;

 public:
  XA_spec_list(MEM_ROOT *mem_root)
      : m_mem_root(mem_root),
        m_commit_alloc(m_mem_root),
        m_commit_map(m_commit_alloc),
        m_state_alloc(m_mem_root),
        m_state_map(m_state_alloc) {}

  void add(const my_xid xid, XA_specification *spec) {
    m_commit_map[xid] = spec;
  }
  void add(const XID &xid, XA_specification *spec) { m_state_map[xid] = spec; }

  XA_specification *find(const my_xid xid) {
    auto found = m_commit_map.find(xid);
    if (found != m_commit_map.end()) {
      return found->second;
    }
    return nullptr;
  }

  XA_specification *find(const XID &xid) {
    auto found = m_state_map.find(xid);
    if (found != m_state_map.end()) {
      return found->second;
    }
    return nullptr;
  }

  Commit_map *commit_map() { return &m_commit_map; }
  State_map *state_map() { return &m_state_map; }

 private:
  MEM_ROOT *m_mem_root;

  Mem_root_allocator<Commit_pair> m_commit_alloc;
  Commit_map m_commit_map;

  Mem_root_allocator<State_pair> m_state_alloc;
  State_map m_state_map;
};
#endif
