/* Copyright (c) 2011, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License as
   published by the Free Software Foundation; version 2 of the
   License.

   This program is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   02110-1301 USA */

#include <stddef.h>

#include "m_ctype.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "mysql/service_mysql_alloc.h"
#include "mysqld_error.h"      // ER_*
#include "prealloced_array.h"
#include "psi_memory_key.h"
#include "rpl_gtid.h"

Owned_gtids::Owned_gtids(Checkable_rwlock *_sid_lock)
  : sid_lock(_sid_lock), sidno_to_hash(key_memory_Owned_gtids_sidno_to_hash)
{
}


Owned_gtids::~Owned_gtids()
{
  // destructor should only be called when no other thread may access object
  //sid_lock->assert_no_lock();
  // need to hold lock before calling get_max_sidno
  sid_lock->rdlock();
  rpl_sidno max_sidno= get_max_sidno();
  for (int sidno= 1; sidno <= max_sidno; sidno++)
  {
    delete get_hash(sidno);
  }
  sid_lock->unlock();
  //sid_lock->assert_no_lock();
}


enum_return_status Owned_gtids::ensure_sidno(rpl_sidno sidno)
{
  DBUG_ENTER("Owned_gtids::ensure_sidno");
  sid_lock->assert_some_wrlock();
  rpl_sidno max_sidno= get_max_sidno();
  if (sidno > max_sidno || get_hash(sidno) == NULL)
  {
    for (int i= max_sidno; i < sidno; i++)
    {
      sidno_to_hash.push_back
        (new malloc_unordered_multimap<rpl_gno, unique_ptr_my_free<Node>>
          (key_memory_Owned_gtids_sidno_to_hash));
    }
  }
  RETURN_OK;
}


enum_return_status Owned_gtids::add_gtid_owner(const Gtid &gtid,
                                               my_thread_id owner)
{
  DBUG_ENTER("Owned_gtids::add_gtid_owner(Gtid, my_thread_id)");
  DBUG_ASSERT(gtid.sidno <= get_max_sidno());
  Node *n= (Node *)my_malloc(key_memory_Sid_map_Node,
                             sizeof(Node), MYF(MY_WME));
  if (n == NULL)
    RETURN_REPORTED_ERROR;
  n->gno= gtid.gno;
  n->owner= owner;
  /*
  printf("Owned_gtids(%p)::add sidno=%d gno=%lld n=%p n->owner=%u\n",
         this, sidno, gno, n, n?n->owner:0);
  */
  get_hash(gtid.sidno)->emplace(gtid.gno, unique_ptr_my_free<Node>(n));
  RETURN_OK;
}


void Owned_gtids::remove_gtid(const Gtid &gtid, const my_thread_id owner)
{
  DBUG_ENTER("Owned_gtids::remove_gtid(Gtid)");
  //printf("Owned_gtids::remove(sidno=%d gno=%lld)\n", sidno, gno);
  //DBUG_ASSERT(contains_gtid(sidno, gno)); // allow group not owned
  malloc_unordered_multimap<rpl_gno, unique_ptr_my_free<Node>>
    *hash= get_hash(gtid.sidno);
  auto it_range= hash->equal_range(gtid.gno);
  for (auto it= it_range.first; it != it_range.second; ++it)
  {
    if (it->second->owner == owner)
    {
      hash->erase(it);
      DBUG_VOID_RETURN;
    }
  }
  DBUG_VOID_RETURN;
}


bool Owned_gtids::is_intersection_nonempty(const Gtid_set *other) const
{
  DBUG_ENTER("Owned_gtids::is_intersection_nonempty(Gtid_set *)");
  if (sid_lock != NULL)
    sid_lock->assert_some_wrlock();
  Gtid_iterator git(this);
  Gtid g= git.get();
  while (g.sidno != 0)
  {
    if (other->contains_gtid(g.sidno, g.gno))
      DBUG_RETURN(true);
    git.next();
    g= git.get();
  }
  DBUG_RETURN(false);
}

void Owned_gtids::get_gtids(Gtid_set &gtid_set) const
{
  DBUG_ENTER("Owned_gtids::get_gtids");

  if (sid_lock != NULL)
    sid_lock->assert_some_wrlock();

  Gtid_iterator git(this);
  Gtid g= git.get();
  while (g.sidno != 0)
  {
    gtid_set._add_gtid(g);
    git.next();
    g= git.get();
  }
  DBUG_VOID_RETURN;
}

bool Owned_gtids::contains_gtid(const Gtid &gtid) const
{
  malloc_unordered_multimap<rpl_gno, unique_ptr_my_free<Node>>
    *hash= get_hash(gtid.sidno);
  sid_lock->assert_some_lock();
  return hash->count(gtid.gno) != 0;
}

bool Owned_gtids::is_owned_by(const Gtid &gtid, const my_thread_id thd_id) const
{
  malloc_unordered_multimap<rpl_gno, unique_ptr_my_free<Node>>
    *hash= get_hash(gtid.sidno);
  auto it_range= hash->equal_range(gtid.gno);

  if (thd_id == 0)
    return it_range.first == it_range.second;

  for (auto it= it_range.first; it != it_range.second; ++it)
  {
    if (it->second->owner == thd_id)
      return true;
  }
  return false;
}
