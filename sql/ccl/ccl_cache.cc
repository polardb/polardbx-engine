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

#include "sql/ccl/ccl.h"

#include "my_macros.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"

/**
  Concurrency control cache.

  Init it when mysqld boot, and destroy when shutdown,
*/
namespace im {

template class Ring_buffer<Ccl_slot, CCL_SLOT_SIZE>;

/* Singleton static Ccl_ring_slots object init */
template <>
Ccl_ring_slots *Ccl_ring_slots::m_ring_buffer = nullptr;

template <>
Ccl_ring_slots *Ccl_ring_slots::instance() {
  return m_ring_buffer;
}

/* All concurrency control system memory usage */
PSI_memory_key key_memory_ccl;
/* CCL slot mutex psi key */
PSI_mutex_key key_LOCK_ccl_slot;
/* CCL slot condition psi key */
PSI_cond_key key_COND_ccl_slot;
/* CCL container rwlock psi key */
PSI_rwlock_key key_rwlock_rule_container;

/* Singleton static System_ccl object init */
System_ccl *System_ccl::m_system_ccl = nullptr;

static bool ccl_inited = false;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info ccl_memory[] = {
    {&key_memory_ccl, "im::ccl", 0, 0, PSI_DOCUMENT_ME}};

static PSI_mutex_info ccl_mutexes[] = {
    {&key_LOCK_ccl_slot, "LOCK_ccl_slot", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_info ccl_conds[] = {
    {&key_COND_ccl_slot, "COND_ssl_slot", 0, 0, PSI_DOCUMENT_ME}};

static PSI_rwlock_info ccl_rwlocks[] = {
    {&key_rwlock_rule_container, "RWLOCK_rules", 0, 0, PSI_DOCUMENT_ME}};

/**
  Init all the ccl psi keys
*/
static void init_ccl_psi_key() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(ccl_memory));
  mysql_memory_register(category, ccl_memory, count);

  count = static_cast<int>(array_elements(ccl_mutexes));
  mysql_mutex_register(category, ccl_mutexes, count);

  count = static_cast<int>(array_elements(ccl_conds));
  mysql_cond_register(category, ccl_conds, count);

  count = static_cast<int>(array_elements(ccl_rwlocks));
  mysql_rwlock_register(category, ccl_rwlocks, count);
}
#endif

/* Init singleton static system_ccl object */
static void system_ccl_init() {
  DBUG_ENTER("system_ccl_init");
  assert(System_ccl::m_system_ccl == nullptr);
  System_ccl::m_system_ccl = allocate_ccl_object<System_ccl>(key_memory_ccl);
  DBUG_VOID_RETURN;
}

/* Destroy singleton static system_ccl object */
static void system_ccl_destroy() {
  DBUG_ENTER("system_ccl_destroy");
  assert(System_ccl::m_system_ccl != nullptr);
  destroy_object<System_ccl>(System_ccl::m_system_ccl);
  System_ccl::m_system_ccl = nullptr;
  DBUG_VOID_RETURN;
}

/* Init the global ring buffer for ccl slot */
static void ccl_ring_slots_init() {
  Ccl_ring_slots *ccl_ring_slots;
  DBUG_ENTER("ccl_ring_slots_init");
  ccl_ring_slots = allocate_ccl_object<Ccl_ring_slots>(key_memory_ccl);
  for (size_t i = 0; i < CCL_SLOT_SIZE; i++) {
    Ccl_slot *slot = allocate_ccl_object<Ccl_slot>();
    ccl_ring_slots->get_slots().push_back(slot);
  }
  assert(ccl_ring_slots->get_slots().size() == CCL_SLOT_SIZE);
  Ccl_ring_slots::m_ring_buffer = ccl_ring_slots;
  DBUG_VOID_RETURN;
}
/* Destroy global ring buffer for ccl slot */
static void ccl_ring_slots_destroy() {
  DBUG_ENTER("ccl_ring_slots_destroy");
  assert(Ccl_ring_slots::m_ring_buffer != nullptr);
  destroy_object<Ccl_ring_slots>(Ccl_ring_slots::m_ring_buffer);
  Ccl_ring_slots::m_ring_buffer = nullptr;
  DBUG_VOID_RETURN;
}
/*
  Initialize system concurrency control

  It should be called when mysqld boot
*/
void ccl_init() {
#ifdef HAVE_PSI_INTERFACE
  init_ccl_psi_key();
#endif

  system_ccl_init();

  ccl_ring_slots_init();

  ccl_inited = true;
}

/**
  Make sure system_ccl is destroyed firstly,
  since some ccl slot maybe still be referenced by ccl rule.
*/
void ccl_destroy() {
  if (ccl_inited) {
    system_ccl_destroy();
    ccl_ring_slots_destroy();
  }
}
/**
  Flush ccl cache if force_clean is true and fill new rules.

  Only report  warning message if some rule failed.

  @param[in]      records         rule record set
  @param[in]      force_clean     whether clear the rule cache

  @retval         number          how many rule added.
*/
size_t refresh_ccl_cache(Conf_records *records, bool force_clean) {
  DBUG_ENTER("refresh_ccl_cache");
  assert(ccl_inited);
  size_t num = 0;

  /** In advance clear all rules since the rule record may be modified
      through update statement. */
  if (force_clean) System_ccl::instance()->clear_rules();

  for (uint i = static_cast<uint>(Ccl_rule_type::RULE_SELECT);
       i <= static_cast<uint>(Ccl_rule_type::RULE_DELETE); i++) {
    Ccl_record_group group;
    Ccl_rule_type type = static_cast<Ccl_rule_type>(i);
    group.classify_rule(type, records);

    num += System_ccl::instance()->refresh_rules(group, type, force_clean,
                                                 Ccl_error_level::CCL_WARNING);
  }
  DBUG_RETURN(num);
}

} /* namespace im */
