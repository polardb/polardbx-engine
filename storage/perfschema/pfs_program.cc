/* Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software Foundation,
  51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

/**
  @file storage/perfschema/pfs_program.cc
  Statement Digest data structures (implementation).
*/

/*
  This code needs extra visibility in the lexer structures
*/

#include "my_global.h"
#include "my_sys.h"
#include "pfs_instr.h"
#include "pfs_program.h"
#include "pfs_global.h"
#include "sql_string.h"
#include <string.h>

/** EVENTS_STATEMENTS_SUMMARY_BY_PROGRAM circular buffer. */
PFS_program_stat *program_stat_array= NULL;
/** Consumer flag for table EVENTS_STATEMENTS_SUMMARY_BY_PROGRAM. */
bool flag_programs= true;
/** Current index in Stat array where new record is to be inserted. */
volatile uint32 program_index= 0;

ulong program_max= 0;
ulong program_lost= 0;

LF_HASH program_hash;                                                           
static bool program_hash_inited= false;

/**
  Initialize table EVENTS_STATEMENTS_SUMMARY_BY_ROUTINE.
  @param param performance schema sizing
*/
int init_program(const PFS_global_param *param)
{
  unsigned int index;

  /*
    Allocate memory for program_stat_array based on
    performance_schema_max_program_instances value.
  */
  program_max= param->m_program_sizing;
  program_lost= 0;

  if (program_max == 0)
    return 0;

  program_stat_array=
    PFS_MALLOC_ARRAY(program_max, PFS_program_stat,
                     MYF(MY_ZEROFILL));
  if (unlikely(program_stat_array == NULL))
    return 1;

  for (index= 0; index < program_max; index++)
  {
    program_stat_array[index].reset_data();
  }

  return 0;
}

/** Cleanup table EVENTS_STATEMENTS_SUMMARY_BY_PROGRAM. */
void cleanup_program(void)
{
  /*  Free memory allocated to program_stat_array. */
  pfs_free(program_stat_array);
  program_stat_array= NULL;
}

C_MODE_START
static uchar *program_hash_get_key(const uchar *entry, size_t *length,
                                   my_bool)
{
  const PFS_program_stat * const *typed_entry;
  const PFS_program_stat *program;
  const void *result;
  typed_entry= reinterpret_cast<const PFS_program_stat* const *> (entry);
  DBUG_ASSERT(typed_entry != NULL);
  program= *typed_entry;
  DBUG_ASSERT(program != NULL);
  *length= program->m_key.m_key_length;
  result= program->m_key.m_hash_key;
  return const_cast<uchar*> (reinterpret_cast<const uchar*> (result));
}
C_MODE_END

/**
  Initialize the program hash.
  @return 0 on success
*/
int init_program_hash(void)
{
  if ((! program_hash_inited) && (program_max > 0))
  {
    lf_hash_init(&program_hash, sizeof(PFS_program_stat*), LF_HASH_UNIQUE,
                 0, 0, program_hash_get_key, &my_charset_bin);
    program_hash.size= program_max;
    program_hash_inited= true;
  }
  return 0;
}

/** Cleanup the program hash. */
void cleanup_program_hash(void)
{
  if (program_hash_inited)
  {
    lf_hash_destroy(&program_hash);
    program_hash_inited= false;
  }
}

static void set_program_key(PFS_program_key *key,
                            enum_object_type object_type,
                            const char *object_name, uint object_name_length,
                            const char *schema_name, uint schema_name_length)
{
  DBUG_ASSERT(object_name_length <= OBJECT_NAME_LENGTH);
  DBUG_ASSERT(schema_name_length <= SCHEMA_NAME_LENGTH);

  char *ptr= &key->m_hash_key[0];
  switch(object_type)
  {
    case OBJECT_TYPE_PROCEDURE:
      memcpy(ptr, "PROCEDURE", 9);
      ptr+= 9;
    break;
    case OBJECT_TYPE_FUNCTION:
      memcpy(ptr, "FUNCTION", 8);
      ptr+= 8;
    break;
    case OBJECT_TYPE_TRIGGER:
      memcpy(ptr, "TRIGGER", 7);
      ptr+= 7;
    break;
/*
    case OBJECT_TYPE_EVENT:
      memcpy(ptr, "EVENT", 5);
      ptr+= 5;
    break;
*/
    default:
      //DBUG_ASSERT(0);
    break;
  }
  ptr[0]= 0;
  ptr++;

  if (object_name_length > 0)
  {
    memcpy(ptr, object_name, object_name_length);
    ptr+= object_name_length;
  }
  ptr[0]= 0;
  ptr++;

  if (schema_name_length > 0)
  {
    memcpy(ptr, schema_name, schema_name_length);
    ptr+= schema_name_length;
  }
  ptr[0]= 0;
  ptr++;

  key->m_key_length= ptr - &key->m_hash_key[0];
}



void PFS_program_stat::reset_data()
{
  m_object_name_length= 0;
  m_schema_name_length= 0;
  m_sp_stat.reset();
  m_stmt_stat.reset();
}

void reset_esms_by_program()
{
  uint index;

  if (program_stat_array == NULL)
    return;

  /* Reset program_stat_array. */
  for (index= 0; index < program_max; index++)
  {
    program_stat_array[index].reset_data();
  }

  /*
    Reset index which indicates where the next program information
    to be inserted in event_stat_array.
  */
  program_index= 0;
}

static LF_PINS* get_program_hash_pins(PFS_thread *thread)
{
  if (unlikely(thread->m_program_hash_pins == NULL))
  {
    if (! program_hash_inited)
      return NULL;
    thread->m_program_hash_pins= lf_hash_get_pins(&program_hash);
  }
  return thread->m_program_hash_pins;
}

PFS_program_stat*
find_or_create_program(PFS_thread *thread,
                      enum_object_type object_type,
                      const char *object_name,
                      uint object_name_length,
                      const char *schema_name,
                      uint schema_name_length)
{
  if (program_stat_array == NULL || program_max == 0 ||
      object_name_length ==0 || schema_name_length == 0)
    return NULL;

  LF_PINS *pins= get_program_hash_pins(thread);
  if (unlikely(pins == NULL))
    return NULL;
 
  /* Prepare program key */
  PFS_program_key key;
  key.m_hash_key[0]= 0;
  set_program_key(&key, object_type,
                  object_name, object_name_length,
                  schema_name, schema_name_length);
  
  PFS_program_stat **entry;
  PFS_program_stat *pfs= NULL;
  uint retry_count= 0;
  const uint retry_max= 3; 
  ulong safe_index= 0;

search:
  entry= reinterpret_cast<PFS_program_stat**>
    (lf_hash_search(&program_hash, pins,
                    key.m_hash_key, key.m_key_length));

  if (entry && (entry != MY_ERRPTR))
  {
    /* If record already exists then return its pointer. */
    pfs= *entry;
    lf_hash_search_unpin(pins);
    return pfs;
  }
  
  lf_hash_search_unpin(pins);

  safe_index= PFS_atomic::add_u32(& program_index, 1);
  if (safe_index >= program_max)
  {
    program_lost++;
    return NULL;
  }

  /* Else create a new record in program stat array. */
  pfs= &program_stat_array[safe_index];

  /* Do the assignments. */
  memcpy(pfs->m_key.m_hash_key, key.m_hash_key, key.m_key_length);
  pfs->m_key.m_key_length= key.m_key_length;
  pfs->m_type= object_type;
  strncpy(pfs->m_object_name, object_name, object_name_length);
  pfs->m_object_name_length= object_name_length;
  strncpy(pfs->m_schema_name, schema_name, schema_name_length);
  pfs->m_schema_name_length= schema_name_length;

  /* Insert this record. */
  int res= lf_hash_insert(&program_hash, pins, &pfs);
 
  if (likely(res == 0))
  {
    return pfs;
  }

  if (res > 0)
  {
    /* Duplicate insert by another thread */
    if (++retry_count > retry_max)
    {
      /* Avoid infinite loops */
      program_lost++;
      return NULL;
    }
    goto search;
  }

  /* OOM in lf_hash_insert */
  program_lost++;
  return NULL;
}

int find_and_drop_program(PFS_thread *thread, 
                          enum_object_type object_type,
                          const char *object_name,
                          uint object_name_length,
                          const char *schema_name,
                          uint schema_name_length)
{
  int res;
  LF_PINS *pins= get_program_hash_pins(thread);
  if (unlikely(pins == NULL))
    return -1;
 
  /* Prepare program key */
  PFS_program_key key;
  key.m_hash_key[0]= 0;
  set_program_key(&key, object_type,
                  object_name, object_name_length,
                  schema_name, schema_name_length);

  PFS_program_stat **entry;
  entry= reinterpret_cast<PFS_program_stat**>
    (lf_hash_search(&program_hash, pins,
                    key.m_hash_key, key.m_key_length));

  if (entry && (entry != MY_ERRPTR))
  {
    PFS_program_stat *pfs= NULL;
    pfs= *entry;
    pfs->reset_data();

    res= lf_hash_delete(&program_hash, pins,
                        key.m_hash_key, key.m_key_length);
    if(res==0)
    { 
      /* 
         TODO: Need to put code to make use of this index in program_stat_array
               to be available for further new programs' statistics.
      */
    }
  }
  
  lf_hash_search_unpin(pins);
  return res;
}
