/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef PPI_PPI_THREAD_BASE_H_INCLUDED
#define PPI_PPI_THREAD_BASE_H_INCLUDED

#include "ppi/ppi_stat.h"

struct PPI_transaction;

/**
  Performance Point Base Defination

  PPI for thread
*/

/**
  Only thread structure declaration

  It will be reinterpret_casted within Performance Point plugin.
*/
struct PPI_thread;

typedef struct PPI_thread PPI_thread;

/* Create PPI_thread object that will be saved into THD */
typedef PPI_thread *(*create_thread_func)();
/* Destroy PPI_thread object when disconnection */
typedef void (*destroy_thread_func)(PPI_thread *ppi_thread);
/* Copy thread statistics */
typedef void (*get_thread_statistic_func)(const PPI_thread *ppi_thread,
                                          PPI_stat *stat);
/* Get transaction context */
typedef PPI_transaction *(*get_transaction_context_func)(
    const PPI_thread *ppi_thread);

struct PPI_thread_service_t {
  /* Create PPI thread context */
  create_thread_func create_thread;
  /* Destroy PPI thread */
  destroy_thread_func destroy_thread;
  /* Copy thread statistics */
  get_thread_statistic_func get_thread_statistic;
  /* Get transaction context */
  get_transaction_context_func get_transaction_context;
};

typedef struct PPI_thread_service_t PPI_thread_service_t;

/* Global PPI thread service object */
extern PPI_thread_service_t *PPI_thread_service;

#endif
