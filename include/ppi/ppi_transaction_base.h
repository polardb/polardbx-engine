/* Copyright (c) 2008, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef PPI_PPI_TRANSACTION_BASE_H_INCLUDED
#define PPI_PPI_TRANSACTION_BASE_H_INCLUDED

#include "ppi/ppi_stat.h"

/**
  Performance Point transaction structure declaration

  It will be reinterpret_casted within Performance Point plugin.

  Pls include ppi_transaction.h file instead of ppi_transaction_base.h
*/

struct PPI_transaction;
struct PPI_thread;

typedef struct PPI_transaction PPI_transaction;

/* Start transaction */
typedef void (*start_transaction_func)(PPI_transaction *ppi_transaction);
/* Commit/rollback transaction */
typedef void (*end_transaction_func)(PPI_transaction *ppi_transaction);
/* Aggregate the transaction binlog bytes */
typedef void (*inc_transaction_binlog_size_func)(
    PPI_transaction *ppi_transaction, unsigned long long bytes);
/* Backup current transaction context */
typedef void (*backup_transaction_func)(PPI_thread *ppi_thread);
/* Restore parent transaction context */
typedef void (*restore_transaction_func)(PPI_thread *ppi_thread);
/* Get transaction statistics */
typedef void (*get_transaction_stat_func)(PPI_thread *ppi_thread,
                                          PPI_transaction_stat *stat);

/* Handler for the transaction service */
struct PPI_transaction_service_t {
  /* Start transaction */
  start_transaction_func start_transaction;
  /* Commit/rollback transaction */
  end_transaction_func end_transaction;
  /* Aggregate the transaction binlog bytes */
  inc_transaction_binlog_size_func inc_transaction_binlog_size;
  /* Backup current transaction context */
  backup_transaction_func backup_transaction;
  /* Restore parent transaction context */
  restore_transaction_func restore_transaction;
  /* Get transaction statistics */
  get_transaction_stat_func get_transaction_stat;
};

typedef struct PPI_transaction_service_t PPI_transaction_service_t;

extern PPI_transaction_service_t *PPI_transaction_service;

#endif
