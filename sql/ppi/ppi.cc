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

/**
  @file

  Performance Point register entrance.
*/

#include "ppi/ppi_global.h"
#include "ppi/ppi_statement.h"
#include "ppi/ppi_thread.h"
#include "ppi/ppi_transaction.h"

/**
  Performance Point interface register function.

  It will be called when PPS plugin init.
*/
void PPI_register_thread_service(PPI_thread_service_t *service) {
  PPI_thread_service = service;
}

void PPI_register_statement_service(PPI_statement_service_t *service) {
  PPI_statement_service = service;
}

void PPI_register_global_service(PPI_global_service_t *service) {
  PPI_global_service = service;
}

void PPI_register_transaction_service(PPI_transaction_service_t *service) {
  PPI_transaction_service = service;
}
