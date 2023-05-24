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

#ifndef PPI_PPI_GLOBAL_BASE_H_INCLUDED
#define PPI_PPI_GLOBAL_BASE_H_INCLUDED

#include "ppi/ppi_stat.h"

/**
  Performance Point Base Definition

  PPI for global
*/

/* Copy global io statistics */
typedef void (*get_io_statistic_func)(PPI_iostat_data *stat, bool first_read,
                                      int *index);

struct PPI_global_service_t {
  /* Copy global statistics */
  get_io_statistic_func get_io_statistic;
};

typedef struct PPI_global_service_t PPI_global_service_t;

/* Global PPI global service object */
extern PPI_global_service_t *PPI_global_service;

#endif
