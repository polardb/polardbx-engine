/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


#ifndef  EASY_SUMMARY_H
#define  EASY_SUMMARY_H

#include <easy_define.h>

#include "easy_io_struct.h"
#include "easy_log.h"

EASY_CPP_START
//////////////////////////////////////////////////////////////////////////////////
//接口函数
extern easy_summary_t          *easy_summary_create();
extern void                     easy_summary_destroy(easy_summary_t *sum);
extern easy_summary_node_t     *easy_summary_locate_node(int fd, easy_summary_t *sum, int hidden);
extern void                     easy_summary_destroy_node(int fd, easy_summary_t *sum);
extern void                     easy_summary_copy(easy_summary_t *src, easy_summary_t *dest);
extern easy_summary_t          *easy_summary_diff(easy_summary_t *ns, easy_summary_t *os);
extern void                     easy_summary_html_output(easy_pool_t *pool,
        easy_list_t *bc, easy_summary_t *sum, easy_summary_t *last);
extern void                     easy_summary_raw_output(easy_pool_t *pool,
        easy_list_t *bc, easy_summary_t *sum, const char *desc);

/////////////////////////////////////////////////////////////////////////////////

EASY_CPP_END

#endif
