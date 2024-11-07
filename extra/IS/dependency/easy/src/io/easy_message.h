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


#ifndef EASY_MESSAGE_H_
#define EASY_MESSAGE_H_

#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * 接收message
 */

EASY_CPP_START

easy_message_t *easy_message_create(easy_connection_t *c);
easy_message_t *easy_message_create_nlist(easy_connection_t *c);
int easy_message_destroy(easy_message_t *m, int del);
int easy_session_process(easy_session_t *s, int stop);

EASY_CPP_END

#endif
