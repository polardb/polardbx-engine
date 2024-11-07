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


#ifndef EASY_SSL_H_
#define EASY_SSL_H_

#ifndef LOW_LEVEL_SSL
#include <openssl/ssl_xdb.h>
#endif
#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * ssl支持模块
 */

EASY_CPP_START

#define EASY_SSL_SCACHE_BUILTIN 0
#define EASY_SSL_SCACHE_OFF     1
typedef struct easy_ssl_ctx_server_t {
    easy_hash_list_t        node;
    char                    *server_name;
    easy_ssl_ctx_t          *ss;
} easy_ssl_ctx_server_t;

typedef struct easy_ssl_pass_phrase_dialog_t {
    char                    *type;
    char                    *server_name;
} easy_ssl_pass_phrase_dialog_t;

#define easy_ssl_get_connection(s) SSL_get_ex_data((SSL*)s, easy_ssl_connection_index)
extern int              easy_ssl_connection_index;

// function
int easy_ssl_connection_create(easy_ssl_ctx_t *ssl, easy_connection_t *c);
int easy_ssl_connection_destroy(easy_connection_t *c);
void easy_ssl_connection_handshake(struct ev_loop *loop, ev_io *w, int revents);
int easy_ssl_client_do_handshake(easy_connection_t *c);
void easy_ssl_client_handshake(struct ev_loop *loop, ev_io *w, int revents);

EASY_CPP_END

#endif
