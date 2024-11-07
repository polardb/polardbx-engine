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


#ifndef EASY_SOCKET_H_
#define EASY_SOCKET_H_

#include <easy_define.h>
#include "easy_io_struct.h"
#include "easy_log.h"
#include <netinet/in.h>

/**
 * socket处理
 */

EASY_CPP_START

#define EASY_FLAGS_DEFERACCEPT 0x001
#define EASY_FLAGS_REUSEPORT   0x002
#define EASY_FLAGS_SREUSEPORT  0x004
#define EASY_FLAGS_NOLISTEN    0x008
#define SO_REUSEPORT 15
int easy_socket_listen(int udp, easy_addr_t *address, int *flags, int backlog);
int easy_socket_write(easy_connection_t *c, easy_list_t *l);
int easy_socket_read(easy_connection_t *c, char *buf, int size, int *pending);
int easy_socket_non_blocking(int fd);
int easy_socket_set_tcpopt(int fd, int option, int value);
int easy_socket_set_opt(int fd, int option, int value);
int easy_socket_support_ipv6();
int easy_socket_usend(easy_connection_t *c, easy_list_t *l);
int easy_socket_error(int fd);
int easy_socket_set_linger(int fd, int t);

int easy_socket_udpwrite(int fd, struct sockaddr *addr, easy_list_t *l);
int easy_socket_tcpwrite(int fd, easy_list_t *l);

EASY_CPP_END

#endif
