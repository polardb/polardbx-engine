/***********************************************************************

Copyright (c) 2011, 2014, Oracle and/or its affiliates. All rights reserved.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

***********************************************************************/

/**************************************************//**
@file memcached_mysql.h
InnoDB Memcached plugin

Created 04/12/2011 Jimmy Yang
*******************************************************/

#ifndef MEMCACHED_MYSQL_H
#define MEMCACHED_MYSQL_H

/** The main memcached header holding commonly used data
structures and function prototypes. */
struct memcached_context
{
	char*		m_engine_library;
	char*		m_mem_option;
	void*		m_innodb_api_cb;
	unsigned int	m_r_batch_size;
	unsigned int	m_w_batch_size;
	bool		m_enable_binlog;
};

typedef struct memcached_context        memcached_context_t;

# ifdef __cplusplus
 extern "C" {
# endif

void* daemon_memcached_main(void *p);

void shutdown_server(void);

bool initialize_complete(void);
bool shutdown_complete(void);

bool init_complete(void);

# ifdef __cplusplus
}
# endif

#endif    /* MEMCACHED_MYSQL_H */

