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


#ifndef EASY_BUF_H_
#define EASY_BUF_H_

/**
 * 网络的读写的BUFFER
 */
#include "easy_define.h"
#include "easy_pool.h"

EASY_CPP_START

#define EASY_BUF_FILE        1
#define EASY_BUF_CLOSE_FILE  3

typedef struct easy_buf_t easy_buf_t;
typedef struct easy_file_buf_t easy_file_buf_t;
typedef struct easy_buf_string_t easy_buf_string_t;
typedef void (easy_buf_cleanup_pt)(easy_buf_t *, void *);

#define EASY_BUF_DEFINE                 \
    easy_list_t             node;       \
    int                     flags;      \
    easy_buf_cleanup_pt     *cleanup;   \
    void                    *args

struct easy_buf_t {
    EASY_BUF_DEFINE;
    char                    *pos;
    char                    *last;
    char                    *end;
};

struct easy_file_buf_t {
    EASY_BUF_DEFINE;
    int                     fd;
    int64_t                 offset;
    int64_t                 count;
};

struct easy_buf_string_t {
    char                    *data;
    int                     len;
};

extern easy_buf_t *easy_buf_create(easy_pool_t *pool, uint32_t size);
extern void easy_buf_set_cleanup(easy_buf_t *b, easy_buf_cleanup_pt *cleanup, void *args);
extern void easy_buf_set_data(easy_pool_t *pool, easy_buf_t *b, const void *data, uint32_t size);
extern easy_buf_t *easy_buf_pack(easy_pool_t *pool, const void *data, uint32_t size);
extern easy_file_buf_t *easy_file_buf_create(easy_pool_t *pool);
extern void easy_buf_destroy(easy_buf_t *b);
extern int easy_buf_check_read_space(easy_pool_t *pool, easy_buf_t *b, uint32_t size);
extern easy_buf_t *easy_buf_check_write_space(easy_pool_t *pool, easy_list_t *bc, uint32_t size);
extern void easy_file_buf_set_close(easy_file_buf_t *b);

extern void easy_buf_chain_clear(easy_list_t *l);
extern void easy_buf_chain_offer(easy_list_t *l, easy_buf_t *b);

///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_buf_string

#define easy_buf_string_set(str, text) {(str)->len=strlen(text); (str)->data=(char*)text;}

static inline char *easy_buf_string_ptr(easy_buf_string_t *s)
{
    return s->data;
}

void easy_buf_string_append(easy_buf_string_t *s, const char *value, int len);

static inline int easy_buf_len(easy_buf_t *b)
{
    if (unlikely(b->flags & EASY_BUF_FILE))
        return (int)(((easy_file_buf_t *)b)->count);
    else
        return (int)(b->last - b->pos);
}

extern int easy_buf_string_copy(easy_pool_t *pool, easy_buf_string_t *d, const easy_buf_string_t *s);
extern int easy_buf_string_printf(easy_pool_t *pool, easy_buf_string_t *d, const char *fmt, ...);
extern int easy_buf_list_len(easy_list_t *l);

#define EASY_FSTR           ".*s"
#define EASY_PSTR(a)        ((a)->len),((a)->data)

extern const easy_buf_string_t easy_string_null;

EASY_CPP_END

#endif
