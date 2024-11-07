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


/**
 * 固定长度数组分配
 */

#include "easy_array.h"

easy_array_t *easy_array_create(int object_size)
{
    easy_pool_t             *pool;
    easy_array_t            *array;

    if ((pool = easy_pool_create(0)) == NULL)
        return NULL;

    if ((array = (easy_array_t *)easy_pool_alloc(pool, sizeof(easy_array_t))) == NULL)
        return NULL;

    easy_list_init(&array->list);
    array->count = 0;
    array->pool = pool;
    array->object_size = easy_max(object_size, (int)sizeof(easy_list_t));

    return array;
}

void easy_array_destroy(easy_array_t *array)
{
    easy_pool_destroy(array->pool);
}

void *easy_array_alloc(easy_array_t *array)
{
    if (easy_list_empty(&array->list) == 0) {
        array->count --;
        char                    *ptr = (char *)array->list.prev;
        easy_list_del((easy_list_t *)ptr);
        return ptr;
    }

    return easy_pool_alloc(array->pool, array->object_size);
}

void easy_array_free(easy_array_t *array, void *ptr)
{
    array->count ++;
    easy_list_add_tail((easy_list_t *)ptr, &array->list);
}
