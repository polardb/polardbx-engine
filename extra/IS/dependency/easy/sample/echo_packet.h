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


#include <easy_io.h>

/**
 * 定义echo packet
 */
typedef struct echo_packet_t {
    int                     len;
    char                    *data;
    char                    buffer[0];
} echo_packet_t;

/**
 * decode
 */
void *echo_decode(easy_message_t *m)
{
    echo_packet_t           *packet;
    long                    request_size;

    if ((packet = (echo_packet_t *)easy_pool_calloc(m->pool, sizeof(echo_packet_t))) == NULL)
        return NULL;

    if (m->c->handler->user_data) {
        request_size = (long)m->c->handler->user_data;

        if (m->input->last - m->input->pos < request_size) {
            m->next_read_len = request_size - (m->input->last - m->input->pos);
            return NULL;
        }

        packet->data = (char *)m->input->pos;
        packet->len = request_size;
        m->input->pos += request_size;
    } else {
        packet->data = (char *)m->input->pos;
        packet->len = m->input->last - m->input->pos;
        m->input->pos = m->input->last;
    }

    return packet;
}

/**
 * encode
 */
int echo_encode(easy_request_t *r, void *data)
{
    echo_packet_t           *packet;
    easy_buf_t              *b;

    packet = (echo_packet_t *) data;

    if ((b = easy_buf_create(r->ms->pool, packet->len)) == NULL)
        return EASY_ERROR;

    memcpy(b->pos, packet->data, packet->len);
    b->last += packet->len;

    easy_request_addbuf(r, b);

    return EASY_OK;
}
