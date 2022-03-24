#include "easy_io.h"
#include "easy_message.h"
#include "easy_connection.h"
#include "easy_request.h"
#include "easy_baseth_pool.h"

#ifdef EASY_DEBUG_DOING
easy_atomic_t easy_debug_uuid = 0;
#endif

easy_message_t *easy_message_create_nlist(easy_connection_t *c)
{
    easy_pool_t             *pool;
    easy_message_t          *m;
    easy_buf_t              *input;
    int                     size;

    if ((pool = easy_pool_create(c->default_msglen)) == NULL)
        return NULL;

    // 新建一个message
    pool->ref = 1;
    m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    size = c->first_msglen;
    input = easy_buf_create(pool, size);

    if (m == NULL || input == NULL) {
        easy_pool_destroy(pool);
        return NULL;
    }

#ifdef EASY_DEBUG_MAGIC
    m->magic = EASY_DEBUG_MAGIC_MESSAGE;
#endif

    m->pool = pool;
    m->c = c;
    m->next_read_len = (c->client && c->client->is_ssl) ? EASY_IO_BUFFER_SIZE : size;
    m->input = input;
    m->type = EASY_TYPE_MESSAGE;
    m->request_list_count = 0;
    easy_list_init(&m->request_list);
    easy_list_init(&m->all_list);

    return m;
}

easy_message_t *easy_message_create(easy_connection_t *c)
{
    easy_message_t *m = easy_message_create_nlist(c);

    if (m == NULL)
        return NULL;
    else
        easy_list_add_tail(&m->message_list_node, &c->message_list);

    return m;
}

/**
 * destroy掉easy_message_t对象
 *
 * @param m - easy_message_t对象
 */
int easy_message_destroy(easy_message_t *m, int del)
{
    easy_request_t          *r, *n;

    // delete from message_list
    if (del) {
        if (m->status == EASY_MESG_DESTROY)
            return EASY_OK;

        m->status = EASY_MESG_DESTROY;
        easy_list_del(&m->message_list_node);
    }

    if (easy_atomic_add_return(&m->pool->ref, -1) == 0) {
        // server done
        easy_list_for_each_entry_safe(r, n, &m->all_list, all_node) {
            easy_list_del(&r->all_node);
            easy_list_del(&r->request_list_node);
            easy_request_server_done(r);
        }

        easy_list_del(&m->message_list_node);

        if (m->input) easy_buf_destroy(m->input);

#ifdef EASY_DEBUG_MAGIC
        m->magic ++;
#endif
        easy_pool_destroy(m->pool);
        return EASY_BREAK;
    }

    return EASY_OK;
}

/**
 * 新建一个session_t
 */
easy_session_t *easy_session_create(int64_t asize)
{
    easy_pool_t             *pool;
    easy_session_t          *s;
    int                     hint = (int)(asize >> 32);
    int                     size = (int)asize;

    // 新建一个pool
    size += sizeof(easy_session_t);

    if ((pool = easy_pool_create((hint < size ? size : hint))) == NULL)
        return NULL;

    // 新建一个message
    if ((s = (easy_session_t *)easy_pool_alloc(pool, size)) == NULL) {
        easy_pool_destroy(pool);
        return NULL;
    }

    memset(s, 0, sizeof(easy_session_t));
#ifdef EASY_DEBUG_MAGIC
    s->magic = EASY_DEBUG_MAGIC_SESSION;
    s->r.magic = EASY_DEBUG_MAGIC_REQUEST;
#endif
#ifdef EASY_DEBUG_DOING
    s->r.uuid = easy_atomic_add_return(&easy_debug_uuid, 1);
#endif
    s->pool = pool;
    s->r.ms = (easy_message_session_t *)s;
    s->type = EASY_TYPE_SESSION;
    easy_list_init(&s->session_list_node);

    return s;
}

/**
 * destroy掉easy_session_t对象
 *
 * @param s - easy_session_t对象
 */
void easy_session_destroy(void *data)
{
    easy_message_t          *m;
    easy_session_t          *s;

    s = (easy_session_t *) data;

    if (s->cleanup)
        (s->cleanup)(&s->r, NULL);

    // 如果存在
    if (s->async && (m = (easy_message_t *)s->r.request_list_node.next)) {
        s->r.request_list_node.next = NULL;
        easy_message_destroy(m, 0);
    }

#ifdef EASY_DEBUG_MAGIC
    s->magic ++;
#endif
    easy_pool_destroy(s->pool);
}

int easy_session_process(easy_session_t *s, int stop)
{
    if (stop) {
        ev_timer_stop(s->c->loop, &s->timeout_watcher);
        easy_list_del(&s->session_list_node);
        easy_request_client_done(&s->r);
        easy_atomic_dec(&s->c->pool->ref);
    }

    // timeout, 把output里的clear掉
    int cnt = 0;
    int ret = EASY_ERROR;

    if (s->nextb && easy_list_empty(s->nextb) == 0) {
        easy_buf_t              *b, *b2;
        easy_list_for_each_entry_safe_reverse(b, b2, s->nextb, node) {
            if (b->args != s->pool) break;

            easy_list_del(&b->node);
        }
        cnt ++;
        easy_list_del(s->nextb);
    }

    if (s->process) {
        if (s->now) s->now = ev_now(s->c->loop) - s->now;

        ret = (s->process)(&s->r);
    } else {
        easy_error_log("session process is null, s = %p\n", s);
        easy_session_destroy(s);
    }

    return (cnt > 0 ? EASY_ERROR : ret);
}
