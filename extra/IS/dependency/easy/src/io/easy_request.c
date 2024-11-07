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


#include "easy_io.h"
#include "easy_request.h"
#include "easy_message.h"
#include "easy_connection.h"
#include "easy_file.h"

static void easy_request_on_wakeup(struct ev_loop *loop, ev_async *w, int revents);
static void easy_request_doreq(easy_request_thread_t *th, easy_list_t *request_list);
static void easy_request_dosess(easy_request_thread_t *th, easy_list_t *session_list);
static void easy_request_cleanup (easy_buf_t *b, void *args);

/**
 * 对request回复响应
 *
 * @param packet对象
 */
int easy_request_do_reply(easy_request_t *r)
{
    easy_connection_t       *c;
    easy_message_t          *m;

    // encode
    m = (easy_message_t *)r->ms;
    c = m->c;

    if (c->ioth->tid != pthread_self()) {
        easy_fatal_log("not run at other thread: %lx <> %lx\n", r, pthread_self(), c->ioth->tid);
        return EASY_ERROR;
    }

    if (c->type == EASY_TYPE_CLIENT)
        return EASY_OK;

    easy_list_del(&r->request_list_node);

    if (easy_connection_request_done(r) == EASY_OK) {
        if (easy_list_empty(&c->output) == 0) {
            ev_io_start(c->loop, &c->write_watcher);
        }

        if (m->request_list_count == 0 && m->status != EASY_MESG_READ_AGAIN) {
            return easy_message_destroy(m, 1);
        }
    }

    return EASY_OK;
}

/**
 * push到c->output上
 */
void easy_request_addbuf(easy_request_t *r, easy_buf_t *b)
{
    easy_message_session_t  *ms = r->ms;

    // 在超时的时间用到
    if (ms->type == EASY_TYPE_SESSION) {
        ((easy_session_t *)ms)->nextb = &b->node;
    }

    easy_list_add_tail(&b->node, &ms->c->output);
}

/**
 * 加list到c->output上
 */
void easy_request_addbuf_list(easy_request_t *r, easy_list_t *list)
{
    easy_buf_t              *b;
    easy_message_session_t  *ms = r->ms;

    // 是否为空
    if (easy_list_empty(list))
        return;

    // 在超时的时间用到
    if (ms->type == EASY_TYPE_SESSION) {
        b = easy_list_get_last(list, easy_buf_t, node);

        if (b) ((easy_session_t *)ms)->nextb = &b->node;
    }

    easy_list_join(list, &ms->c->output);
    easy_list_init(list);
}

/**
 * 用于回调, 写完调用server_done
 */
static void easy_request_cleanup (easy_buf_t *b, void *args)
{
    easy_request_t          *r = (easy_request_t *) args;
    easy_connection_t       *c = r->ms->c;

    if (r->status == EASY_REQUEST_DONE) {
        easy_list_del(&r->all_node);
        easy_list_del(&r->request_list_node);
        easy_request_server_done(r);
    }

    if (c->handler->send_buf_done) {
        (c->handler->send_buf_done)(r);
    }

    easy_message_destroy((easy_message_t *)r->ms, 0);
}

/**
 * 设置request的cleanup方法
 */
void easy_request_set_cleanup(easy_request_t *r, easy_list_t *output)
{
    easy_buf_t              *b;
    easy_message_session_t  *ms = r->ms;
    b = easy_list_get_last(output, easy_buf_t, node);

    if (ms->type == EASY_TYPE_MESSAGE && b) {
        easy_atomic_inc(&ms->pool->ref);
        easy_buf_set_cleanup(b, easy_request_cleanup, r);
    }
}

/**
 * destroy掉easy_request_t对象
 */
void easy_request_server_done(easy_request_t *r)
{
    int                     doing;
    easy_connection_t       *c = r->ms->c;

    if (c->type == EASY_TYPE_SERVER) {
#ifdef EASY_DEBUG_DOING
        EASY_PRINT_BT("doing_request_count_dec:%d,c:%s,r:%p,%ld.", c->doing_request_count, easy_connection_str(c), r, r->uuid);
#endif
#ifdef EASY_DEBUG_MAGIC
        r->magic ++;
#endif

        if (r->alone == 0) {
            assert(c->doing_request_count > 0);
            c->doing_request_count --;
            doing = easy_atomic32_add_return(&c->ioth->doing_request_count, -1);
            assert(doing >= 0);
        }

        if (!r->status) c->con_summary->done_request_count ++;

        c->con_summary->rt_total += (ev_time() - r->start_time);

        if (c->handler->cleanup) (c->handler->cleanup)(r, NULL);
    }
}

void easy_request_client_done(easy_request_t *r)
{
    easy_connection_t       *c = r->ms->c;
#ifdef EASY_DEBUG_DOING
    EASY_PRINT_BT("doing_request_count_dec:%d,c:%s,r:%p,%ld.", c->doing_request_count, easy_connection_str(c), r, r->uuid);
#endif
#ifdef EASY_DEBUG_MAGIC
    r->magic ++;
#endif
    c->doing_request_count --;
    c->con_summary->doing_request_count --;
    c->con_summary->done_request_count ++;
    easy_atomic32_dec(&c->ioth->doing_request_count);
}

// request thread pool
easy_thread_pool_t *easy_thread_pool_create(easy_io_t *eio, int cnt, easy_request_process_pt *cb, void *args)
{
    return easy_thread_pool_create_ex(eio, cnt, easy_baseth_on_start, cb, args);
}

// 自己定义start
easy_thread_pool_t *easy_thread_pool_create_ex(easy_io_t *eio, int cnt,
        easy_baseth_on_start_pt *start, easy_request_process_pt *cb, void *args)
{
    easy_thread_pool_t      *tp;
    easy_request_thread_t   *rth;

    if ((tp = easy_baseth_pool_create(eio, cnt, sizeof(easy_request_thread_t))) == NULL)
        return NULL;

    // 初始化线程池
    easy_thread_pool_for_each(rth, tp, 0) {
        easy_baseth_init(rth, tp, start, easy_request_on_wakeup);

        rth->process = cb;
        rth->args = args;
        easy_list_init(&rth->task_list);
        easy_list_init(&rth->session_list);
    }

    // join
    tp->next = eio->thread_pool;
    eio->thread_pool = tp;

    return tp;
}

int easy_thread_pool_push(easy_thread_pool_t *tp, easy_request_t *r, uint64_t hv)
{
    easy_request_thread_t   *rth;

    // dispatch
    if (hv == 0) hv = easy_hash_key((long)r->ms->c);

    rth = (easy_request_thread_t *)easy_thread_pool_hash(tp, hv);
    easy_list_del(&r->request_list_node);

    easy_request_sleeping(r);

    easy_spin_lock(&rth->thread_lock);
    easy_list_add_tail(&r->request_list_node, &rth->task_list);
    rth->task_list_count ++;
    easy_spin_unlock(&rth->thread_lock);
    ev_async_send(rth->loop, &rth->thread_watcher);

    return EASY_OK;
}

int easy_thread_pool_push_message(easy_thread_pool_t *tp, easy_message_t *m, uint64_t hv)
{
    easy_request_thread_t   *rth;

    // dispatch
    if (hv == 0) hv = easy_hash_key((long)m->c);

    rth = (easy_request_thread_t *)easy_thread_pool_hash(tp, hv);

    // 引用次数
    easy_atomic_add(&m->c->pool->ref, m->request_list_count);
    easy_atomic_add(&m->pool->ref, m->request_list_count);
    easy_pool_set_lock(m->pool);

    easy_spin_lock(&rth->thread_lock);
    easy_list_join(&m->request_list, &rth->task_list);
    rth->task_list_count += m->request_list_count;
    easy_spin_unlock(&rth->thread_lock);
    ev_async_send(rth->loop, &rth->thread_watcher);

    easy_list_init(&m->request_list);

    return EASY_OK;
}

/**
 * push session
 */
int easy_thread_pool_push_session(easy_thread_pool_t *tp, easy_session_t *s, uint64_t hv)
{
    easy_request_thread_t   *rth;

    // choice
    if (hv == 0) hv = easy_hash_key((long)s);

    rth = (easy_request_thread_t *)easy_thread_pool_hash(tp, hv);

    easy_spin_lock(&rth->thread_lock);
    easy_list_add_tail(&s->session_list_node, &rth->session_list);
    easy_spin_unlock(&rth->thread_lock);
    ev_async_send(rth->loop, &rth->thread_watcher);

    return EASY_OK;
}

/**
 * WORK线程的回调程序
 */
static void easy_request_on_wakeup(struct ev_loop *loop, ev_async *w, int revents)
{
    easy_request_thread_t       *th;
    easy_list_t                 request_list;
    easy_list_t                 session_list;

    th = (easy_request_thread_t *) w->data;

    // 取回list
    easy_spin_lock(&th->thread_lock);
    th->task_list_count = 0;
    easy_list_movelist(&th->task_list, &request_list);
    easy_list_movelist(&th->session_list, &session_list);
    easy_spin_unlock(&th->thread_lock);

    easy_request_doreq(th, &request_list);
    easy_request_dosess(th, &session_list);
}

static void easy_request_doreq(easy_request_thread_t *th, easy_list_t *request_list)
{
    easy_request_t              *r, *r2;
    int                         retcode;

    // process
    easy_list_for_each_entry_safe(r, r2, request_list, request_list_node) {
        easy_list_del(&r->request_list_node);

        // 处理
        retcode = (th->process)(r, th->args);

        if (retcode == EASY_ABORT)
            continue;

        r->retcode = retcode;
        easy_request_wakeup(r);
    }
}

static void easy_request_dosess(easy_request_thread_t *th, easy_list_t *session_list)
{
    easy_session_t              *s, *s2;

    // process
    easy_list_for_each_entry_safe(s, s2, session_list, session_list_node) {
        easy_list_del(&s->session_list_node);

        /* update time */
        ev_now_update(th->loop);
        if ((th->process)(&s->r, th->args) != EASY_AGAIN) {
            easy_session_destroy(s);
        }
    }
}

/**
 * 重新wakeup
 */
void easy_request_wakeup(easy_request_t *r)
{
    if (r) {
        easy_io_thread_t        *ioth = r->ms->c->ioth;

        easy_spin_lock(&ioth->thread_lock);
        easy_list_add_tail(&r->request_list_node, &ioth->request_list);
        easy_spin_unlock(&ioth->thread_lock);
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }
}

/**
 * 引用计数增加,与easy_request_wakeup成对
 */
void easy_request_sleeping(easy_request_t *r)
{
    if (r) {
        // 引用次数
        easy_atomic_inc(&r->ms->c->pool->ref);
        easy_atomic_inc(&r->ms->pool->ref);
        easy_pool_set_lock(r->ms->pool);
    }
}

/**
 * 引用计数减掉
 */
void easy_request_sleepless(easy_request_t *r)
{
    if (r) {
        r->ms->c->pool->ref --;
        easy_atomic_dec(&r->ms->pool->ref);
    }
}

