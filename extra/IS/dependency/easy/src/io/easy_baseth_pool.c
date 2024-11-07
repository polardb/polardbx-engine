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


#include <pthread.h>
#include "easy_io_struct.h"
#include "easy_log.h"
#include "easy_baseth_pool.h"
#include "easy_connection.h"
#include "easy_message.h"
#include <sys/socket.h>

__thread easy_baseth_t  *easy_baseth_self;
static void easy_baseth_pool_invoke(struct ev_loop *loop);

/**
 * start
 */
void *easy_baseth_on_start(void *args)
{
    easy_baseth_t           *th;
    easy_io_t               *eio;
    th = (easy_baseth_t *) args;
    easy_baseth_self = th;
    eio = th->eio;

    if (eio->block_thread_signal)
        pthread_sigmask(SIG_BLOCK, &eio->block_thread_sigset, NULL);

    ev_run(th->loop, 0);
    easy_baseth_self = NULL;

    easy_debug_log("pthread exit: %lx.\n", pthread_self());

    return (void *)NULL;
}

/**
 * wakeup
 */
void easy_baseth_on_wakeup(void *args)
{
    easy_baseth_t           *th = (easy_baseth_t *)args;

    easy_spin_lock(&th->thread_lock);
    ev_async_fsend(th->loop, &th->thread_watcher);
    easy_spin_unlock(&th->thread_lock);
}

void easy_baseth_init(void *args, easy_thread_pool_t *tp,
                      easy_baseth_on_start_pt *start, easy_baseth_on_wakeup_pt *wakeup)
{
    easy_baseth_t           *th = (easy_baseth_t *)args;
    th->idx = (((char *)(th)) - (&(tp)->data[0])) / (tp)->member_size;
    th->on_start = start;

    th->loop = ev_loop_new(0);
    th->thread_lock = EASY_SPIN_INITER;

    ev_async_init (&th->thread_watcher, wakeup);
    th->thread_watcher.data = th;
    ev_async_start (th->loop, &th->thread_watcher);

    ev_set_userdata(th->loop, th);
    ev_set_invoke_pending_cb(th->loop, easy_baseth_pool_invoke);
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * 创建一个thread pool
 */
easy_thread_pool_t *easy_baseth_pool_create(easy_io_t *eio, int thread_count, int member_size)
{
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp;
    int                     size;

    size = sizeof(easy_thread_pool_t) + member_size * thread_count;

    if ((tp = (easy_thread_pool_t *) easy_pool_calloc(eio->pool, size)) == NULL)
        return NULL;

    tp->thread_count = thread_count;
    tp->member_size = member_size;
    tp->last = &tp->data[0] + member_size * thread_count;
    easy_list_add_tail(&tp->list_node, &eio->thread_pool_list);
    easy_thread_pool_for_each(th, tp, 0) {
        th->eio = eio;
    }

    return tp;
}

/**
 * wakeup pool
 */
void easy_baseth_pool_on_wakeup(easy_thread_pool_t *tp)
{
    easy_baseth_t           *th;
    easy_thread_pool_for_each(th, tp, 0) {
        easy_baseth_on_wakeup(th);
    }
}

/**
 * destroy pool
 */
void easy_baseth_pool_destroy(easy_thread_pool_t *tp)
{
    easy_baseth_t           *th;
    easy_thread_pool_for_each(th, tp, 0) {
        ev_loop_destroy(th->loop);
    }
}

static void easy_baseth_pool_wakeup_session(easy_baseth_t *th)
{
    if (th->iot == 0)
        return;

    easy_connection_t       *c, *c1;
    easy_session_t          *s, *s1;
    easy_io_thread_t        *ioth = (easy_io_thread_t *) th;

    // session at ioth
    easy_spin_lock(&ioth->thread_lock);

    easy_list_for_each_entry_safe(s, s1, &ioth->session_list, session_list_node) {
        if (s->status == 0 || s->status == EASY_CONNECT_SEND) {
            easy_warn_log("session fail due to io thread exit %p", s);
            easy_list_del(&s->session_list_node);
            easy_session_process(s, 0);
        }
    }
    // connection at ioth
    easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node) {
        easy_connection_wakeup_session(c);
    }
    // foreach connected_list
    easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node) {
        easy_connection_wakeup_session(c);
    }
    easy_spin_unlock(&ioth->thread_lock);
}


/**
 * 判断是否退出
 */
static void easy_baseth_pool_invoke(struct ev_loop *loop)
{
    easy_baseth_t           *th = (easy_baseth_t *) ev_userdata (loop);
    easy_connection_t       *c, *c1;
    easy_io_thread_t        *ioth;
    easy_listen_t           *l;

    if (th->user_process) (*th->user_process)(th);

    ev_invoke_pending(loop);

    if (th->eio->shutdown && th->iot == 1) {
        ioth = (easy_io_thread_t *) ev_userdata (loop);

        if (ioth->eio->listen) {
            int ts = (ioth->eio->listen_all || ioth->eio->io_thread_count == 1);

            for (l = ioth->eio->listen; l; l = l->next) {
                if (l->reuseport || ts) {
                    ev_io_stop(loop, &l->read_watcher[ioth->idx]);
                } else {
                    ev_timer_stop (loop, &ioth->listen_watcher);
                }
            }
        }
        // connection at ioth
        easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node) {
            shutdown(c->fd, SHUT_RD);
            easy_connection_destroy(c);
        }
        // foreach connected_list
        easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node) {
            shutdown(c->fd, SHUT_RD);
            easy_connection_destroy(c);
        }
    }

    if (th->eio->stoped) {
        easy_baseth_pool_wakeup_session(th);
        ev_break(loop, EVBREAK_ALL);
        easy_debug_log("ev_break: eio=%p\n", th->eio);
    }
}
