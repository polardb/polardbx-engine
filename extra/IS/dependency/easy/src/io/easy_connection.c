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


#include <easy_string.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include "easy_io.h"
#include "easy_connection.h"
#include "easy_message.h"
#include "easy_request.h"
#include "easy_file.h"
#include "easy_client.h"
#include "easy_socket.h"
#include "easy_log.h"

static void easy_switch_listen(void *data);
static easy_connection_t *easy_connection_new();
static void easy_connection_on_timeout_mesg(struct ev_loop *loop, ev_timer *w, int revents);
static void easy_connection_on_timeout_conn(struct ev_loop *loop, ev_timer *w, int revents);
static void easy_connection_on_pause(struct ev_loop *loop, ev_timer *w, int revents);
static int easy_connection_redispatch_thread(easy_connection_t *c);
static void easy_connection_evio_start(easy_connection_t *c);

static int easy_connection_do_request(easy_message_t *m);
static int easy_connection_do_response(easy_message_t *m);
static int easy_connection_send_response(easy_list_t *request_list);

static easy_message_t *easy_connection_recycle_message(easy_message_t *m);
static easy_connection_t *easy_connection_do_connect(easy_client_t *client, int afd);
static easy_connection_t *easy_connection_do_client(easy_session_t *s);
static void easy_connection_autoconn(easy_connection_t *c);
static int easy_connection_process_request(easy_connection_t *c, easy_list_t *list);
static int easy_connection_sendsocket(easy_connection_t *c);
static int easy_connection_listen_dispatch(easy_io_t *eio, easy_addr_t addr, easy_listen_t *listen);
static void easy_connection_listen_watcher(easy_session_t *s);
static int easy_connection_accept_one(struct ev_loop *loop, ev_io *w);
static int easy_connection_checkself(easy_connection_t *c);
static void easy_connection_dump_slow_request(easy_connection_t *c);

/**
 * 增加监听端口, 要在easy_io_start开始调用
 *
 * @param host  机器名或IP, 或NULL
 * @param port  端口号
 *
 * @return      如果成功返回easy_listen_t对象, 否则返回NULL
 */
easy_listen_t *easy_connection_add_listen(easy_io_t *eio,
        const char *host, int port, easy_io_handler_pt *handler)
{
    return easy_add_listen(eio, host, port, handler, NULL);
}

easy_listen_t *easy_connection_listen_addr(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler)
{
    int                     udp = ((handler && handler->is_udp) ? 1 : 0);
    return easy_add_listen_addr(eio, addr, handler, udp, NULL);
}

easy_listen_t *easy_add_listen(easy_io_t *eio, const char *host, int port,
                               easy_io_handler_pt *handler, void *args)
{
    easy_addr_t             address;
    int                     udp;

    udp = ((handler && handler->is_udp) ? 1 : 0);

    if (host == NULL) {
        if (eio->support_ipv6) host = "[]";
    } else if (memcmp(host, "udp:", 4) == 0 || memcmp(host, "tcp:", 4) == 0) {
        udp = (*host == 'u');
        host += 4;
    }

    if ((address = easy_inet_str_to_addr(host, port)).family == 0) {
        easy_trace_log("error addr: host=%s, port=%d.\n", host, port);
        return NULL;
    }

    return easy_add_listen_addr(eio, address, handler, udp, args);
}

/**
 * 通过easy_addr_t创建easy_listen_t
 */
easy_listen_t *easy_add_listen_addr(easy_io_t *eio, easy_addr_t addr,
                                    easy_io_handler_pt *handler, int udp, void *args)
{
    int                     i, size, cnt, fd;
    int                     flags = (eio->tcp_defer_accept ? EASY_FLAGS_DEFERACCEPT : 0);
    char                    buffer[32];
    easy_listen_t           *l;

    if (eio->pool == NULL) {
        easy_error_log("easy_connection_add_listen failure: eio->started=%d, eio->pool=%p\n",
                       eio->started, eio->pool);
        return NULL;
    }

    // alloc memory
    cnt = eio->io_thread_count;
    size = cnt * sizeof(ev_io);
    size += sizeof(easy_listen_t);

    if ((l = (easy_listen_t *) easy_pool_calloc(eio->pool, size)) == NULL) {
        easy_error_log("easy_pool_calloc failure: eio->pool=%p, size=%d\n",
                       eio->pool, size);
        return NULL;
    }

    // 打开监听
    l->listen_lock = EASY_SPIN_INITER;
    l->addr = addr;
    l->handler = handler;

    if (eio->no_reuseport == 0) flags |= EASY_FLAGS_NOLISTEN;

    if ((fd = easy_socket_listen(udp, &l->addr, &flags, eio->listen_backlog)) < 0) {
        easy_error_log("easy_socket_listen failure: host=%s\n", easy_inet_addr_to_str(&l->addr, buffer, 32));
        return NULL;
    }

    // 初始化
    for(i = 0; i < cnt; i++) {
        if (udp) {
            ev_io_init(&l->read_watcher[i], easy_connection_on_udpread, fd, EV_READ | EV_CLEANUP);
        } else {
            ev_io_init(&l->read_watcher[i], easy_connection_on_accept, fd, EV_READ | EV_CLEANUP);
        }

        ev_set_priority (&l->read_watcher[i], EV_MAXPRI);
        l->read_watcher[i].data = l;
    }

    if (eio->no_reuseport == 0) {
        l->reuseport = (flags & EASY_FLAGS_REUSEPORT) ? 1 : 0;
    }

    l->fd = fd;
    l->accept_count = eio->accept_count;

    if (!l->reuseport) {
        easy_info_log("easy_socket_listen: host=%s, fd=%d", easy_inet_addr_to_str(&addr, buffer, 32), fd);
    }

    if (eio->started) {
        if (l->reuseport) {
            for (i = 0; i < eio->io_thread_count; i++) {
                addr.cidx = i;
                easy_connection_listen_dispatch(eio, addr, l);
            }

            i = 50;

            while(l->bind_port_cnt < eio->io_thread_count && i-- > 0) usleep(1000);
        } else {
            addr.cidx = 0;
            easy_connection_listen_dispatch(eio, addr, l);
        }
    } else {
        l->next = eio->listen;
        eio->listen = l;
    }

    return l;
}

/**
 * session_timeout
 */
void easy_connection_wakeup_session(easy_connection_t *c)
{
    easy_session_t          *s, *sn;
    easy_request_t          *r, *rn;

    if (c->type == EASY_TYPE_CLIENT && c->send_queue) {
        easy_list_for_each_entry_safe(s, sn, &(c->send_queue->list), send_queue_list) {
            easy_hash_del_node(&s->send_queue_hash);
            easy_session_process(s, 1);
        }
        c->send_queue->count = 0;
        c->send_queue->seqno = 1;
        easy_list_init(&c->send_queue->list);
    }

    if (c->type != EASY_TYPE_SERVER || c->conn_has_error == 0 || easy_list_empty(&c->session_list))
        return;

    easy_list_for_each_entry_safe(r, rn, &c->session_list, request_list_node) {
        if (r->waiting == 0) continue;

        easy_list_del(&r->request_list_node);
        (c->handler->process)(r);
    }
}

/**
 * destroy掉easy_connection_t对象
 *
 * @param c - easy_connection_t对象
 */
void easy_connection_destroy(easy_connection_t *c)
{
    easy_message_t          *m, *m2;
    easy_io_t               *eio;

    // release session
    easy_connection_wakeup_session(c);

    // disconnect
    eio = c->ioth->eio;

    if (c->status != EASY_CONN_CLOSE && c->handler->on_disconnect) {
        (c->handler->on_disconnect)(c);
    }

    // refcount
    if (likely(eio->stoped == 0)) {
        if (c->status != EASY_CONN_CLOSE && c->pool->ref > 0) {
            ev_io_stop(c->loop, &c->read_watcher);
            ev_io_stop(c->loop, &c->write_watcher);

            if (c->pool->ref > 0) {
                ev_timer_set(&c->timeout_watcher, 0.0, 0.5);
                ev_timer_again(c->loop, &c->timeout_watcher);
            }
        }

        if (c->status != EASY_CONN_CLOSE) {
            c->last_time = ev_now(c->loop);
            c->status = EASY_CONN_CLOSE;
        }

        if (c->pool->ref > 0) return;
    }

    // release message
    if (easy_list_empty(&c->output) == 0) {
        easy_warn_log("%s has data", easy_connection_str(c));
        easy_buf_chain_clear(&c->output);
    }

    easy_list_for_each_entry_safe(m, m2, &c->message_list, message_list_node) {
        if (eio->stoped) m->pool->ref = 1;

        easy_message_destroy(m, 1);
    }
    easy_list_init(&c->message_list);
    ev_io_stop(c->loop, &c->read_watcher);
    ev_io_stop(c->loop, &c->write_watcher);
    ev_timer_stop(c->loop, &c->timeout_watcher);
    ev_timer_stop(c->loop, &c->pause_watcher);

    //clean summary
    easy_summary_destroy_node(c->fd, eio->eio_summary);

    // close
    if (c->fd >= 0) {
        easy_debug_log("%s close\n", easy_connection_str(c));

        if (!c->read_eof) {
            char                    buf[EASY_POOL_PAGE_SIZE];

            while (read(c->fd, buf, EASY_POOL_PAGE_SIZE) > 0);
        }

        close(c->fd);
        c->fd = -1;
    }

    // autoreconn
    if (c->auto_reconn && eio->stoped == 0) {
        c->status = EASY_CONN_AUTO_CONN;
        double                  t = c->reconn_time / 1000.0 * (1 << c->reconn_fail);

        if (t > 30) t = 30;

        if (c->reconn_fail < 16) c->reconn_fail ++;

        t = ((c->client && c->client->timeout) ? c->client->timeout : EASY_CLIENT_DEFAULT_TIMEOUT) / 1000.0;
        easy_warn_log("%s reconn_time: %f, reconn_fail: %d\n", easy_connection_str(c), t, c->reconn_fail);
        ev_timer_set(&c->timeout_watcher, 0.0, t);
        ev_timer_again(c->loop, &c->timeout_watcher);
        return;
    }

    easy_list_del(&c->conn_list_node);
    easy_list_del(&c->group_list_node);
    easy_atomic32_add_return(&c->ioth->doing_request_count, -c->doing_request_count);

    if (c->client) c->client->c = NULL;

    if (eio->stoped) c->pool->ref = 0;

    if (c->handler && c->handler->on_close) {
        (c->handler->on_close)(c);
    }

#ifdef EASY_DEBUG_MAGIC
    c->magic ++;
#endif

    easy_pool_destroy(c->pool);
}

/**
 * connect参数设置
 */
easy_session_t *easy_connection_connect_init(easy_session_t *s,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags,
        char *servername)
{
    easy_pool_t             *pool = NULL;

    if (!s) {
        s = easy_session_create(0);
        pool = s->pool;
    }

    memset(s, 0, sizeof(easy_session_t));
    s->pool = pool;
    s->status = EASY_CONNECT_ADDR;
    s->thread_ptr = (void *)handler;
    s->timeout = conn_timeout;
    s->r.args = args;
    s->packet_id = flags;

    if (servername) {
        s->packet_id |= EASY_CONNECT_SSL;
        s->r.user_data = servername;
    }

    return s;
}

/**
 * 异步连接
 */
int easy_connection_connect(easy_io_t *eio, easy_addr_t addr,
                            easy_io_handler_pt *handler, int conn_timeout, void *args, int flags)
{
    easy_session_t          *s = easy_connection_connect_init(NULL, handler, conn_timeout, args, flags, NULL);
    return easy_connection_connect_ex(eio, addr, s);
}

int easy_connection_connect_ex(easy_io_t *eio, easy_addr_t addr, easy_session_t *s)
{
    int                     ret;

    if (addr.family == 0 || s == NULL)
        return EASY_ERROR;

    if ((ret = easy_client_dispatch(eio, addr, s)) != EASY_OK)
        easy_session_destroy(s);

    return ret;
}

/**
 * 同步连接
 */
easy_connection_t *easy_connection_connect_thread(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags)
{
    if (addr.family == 0)
        return NULL;

    easy_session_t          s, *ps;
    ps = easy_connection_connect_init(&s, handler, conn_timeout, args, flags, NULL);
    return easy_connection_connect_thread_ex(addr, ps);
}

easy_connection_t *easy_connection_connect_thread_ex(easy_addr_t addr, easy_session_t *s)
{
    if (addr.family == 0 || s == NULL)
        return NULL;

    s->addr = addr;
    return easy_connection_do_client(s);
}

/**
 * 断开连接
 */
int easy_connection_disconnect(easy_io_t *eio, easy_addr_t addr)
{
    int                     ret;

    if (addr.family == 0)
        return EASY_ERROR;

    easy_session_t          *s = easy_session_create(0);
    s->status = EASY_DISCONNECT_ADDR;

    if ((ret = easy_client_dispatch(eio, addr, s)) != EASY_OK) {
        easy_session_destroy(s);
    }

    return ret;
}

int easy_connection_disconnect_thread(easy_io_t *eio, easy_addr_t addr)
{
    if (addr.family == 0)
        return EASY_ERROR;

    easy_session_t          s;
    memset(&s, 0, sizeof(easy_session_t));
    s.status = EASY_DISCONNECT_ADDR;
    s.addr = addr;
    easy_connection_do_client(&s);
    return EASY_OK;
}

int easy_connection_session_build(easy_session_t *s)
{
    double                  t;
    easy_connection_t       *c = s->c;

    if (c->type != EASY_TYPE_CLIENT)
        return EASY_ERROR;

    if (!s->cleanup) s->cleanup = c->handler->cleanup;

    // 得到packet_id
    s->packet_id = easy_connection_get_packet_id(c, s->r.opacket, 0);
    // encode
    (c->handler->encode)(&s->r, s->r.opacket);

    s->timeout_watcher.data = s;
    easy_hash_dlist_add(c->send_queue, s->packet_id, &s->send_queue_hash, &s->send_queue_list);
    easy_atomic_inc(&c->pool->ref);

#ifdef EASY_DEBUG_DOING
    EASY_PRINT_BT("doing_request_count_inc:%d,c:%s,r:%p,%ld.", c->doing_request_count, easy_connection_str(c), &s->r, s->r.uuid);
#endif

    c->doing_request_count ++;
    c->con_summary->doing_request_count ++;

    // 加入c->session_list
    s->now = ev_now(c->loop);
    t = (s->timeout ? s->timeout : EASY_CLIENT_DEFAULT_TIMEOUT) / 1000.0;
    ev_timer_init(&s->timeout_watcher, easy_connection_on_timeout_mesg, t, 0.0);
    ev_timer_start(c->loop, &s->timeout_watcher);

    return EASY_OK;
}

/**
 * 发送到c上, 只允许本io线程调用
 */
int easy_connection_send_session(easy_connection_t *c, easy_session_t *s)
{
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;

    if (ioth == NULL || ioth->iot == 0 || ioth->eio->stoped) {
        return EASY_ERROR;
    }

    if (unlikely(ioth->eio->checkdrc == 0 && ioth->doing_request_count >= EASY_IOTH_DOING_REQ_CNT && s->status)) {
        easy_error_log("%p, ioth->doing_request_count: %d, EASY_IOTH_DOING_REQ_CNT: %d\n",
                       ioth, ioth->doing_request_count, EASY_IOTH_DOING_REQ_CNT);
        return EASY_ERROR;
    }

    s->c = c;

    if (s->process == NULL) s->process = c->handler->process;

    easy_atomic32_inc(&ioth->doing_request_count);

    if (easy_connection_session_build(s) != EASY_OK)
        return EASY_ERROR;

    //easy_warn_log("sendsocket in send_session: %p, packet_id: %" PRIx64 " %s",
    //              s, s->packet_id, easy_connection_str(c));
    return easy_connection_sendsocket(c);
}

/**
 * 把数据发送到c上, 只允许本io线程调用
 */
int easy_connection_send_session_data(easy_connection_t *c, easy_session_t *s)
{
    s->c = c;

    // encode
    (c->handler->encode)(&s->r, s->r.opacket);

    return easy_connection_sendsocket(c);
}
///////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * accept 事件处理
 */
void easy_connection_on_accept(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_listen_simple_t    *listen;
    int                     cnt;

    listen = (easy_listen_simple_t *) w->data;
    if (listen->accept_count > 0) {
        cnt = listen->accept_count;
    } else {
        cnt = (listen->reuseport ? 32 : 5);
    }

    do {
        if (easy_connection_accept_one(loop, w) < 0) break;

        cnt --;
    } while(cnt > 0);
}

static int easy_connection_accept_one(struct ev_loop *loop, ev_io *w)
{
    static easy_atomic_t    easy_accept_sequence = 0;
    int                     fd;
    easy_listen_simple_t    *listen;
    struct sockaddr_storage addr;
    socklen_t               addr_len;
    easy_connection_t       *c;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;

    listen = (easy_listen_simple_t *) w->data;
    addr_len = sizeof(addr);

    // accept
#if HAVE_ACCEPT4

    if (ioth->eio->use_accept4) {
        if ((fd = accept4(w->fd, (struct sockaddr *)&addr, &addr_len, SOCK_NONBLOCK)) < 0) {
            if (errno == ENOSYS) ioth->eio->use_accept4 = 0;

            if ((fd = accept(w->fd, (struct sockaddr *)&addr, &addr_len)) < 0)
                return EASY_ERROR;

            easy_socket_non_blocking(fd);
        }
    } else
#endif
    {
        if ((fd = accept(w->fd, (struct sockaddr *)&addr, &addr_len)) < 0)
            return EASY_ERROR;

        easy_socket_non_blocking(fd);
    }

    // 为新连接创建一个easy_connection_t对象
    if ((c = easy_connection_new()) == NULL) {
        easy_error_log("easy_connection_new\n");
        close(fd);
        return EASY_ERROR;
    }

    if (ioth->eio->tcp_nodelay) {
        easy_socket_set_tcpopt(fd, TCP_NODELAY, 1);
    }

    // 初始化
    c->fd = fd;
    c->type = EASY_TYPE_SERVER;
    c->handler = listen->handler;
    c->evdata = w->data;
    easy_inet_atoe(&addr, &c->addr);
    c->seq = easy_atomic_add_return(&easy_accept_sequence, 1);

    // 事件初始化
    ev_io_init(&c->read_watcher, easy_connection_on_readable, fd, EV_READ);
    ev_io_init(&c->write_watcher, easy_connection_on_writable, fd, EV_WRITE);
    ev_init(&c->timeout_watcher, easy_connection_on_timeout_conn);
    c->read_watcher.data = c;
    c->write_watcher.data = c;
    c->timeout_watcher.data = c;
    c->ioth = ioth;
    c->loop = loop;
    c->start_time = ev_now(ioth->loop);

    easy_debug_log("accept from %s, cb: %p\n", easy_connection_str(c), ev_cb(&c->read_watcher));

    //server locate
    c->con_summary = easy_summary_locate_node(c->fd, c->ioth->eio->eio_summary, listen->hidden_sum);

    // on connect
    if (c->handler->on_connect && (c->handler->on_connect)(c) == EASY_ERROR) {
        easy_pool_destroy(c->pool);
        close(fd);
        return EASY_ERROR;
    }

    // start idle
    if (c->handler->on_idle) {
        double                  t = easy_max(1.0, c->idle_time / 2000.0);
        ev_timer_set(&c->timeout_watcher, 0.0, t);
        ev_timer_again(c->loop, &c->timeout_watcher);
    }

    // 让出来给其他的线程
    if (listen->is_simple == 0 && listen->reuseport == 0 && ioth->eio->listen_all == 0)
        easy_switch_listen(listen);

    // start read
    easy_list_add_tail(&c->conn_list_node, &c->ioth->connected_list);
    c->event_status = EASY_EVENT_READ;

    if (ioth->eio->tcp_defer_accept) {
        (ev_cb(&c->read_watcher))(loop, &c->read_watcher, 0);
    } else {
        easy_connection_evio_start(c);
    }

    return EASY_OK;
}

static void easy_switch_listen(void *data)
{
    easy_listen_t           *listen = (easy_listen_t *)data;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;

    if (ioth->eio->listen_all == 0 && listen->old_ioth == NULL && listen->curr_ioth == ioth) {
        listen->old = listen->cur;
        listen->curr_ioth = NULL;
        listen->old_ioth = ioth;
        ioth->listen_watcher.repeat = 0.5;
        ev_timer_again (ioth->loop, &ioth->listen_watcher);
        easy_unlock(&listen->listen_lock);
    }
}

static void easy_connection_evio_start(easy_connection_t *c)
{
    easy_debug_log("%s evio: %d, cb:%p", easy_connection_str(c), c->event_status, (ev_cb(&c->read_watcher)));

    if ((c->event_status & EASY_EVENT_READ))
        ev_io_start(c->loop, &c->read_watcher);

    if ((c->event_status & EASY_EVENT_WRITE))
        ev_io_start(c->loop, &c->write_watcher);

    if ((c->event_status & EASY_EVENT_TIMEOUT))
        ev_timer_start(c->loop, &c->timeout_watcher);

    c->event_status = 0;
}

/**
 * 为了均衡，切换到其他线程
 */
static int easy_connection_redispatch_thread(easy_connection_t *c)
{
    easy_io_thread_t        *ioth;

    // 处理了８次以下, 或者有读写没完, 不能切换
    if (!c->need_redispatch || easy_list_empty(&c->message_list) == 0 || easy_list_empty(&c->output) == 0)
        return EASY_AGAIN;

    // 选择一新的ioth
    ioth = (easy_io_thread_t *)easy_thread_pool_hash(EASY_IOTH_SELF->eio->io_thread_pool, c->seq);
    return easy_connection_dispatch_to_thread(c, ioth);
}

/**
 * 把c直接给指定ioth
 */
int easy_connection_dispatch_to_thread(easy_connection_t *c, easy_io_thread_t *ioth)
{
    int                     status = EASY_EVENT_READ;
    int                     doing = c->doing_request_count;

    c->need_redispatch = 0;

    if (ioth == c->ioth)
        return EASY_AGAIN;

    easy_list_del(&c->conn_list_node);

    if (ev_is_active(&c->write_watcher)) status |= EASY_EVENT_WRITE;

    if (ev_is_active(&c->timeout_watcher)) status |= EASY_EVENT_TIMEOUT;

    if (status & EASY_EVENT_WRITE) {
        ev_io_stop(c->loop, &c->read_watcher);
    } else {
        ev_io_stop_ctrl(c->loop, &c->read_watcher);
    }

    ev_io_stop_ctrl(c->loop, &c->write_watcher);
    ev_timer_stop(c->loop, &c->timeout_watcher);

    // request_list
    if (c->send_queue && c->type == EASY_TYPE_SERVER) {
        easy_list_t             *request = (easy_list_t *) c->send_queue;
        easy_list_join(request, &c->session_list);
        c->send_queue = NULL;
    }

    easy_info_log("%s redispatch %p to %p, cnt:%d\n", easy_connection_str(c), c->ioth, ioth, doing);

    // 加入到新的队列中
    if (doing > 0) {
        easy_atomic32_add_return(&c->ioth->doing_request_count, -doing);
        easy_atomic32_add_return(&ioth->doing_request_count, doing);
    }

    c->event_status |= status;
    c->ioth = ioth;
    c->loop = ioth->loop;

    // wakeup
    easy_spin_lock(&ioth->thread_lock);
    easy_list_add_tail(&c->conn_list_node, &ioth->conn_list);
    easy_spin_unlock(&ioth->thread_lock);
    ev_async_send(ioth->loop, &ioth->thread_watcher);

    return EASY_ASYNC;
}

/**
 * 切换listen
 */
void easy_connection_on_listen(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_listen_t               *l;
    easy_io_thread_t            *ioth;
    ioth = (easy_io_thread_t *) w->data;
    easy_io_t                   *eio = ioth->eio;

    if (eio->listenadd) {
        easy_spin_lock(&eio->lock);
        eio->listenadd->next = eio->listen;
        eio->listen = eio->listenadd;
        eio->listenadd = NULL;
        easy_spin_unlock(&eio->lock);
    }

    // 对每一个listen
    for (l = eio->listen; l; l = l->next) {
        if (l->reuseport) continue;

        // trylock一下
        if (easy_trylock(&l->listen_lock)) {
            // 是自己
            if (l->old_ioth == ioth) {
                l->old_ioth = NULL;
                l->curr_ioth = ioth;
            } else {
                l->cur = ((l->cur + 1) & 1);
                ev_io_start(ioth->loop, &l->read_watcher[l->cur]);
                l->curr_ioth = ioth;
                ioth->listen_watcher.repeat = 60.;
                ev_timer_again (ioth->loop, &ioth->listen_watcher);
            }
        } else if (l->curr_ioth && l->old_ioth == ioth) {
            ev_io_stop(ioth->loop, &l->read_watcher[l->old]);
            l->old_ioth = NULL;
        }
    }
}

/**
 * conn事件处理
 */
void easy_connection_on_wakeup(struct ev_loop *loop, ev_async *w, int revents)
{
    easy_connection_t       *c, *c2;
    easy_io_thread_t        *ioth;
    easy_list_t             conn_list;
    easy_list_t             session_list;
    easy_list_t             request_list;

    ioth = (easy_io_thread_t *) w->data;

    // 取回list
    easy_spin_lock(&ioth->thread_lock);
    easy_list_movelist(&ioth->conn_list, &conn_list);
    easy_list_movelist(&ioth->session_list, &session_list);
    easy_list_movelist(&ioth->request_list, &request_list);
    easy_spin_unlock(&ioth->thread_lock);

    // foreach
    easy_list_for_each_entry_safe(c, c2, &conn_list, conn_list_node) {
        c->loop = loop;
        c->start_time = ev_now(ioth->loop);
        easy_connection_evio_start(c);

        if (c->handler->on_redispatch) {
            c->handler->on_redispatch(c);
        }
    }
    easy_list_join(&conn_list, &ioth->connected_list);

    easy_connection_send_session_list(&session_list);
    easy_connection_send_response(&request_list);
}

/**
 * read事件处理
 */
void easy_connection_on_readable(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;
    easy_message_t          *m;
    int                     n;
    int                     pending;

    c = (easy_connection_t *)w->data;
    assert(c->fd == w->fd);

    // 防止请求过多
    if (unlikely(c->type == EASY_TYPE_SERVER &&
                 (c->doing_request_count > EASY_CONN_DOING_REQ_CNT || c->ioth->doing_request_count > EASY_IOTH_DOING_REQ_CNT))) {
        if (c->ioth->eio->checkdrc == 0 || c->ioth->doing_request_count % EASY_WARN_LOG_INTERVAL == 0) {
            easy_warn_log("c->doing_request_count: %d, c->ioth->doing_request_count: %d\n",
                          c->doing_request_count, c->ioth->doing_request_count);

            if (c->ioth->eio->checkdrc == 0) goto error_exit;
        }
    }

    // 最后的请求, 如果数据没完, 需要继续读
    m = easy_list_get_last(&c->message_list, easy_message_t, message_list_node);

    // 第一次读或者上次读完整了, 重新建一个easy_message_t
    if (m == NULL || m->status != EASY_MESG_READ_AGAIN) {
        if ((m = easy_message_create(c)) == NULL) {
            easy_error_log("easy_message_create failure, c=%p\n", c);
            goto error_exit;
        }
    }

    do {
        // 检查buffer大小
        pending = 0;

        if (easy_buf_check_read_space(m->pool, m->input, m->next_read_len) != EASY_OK) {
            easy_error_log("easy_buf_check_read_space failure, m=%p, len=%d\n", m, m->next_read_len);
            goto error_exit;
        }

        // 从conn里读入数据
        if ((n = (c->read)(c, m->input->last, m->next_read_len, &pending)) <= 0) {
            if (n == EASY_AGAIN) {
                easy_connection_evio_start(c);
                return;
            }

            // client
            if (n == 0 && c->type == EASY_TYPE_CLIENT) {
                easy_debug_log("%s: do read eof\n", easy_connection_str(c));

                if(EASY_ERROR == easy_connection_do_response(m)) {
                    easy_debug_log("%s type=client error\n", easy_connection_str(c));
                }
            }

            if (n == 0) {
                easy_debug_log("%s n: %d\n", easy_connection_str(c), n);
            } else {
                easy_warn_log("%s n: %d, error: %s(%d)\n", easy_connection_str(c), n, strerror(errno), errno);
            }

            c->conn_has_error = (n < 0 ? 1 : 0);
            goto error_exit;
        }

        if (unlikely(easy_log_level >= EASY_LOG_DEBUG)) {
            if (easy_log_level == EASY_LOG_DEBUG) {
                easy_debug_log("%s read: %d", easy_connection_str(c), n);
            } else {
                char                    btmp[128];
                easy_trace_log("%s read: %d => %s", easy_connection_str(c), n, easy_string_tohex(m->input->last, n, btmp, 128));
            }
        }

        m->input->last += n;
        c->read_eof = (n < m->next_read_len);
        c->con_summary->in_byte += n;
    } while(pending);

    c->last_time = ev_now(loop);
    c->reconn_fail = 0;

    if (c->read_eof == 0 && c->first_msglen == EASY_FIRST_MSGLEN) {
        c->first_msglen = EASY_IO_BUFFER_SIZE;
        m->next_read_len = c->first_msglen;
    }

    // client
    if (EASY_ERROR == ((c->type == EASY_TYPE_CLIENT) ?
                       easy_connection_do_response(m) : easy_connection_do_request(m))) {
        easy_debug_log("%s type=%s error\n", easy_connection_str(c), (c->type == EASY_TYPE_CLIENT ? "client" : "server"));
        goto error_exit;
    }

    return;
error_exit:
    easy_connection_destroy(c);
}

/**
 * 处理响应
 */
static int easy_connection_do_response(easy_message_t *m)
{
    easy_connection_t       *c;
    easy_session_t          *s;
    uint64_t                packet_id;
    int                     i, cnt, left;
    void                    *packet;
    easy_list_t             list;
    double                  now;

    c = m->c;

    // 处理buf
    cnt = 0;
    left = 0;
    easy_list_init(&list);
    now = ev_now(easy_baseth_self->loop);

    while (m->input->pos < m->input->last) {
        if ((packet = (c->handler->decode)(m)) == NULL) {
            if (m->status != EASY_ERROR) {
                // quickack
                if (EASY_IOTH_SELF->eio->no_delayack && m->next_read_len < EASY_MSS) {
                    easy_socket_set_tcpopt(c->fd, TCP_QUICKACK, 1);
                }

                break;
            }

            easy_warn_log("decode error, %s\n", easy_connection_str(c));
            return EASY_ERROR;
        }

        cnt ++;
        packet_id = easy_connection_get_packet_id(c, packet, 1);
        s = (easy_session_t *) easy_hash_dlist_del(c->send_queue, packet_id);

        if (s == NULL) {
            // 需要cleanup
            if (c->handler->cleanup)
                (c->handler->cleanup)(NULL, packet);

            easy_warn_log("not found session, packet_id=%ld %s\n", packet_id, easy_connection_str(c));
            continue;
        }

        // process
        EASY_IOTH_SELF->done_request_count ++;
        s->r.ipacket = packet;              // in

        if (s->async) {                     // message延后释放
            m->async = s->async;
            easy_atomic_inc(&m->pool->ref);
            s->r.request_list_node.next = (easy_list_t *) m;
        }

        // stop timer
        ev_timer_stop(c->loop, &s->timeout_watcher);
        easy_list_del(&s->session_list_node);
        easy_request_client_done(&s->r);
        easy_atomic_dec(&c->pool->ref);

        if (c->handler->batch_process) {
            if (s->now) s->now = now - s->now;

            easy_list_add_tail(&s->session_list_node, &list);

            if (++ left >= 32) {
                (c->handler->batch_process)((easy_message_t *)&list);
                left = 0;
            }
        } else if (easy_session_process(s, 0) == EASY_ERROR) {
            easy_warn_log("easy_session_process error, fd=%d, s=%p\n", c->fd, s);
            return EASY_ERROR;
        }
    }

    // batch process
    if (cnt) m->recycle_cnt ++;

    if (left > 0) {
        (c->handler->batch_process)((easy_message_t *)&list);
    }

    // close
    if (c->wait_close && c->pool->ref == 0) {
        c->wait_close = 0;
        return EASY_ERROR;
    }

    // send new packet
    if (c->handler->new_packet) {
        left = (EASY_CONN_DOING_REQ_CNT / 2) - c->doing_request_count;

        if (c->ioth->doing_request_count > 0) {
            left = easy_min(left, (EASY_IOTH_DOING_REQ_CNT / 2) - c->ioth->doing_request_count);
        }

        if (left > 0) {
            ev_io_start(c->loop, &c->write_watcher);
            left = easy_min(cnt, left);

            for(i = 0; i < left; i++) {
                if ((c->handler->new_packet)(c) == EASY_ERROR)
                    return EASY_ERROR;
            }
        }
    }

    if ((m = easy_connection_recycle_message(m)) == NULL) {
        easy_warn_log("easy_connection_recycle_message error, fd=%d, m=%p\n", c->fd, m);
        return EASY_ERROR;
    }

    // status, message 没读完
    if (m->input->pos < m->input->last) {
        m->status = EASY_MESG_READ_AGAIN;
    } else {
        easy_message_destroy(m, 1);
    }

    return EASY_OK;
}

/**
 * 处理请求
 */
static int easy_connection_do_request(easy_message_t *m)
{
    easy_connection_t       *c;
    void                    *packet;
    easy_request_t          *r;
    int                     cnt, ret;

    cnt = 0;
    c = m->c;

    // 处理buf, decode
    while (m->input->pos < m->input->last) {
        if ((packet = (c->handler->decode)(m)) == NULL) {
            if (m->status != EASY_ERROR) {
                // quickack
                if ( EASY_IOTH_SELF->eio->no_delayack && m->next_read_len < EASY_MSS) {
                    easy_socket_set_tcpopt(c->fd, TCP_QUICKACK, 1);
                }

                break;
            }

            easy_warn_log("decode error, %s m=%p, cnt=%d\n", easy_connection_str(c), m, cnt);
            c->doing_request_count += cnt;
            easy_atomic32_add(&c->ioth->doing_request_count, cnt);
            return EASY_ERROR;
        }

        // new request
        r = (easy_request_t *)easy_pool_calloc(m->pool, sizeof(easy_request_t));

        if (r == NULL) {
            easy_error_log("easy_pool_calloc failure, %s, m: %p\n", easy_connection_str(c), m);
            c->doing_request_count += cnt;
            easy_atomic32_add(&c->ioth->doing_request_count, cnt);
            return EASY_ERROR;
        }

#ifdef EASY_DEBUG_MAGIC
        r->magic = EASY_DEBUG_MAGIC_REQUEST;
#endif
        r->ms = (easy_message_session_t *)m;
        r->ipacket = packet;    //进来的数据包
        r->start_time = ev_now(c->loop);

        // add m->request_list
        easy_list_add_tail(&r->request_list_node, &m->request_list);
        easy_list_add_tail(&r->all_node, &m->all_list);
        cnt ++;

#ifdef EASY_DEBUG_DOING
        r->uuid = easy_atomic_add_return(&easy_debug_uuid, 1);
        EASY_PRINT_BT("doing_request_count_inc:%d,c:%s,r:%p,%ld.", c->doing_request_count, easy_connection_str(c), r, r->uuid);
#endif
    }

    // cnt
    if (cnt) {
        m->request_list_count += cnt;
        c->doing_request_count += cnt;
        c->con_summary->doing_request_count += cnt;
        easy_atomic32_add(&c->ioth->doing_request_count, cnt);
        m->recycle_cnt ++;
    }

    if ((m = easy_connection_recycle_message(m)) == NULL)
        return EASY_ERROR;

    m->status = ((m->input->pos < m->input->last) ? EASY_MESG_READ_AGAIN : 0);

    // batch process
    if (c->handler->batch_process)
        (c->handler->batch_process)(m);

    // process
    if ((ret = easy_connection_process_request(c, &m->request_list)) != EASY_OK)
        return ret;

    // 加入监听
    if (c->event_status == EASY_EVENT_READ && !c->wait_close)
        easy_connection_evio_start(c);

    easy_connection_redispatch_thread(c);

    return EASY_OK;
}

/**
 * write事件处理
 */
void easy_connection_on_writable(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;
    int                     ret;

    c = (easy_connection_t *)w->data;
    assert(c->fd == w->fd);

    // wait client time
    if (c->wcs > 0.0) {
        c->wait_client_time += (ev_now(c->loop) - c->wcs);
        c->wcs = 0.0;
    }

    if ((ret = easy_connection_write_socket(c)) == EASY_ABORT)
        goto error_exit;

    // 没数据可发, 把write停掉
    if (easy_list_empty(&c->output)) {
        if (easy_connection_redispatch_thread(c) == EASY_ASYNC)
            return;

        ev_io_stop(c->loop, &c->write_watcher);
    }

    // client
    if (c->type == EASY_TYPE_CLIENT) {
        // connected.
        if (c->status == EASY_CONN_CONNECTING) {
            c->status = EASY_CONN_OK;
            ev_io_start(c->loop, &c->read_watcher);
            ev_timer_set(&c->timeout_watcher, 0.0, 0.5);
            ev_timer_again(c->loop, &c->timeout_watcher);

            if (easy_socket_error(c->fd) != 0) {
                c->conn_has_error = 1;
                goto error_exit;
            }

            // on connect
            if (c->handler->on_connect && (c->handler->on_connect)(c) == EASY_ERROR)
                goto error_exit;
        }

        // send new packet
        if (c->handler->new_packet && ret == EASY_OK && c->doing_request_count < (EASY_CONN_DOING_REQ_CNT / 2)) {
            if ((c->handler->new_packet)(c) == EASY_ERROR)
                goto error_exit;
        }

    } else if (easy_list_empty(&c->output) && easy_list_empty(&c->session_list) == 0) {

        // 调用process
        ret = easy_connection_process_request(c, &c->session_list);

        if (ret == EASY_ERROR)
            goto error_exit;
        else if (ret == EASY_ASYNC)
            return;

        // 还有数据可写
        if (easy_list_empty(&c->output) == 0)
            ev_io_start(c->loop, &c->write_watcher);
    }

    return;
error_exit:
    easy_connection_destroy(c);
}

/**
 * 对timeout的处理message
 */
static void easy_connection_on_timeout_mesg(struct ev_loop *loop, ev_timer *w, int revents)
{
    static int              now = 0;
    easy_connection_t       *c;
    easy_session_t          *s;

    s = (easy_session_t *)w->data;
    c = s->c;

    if (now != (int)ev_now(loop) && s->error == 0) {
        easy_warn_log("timeout_mesg: %p, time: %lf (s), packet_id: %" PRIx64 " %s",
                      s, ev_now(loop) - s->now, s->packet_id, easy_connection_str(c));
        now = (int)ev_now(loop);
    }

    // process
    easy_hash_dlist_del(c->send_queue, s->packet_id);
    s->packet_id = 0;

    if (easy_session_process(s, 1) == EASY_ERROR)
        easy_connection_destroy(c);
}

/**
 * 对timeout的处理connection
 */
static void easy_connection_on_timeout_conn(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_connection_t       *c;
    double                  t, now;
    c = (easy_connection_t *)w->data;

    easy_debug_log("%s timeout_conn: status=%d, type=%d\n", easy_connection_str(c), c->status, c->type);

    if (c->status == EASY_CONN_AUTO_CONN) {
        easy_connection_autoconn(c);
    } else if (c->status != EASY_CONN_OK) { // EASY_CONN_CLOSE, EASY_CONN_CONNECTING
        c->conn_has_error = 1;

        if (c->status == EASY_CONN_CLOSE && c->ioth->eio->force_destroy_second > 0) {
            now = ev_now(loop) - c->ioth->eio->force_destroy_second;

            if (c->last_time < now) {
                if (0 == c->ioth->eio->no_force_destroy) {
                    c->pool->ref = 0;
                } else if (1 == c->ioth->eio->no_force_destroy && 0 == c->slow_request_dumped) {
                    easy_connection_dump_slow_request(c);
                }
            }
        }

        goto error_exit;
    } else { // EASY_CONN_OK
        now = ev_now(loop);

        if (c->life_idle)
            t = now - easy_max(c->start_time, c->last_time);
        else
            t = now - c->last_time;

        if (c->handler->on_idle && (c->idle_time / 1000.0 < t)) {
            // on_idle
            int ret = (c->handler->on_idle)(c);

            if (ret == EASY_ABORT)
                return;

            if (ret == EASY_ERROR)
                goto error_exit;
        }

        if (c->type == EASY_TYPE_CLIENT) {
            if (c->handler->on_idle) {
                t = easy_max(1.0, c->idle_time / 2000.0);
                ev_timer_set(&c->timeout_watcher, 0.0, t);
                ev_timer_again(c->loop, &c->timeout_watcher);
            } else {
                ev_timer_stop(c->loop, &c->timeout_watcher);
            }
        }
    }

    return;
error_exit:
    easy_connection_destroy(c);
}

/**
 * 把connection上的output的buffer写到socket上
 *
 * @param c - easy_connection_t对象
 * @return  - EASY_ABORT 网络断开
 *            EASY_AGAIN 没写完,需要继续写
 *            EASY_OK    写完了
 */
int easy_connection_write_socket(easy_connection_t *c)
{
    int                     ret;

    // 空的直接返回
    if (easy_list_empty(&c->output))
        return EASY_OK;

    // 加塞
    if (EASY_IOTH_SELF->eio->tcp_cork && c->tcp_cork_flag == 0) {
        easy_socket_set_tcpopt(c->fd, TCP_CORK, 1);
        c->tcp_cork_flag = 1;
    }

    ret = (c->write)(c, &c->output);
    easy_debug_log("%s write: %d", easy_connection_str(c), ret);

    if (ret > 0) {
        c->con_summary->out_byte += ret;
    }

    if (ret == EASY_ERROR) {
        easy_warn_log("ret=%d, %s, error: %s (%d)\n", ret, easy_connection_str(c), strerror(errno), errno);
        c->conn_has_error = 1;
        ev_io_stop(c->loop, &c->write_watcher);
        return EASY_ABORT;
    }

    c->last_time = ev_now(c->loop);

    return easy_connection_write_again(c);
}

// 判断write again
int easy_connection_write_again(easy_connection_t *c)
{
    // 还有没写出去, 起写事件
    if (easy_list_empty(&c->output) == 0) {
        if (c->handler->sending_data)
            c->handler->sending_data(c);

        c->wcs = ev_now(c->loop);
        ev_io_start(c->loop, &c->write_watcher);
        return EASY_AGAIN;
    } else if (c->handler->send_data_done) {
        return c->handler->send_data_done(c);
    } else {
        if (c->type == EASY_TYPE_SERVER) {
            if (c->wait_close && easy_list_empty(&c->session_list)) {// 需要关闭掉
                c->wait_close = 0;
                shutdown(c->fd, SHUT_WR);
                return EASY_ABORT;
            }
        }

        // tcp_cork
        if (EASY_IOTH_SELF->eio->tcp_cork && c->tcp_cork_flag) {
            easy_socket_set_tcpopt(c->fd, TCP_CORK, 0);
            c->tcp_cork_flag = 0;
        }
    }

    return EASY_OK;
}

/**
 * 得到packet的id
 */
uint64_t easy_connection_get_packet_id(easy_connection_t *c, void *packet, int flag)
{
    uint64_t                packet_id = 0;

    if (c->handler->get_packet_id) {
        packet_id = (c->handler->get_packet_id)(c, packet);
    } else {
        packet_id = c->send_queue->seqno;

        if (flag) packet_id -= c->send_queue->count;

        packet_id <<= 16;
        packet_id |= (c->fd & 0xffff);
    }

    return packet_id;
}

/**
 * new 出一个connection_t对象
 */
static easy_connection_t *easy_connection_new()
{
    easy_pool_t             *pool;
    easy_connection_t       *c;

    // 为connection建pool
    if ((pool = easy_pool_create(0)) == NULL)
        return NULL;

    // 创建easy_connection_t对象
    c = (easy_connection_t *) easy_pool_calloc(pool, sizeof(easy_connection_t));

    if (c == NULL)
        goto error_exit;

    // 初始化
#ifdef EASY_DEBUG_MAGIC
    c->magic = EASY_DEBUG_MAGIC_CONNECT;
#endif

    c->pool = pool;
    c->reconn_time = 100;
    c->idle_time = 60000;
    c->first_msglen = EASY_FIRST_MSGLEN; // 1Kbyte
    c->default_msglen = EASY_IO_BUFFER_SIZE; // 8Kbyte
    c->read = easy_socket_read;
    c->write = easy_socket_write;
    c->fd = -1;
    easy_list_init(&c->message_list);
    easy_list_init(&c->session_list);
    easy_list_init(&c->conn_list_node);
    easy_list_init(&c->group_list_node);
    easy_list_init(&c->output);
    ev_init(&c->pause_watcher, easy_connection_on_pause);

    return c;
error_exit:
    easy_pool_destroy(pool);
    return NULL;
}

/**
 * server回复
 */
static int easy_connection_send_response(easy_list_t *request_list)
{
    easy_request_t              *r, *rn;
    easy_message_t              *m;
    easy_connection_t           *c, *nc;
    easy_list_t                 wlist = EASY_LIST_HEAD_INIT(wlist);
    easy_list_t                 flist = EASY_LIST_HEAD_INIT(flist);
    int                         ret;

    // encode
    easy_list_for_each_entry_safe(r, rn, request_list, request_list_node) {
        easy_list_del(&r->request_list_node);
        m = (easy_message_t *) r->ms;
        c = m->c;

        // 从其他进程返回后
        ret = EASY_ERROR;

        easy_atomic_dec(&m->pool->ref);

        if (r->retcode != EASY_ERROR) {
            if (r->opacket == NULL && r->retcode == EASY_AGAIN) {
                (c->handler->process)(r);
            } else if ((ret = easy_connection_request_done(r)) == EASY_OK) {
                if (easy_list_empty(&c->group_list_node)) {
                    easy_list_add_tail(&c->group_list_node, &wlist);
                }
            }
        } else { // 如果出错
            easy_list_del(&c->group_list_node);
            easy_list_add_tail(&c->group_list_node, &flist);
        }

        // 引用计数
        easy_atomic_dec(&c->pool->ref);

        // message是否也不在使用了
        if (ret == EASY_OK && m->request_list_count == 0 && m->status != EASY_MESG_READ_AGAIN)
            easy_message_destroy(m, 1);
    }

    // failure request, close connection
    easy_list_for_each_entry_safe(c, nc, &flist, group_list_node) {
        easy_list_del(&c->group_list_node);
        easy_connection_destroy(c);
    }

    // foreach write socket
    easy_list_for_each_entry_safe(c, nc, &wlist, group_list_node) {
        easy_list_del(&c->group_list_node);
        if (easy_connection_write_socket(c) == EASY_ABORT) {
            easy_connection_destroy(c);
        } else if (c->type == EASY_TYPE_SERVER) {
            easy_connection_redispatch_thread(c);
        }
    }

    return EASY_OK;
}

int easy_connection_send_session_list(easy_list_t *list)
{
    easy_connection_t       *c, *c1;
    easy_session_t          *s, *s1;
    easy_list_t             wlist = EASY_LIST_HEAD_INIT(wlist);
    int                     status;

    // foreach encode
    easy_list_for_each_entry_safe(s, s1, list, session_list_node) {
        easy_list_del(&s->session_list_node);

        // write buffer
        if (unlikely(s->type >= EASY_TYPE_WBUFFER)) {
            c = s->c;

            if (s->type == EASY_TYPE_WBUFFER) {
                easy_list_add_tail(&((easy_buf_t *)(((easy_message_session_t *)s) + 1))->node, &c->output);
                if (easy_list_empty(&c->group_list_node)) {
                    easy_list_add_tail(&c->group_list_node, &wlist);
                }
            } else if (s->type == EASY_TYPE_PAUSE) {
                easy_connection_pause(c, s->align);
                easy_pool_destroy(s->pool);
            } else if (s->type == EASY_TYPE_LISTEN) {
                easy_connection_listen_watcher(s);
                easy_pool_destroy(s->pool);
            }

            continue;
        }

        // connect, disconnect
        status = s->status;

        if ((c = easy_connection_do_client(s)) == NULL || (status & 0x02))
            continue;

        // build session
        s->c = c;

        if (easy_connection_session_build(s) == EASY_OK) {
            if (easy_list_empty(&s->c->group_list_node)) {
                easy_list_add_tail(&s->c->group_list_node, &wlist);
            }
        }
        //easy_warn_log("sendsocket in send_session_list: %p, packet_id: %" PRIx64 " %s",
        //              s, s->packet_id, easy_connection_str(c));
    }

    // foreach
    easy_list_for_each_entry_safe(c, c1, &wlist, group_list_node) {
        easy_list_del(&c->group_list_node);
        easy_connection_sendsocket(c);
    }

    return EASY_OK;
}

static easy_message_t *easy_connection_recycle_message(easy_message_t *m)
{
    easy_message_t          *newm;
    int                     len, olen;

    len = (m->input->last - m->input->pos);

    if (m->recycle_cnt < 16 || len == 0)
        return m;

    // 增加default_message_len大小
    olen = m->c->first_msglen;
    m->c->first_msglen = easy_max(len, olen);
    newm = easy_message_create(m->c);
    m->c->first_msglen = olen;

    if (newm == NULL)
        return NULL;

    // 把旧的移到新的上面
    memcpy(newm->input->pos, m->input->pos, len);
    newm->input->last += len;
    newm->status = EASY_MESG_READ_AGAIN;
    m->input->pos = m->input->last;

    // 删除之前的message
    if (m->request_list_count == 0) {
        easy_message_destroy(m, 1);
        return newm;
    } else {
        m->status = 0;
        return m;
    }
}

/**
 * 连接到addrv
 */
static easy_connection_t *easy_connection_do_connect(easy_client_t *client, int fd)
{
    struct sockaddr_storage addr;
    int                     v;
    easy_connection_t       *c;
    double                  t;

    // 建立一个connection
    if ((c = easy_connection_new()) == NULL) {
        easy_error_log("new connect failure.\n");
        return NULL;
    }

    memset(&addr, 0, sizeof(addr));
    easy_inet_etoa(&client->addr, &addr);

    if (fd < 0 && (fd = socket(addr.ss_family, SOCK_STREAM, 0)) < 0) {
        easy_error_log("socket failure: %s (%d)\n", strerror(errno), errno);
        goto error_exit;
    }

    // 初始化
    c->fd = fd;
    c->type = EASY_TYPE_CLIENT;
    c->handler = client->handler;
    c->addr = client->addr;
    c->client = client;
    easy_socket_non_blocking(fd);

    // 连接
    if (EASY_IOTH_SELF->eio->tcp_nodelay) {
        easy_socket_set_tcpopt(fd, TCP_NODELAY, 1);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            easy_error_log("connect to %s failure: %s (%d)\n", easy_connection_str(c), strerror(errno), errno);
            goto error_exit;
        }

        c->status = EASY_CONN_CONNECTING;
    }

    // self connect self
    if (easy_connection_checkself(c) == EASY_ERROR) {
        goto error_exit;
    }

    v = offsetof(easy_session_t, send_queue_hash);
    c->send_queue = easy_hash_create(c->pool, EASY_IOTH_SELF->eio->send_qlen, v);

    if (c->send_queue == NULL) {
        easy_error_log("easy_hash_create failure.");
        goto error_exit;
    }

    // 初始化事件
    ev_io_init(&c->read_watcher, easy_connection_on_readable, fd, EV_READ);
    ev_io_init(&c->write_watcher, easy_connection_on_writable, fd, EV_WRITE);
    t = (client->timeout ? client->timeout : EASY_CLIENT_DEFAULT_TIMEOUT) / 1000.0;
    ev_timer_init(&c->timeout_watcher, easy_connection_on_timeout_conn, t, 0.0);
    c->read_watcher.data = c;
    c->write_watcher.data = c;
    c->timeout_watcher.data = c;

    // event_status
    if (c->status == EASY_CONN_CONNECTING)
        v = (EASY_EVENT_TIMEOUT | EASY_EVENT_WRITE);
    else
        v = (EASY_EVENT_TIMEOUT | EASY_EVENT_READ);

    easy_debug_log("connect to '%s' start\n", easy_connection_str(c));

    c->event_status = v;
    c->ioth = EASY_IOTH_SELF;
    c->loop = c->ioth->loop;

    //locate node
    c->con_summary = easy_summary_locate_node(c->fd, c->ioth->eio->eio_summary, 0);

    // 加入
    easy_list_add_tail(&c->conn_list_node, &c->ioth->connected_list);
    easy_connection_evio_start(c);

    return c;
error_exit:

    if (fd >= 0) close(fd);

    easy_pool_destroy(c->pool);
    return NULL;
}

static easy_connection_t *easy_connection_do_client(easy_session_t *s)
{
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    easy_connection_t       *c = NULL;
    easy_client_t           *client;
    int                     error = 0;
    int                     afd = -1;

    // hashcode
    client = (easy_client_t *)easy_client_list_find(ioth->client_list, &s->addr);

    // 正常的session
    if (likely(s->status == 0)) {
        c = (client && client->ref ? client->c : NULL);

        if (s->process == NULL && c && c->handler) {
            s->process = c->handler->process;
        }

        if (unlikely(c == NULL || ioth->eio->stoped)) {
            s->error = 1;
            c = NULL;
            easy_session_process(s, 0);
        }

        return c;
        // 连接指令
    } else if ((s->status & 0x01) != 0) {
        if (client == NULL) {
            if ((client = (easy_client_t *)easy_array_alloc(ioth->client_array)) == NULL) {
                error = 1;
                s->error = 1;
                goto error_exit;
            }

            memset(client, 0, sizeof(easy_client_t));
            client->addr = s->addr;
            client->handler = (easy_io_handler_pt *)s->thread_ptr;
            client->timeout = (int)s->timeout;
            client->user_data = s->r.args;
            client->is_ssl = 0;

            if (s->r.reserved > 0) afd = s->r.reserved;

            if (ioth->eio->ssl && ioth->eio->ssl->client_ctx && (s->packet_id & EASY_CONNECT_SSL)) {
                client->is_ssl = 1;
                client->server_name = s->r.user_data;
            }

            s->thread_ptr = NULL;
            easy_client_list_add(ioth->client_list, &client->addr, &client->client_list_node);
        }

        if (client->c == NULL && (client->c = easy_connection_do_connect(client, afd)) == NULL) {
            error = 1;
            s->error = 1;
            goto error_exit;
        }

        c = client->c;

        if ((s->packet_id & EASY_CONNECT_AUTOCONN)) c->auto_reconn = 1;

        if (s->status != EASY_CONNECT_SEND) client->ref ++;

        // 断开指令
    } else if (client && --client->ref == 0) {
        if ((c = client->c)) {
            c->auto_reconn = 0;
            c->wait_close = 1;
            c->client = NULL;

            if (c->wait_close && c->pool->ref == 0)
                easy_connection_destroy(c);
        }

        easy_hash_del_node(&client->client_list_node);
        easy_array_free(ioth->client_array, client);
    }

error_exit:

    if (s->pool && (s->status & 0x02)) {
        easy_pool_destroy(s->pool);
    } else if (error) {
        easy_session_process(s, 0);
    }

    return c;
}

int easy_connection_request_done(easy_request_t *r)
{
    easy_connection_t       *c;
    easy_message_t          *m;
    int                     retcode = r->retcode;

    m = (easy_message_t *) r->ms;
    c = m->c;

    // encode
    if (r->opacket) {
        if ((c->handler->encode)(r, r->opacket) != EASY_OK)
            return EASY_ERROR;

        easy_request_set_cleanup(r, &c->output);

        if (retcode == EASY_AGAIN) { // 当write_socket写完
            easy_list_add_tail(&r->request_list_node, &c->session_list);
            ev_io_start(c->loop, &c->write_watcher);
            r->opacket = NULL;
        }
    }

    // retcode
    if (retcode == EASY_OK && r->status != EASY_REQUEST_DONE) {
        // 计算
        r->status = EASY_REQUEST_DONE;
        assert(m->request_list_count > 0);
        m->request_list_count --;
        c->con_summary->done_request_count ++;

        // 设置redispatch
        if (c->type == EASY_TYPE_SERVER && EASY_IOTH_SELF->eio->no_redispatch == 0) {
            if (unlikely((c->con_summary->done_request_count & 0xff) == 32)) {
                c->need_redispatch = 1;
            }
        }
    }

    return EASY_OK;
}

static void easy_connection_autoconn(easy_connection_t *c)
{
    int                         fd;
    struct sockaddr_storage     addr;

    c->status = EASY_CONN_CLOSE;

    if (c->client == NULL)
        return;

    memset(&addr, 0, sizeof(addr));
    easy_inet_etoa(&c->addr, &addr);

    if ((fd = socket(addr.ss_family, SOCK_STREAM, 0)) < 0) {
        easy_error_log("socket failure: %s (%d)\n", strerror(errno), errno);
        goto error_exit;
    }

    easy_socket_non_blocking(fd);

    if (EASY_IOTH_SELF->eio->tcp_nodelay) {
        easy_socket_set_tcpopt(fd, TCP_NODELAY, 1);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            easy_error_log("connect to '%s' failure: %s (%d)\n", easy_connection_str(c), strerror(errno), errno);
            close(fd);
            return;
        }

        c->status = EASY_CONN_CONNECTING;
    } else {
        c->status = EASY_CONN_OK;
    }

    // self connect self
    c->fd = fd;

    if (easy_connection_checkself(c) == EASY_ERROR) {
        goto error_exit;
    }

    // 初始化
    c->conn_has_error = 0;
    ev_io_set(&c->read_watcher, fd, EV_READ);
    ev_io_set(&c->write_watcher, fd, EV_WRITE);

    if (c->send_queue) {
        c->send_queue->count = 0;
        c->send_queue->seqno = 1;
        easy_list_init(&c->send_queue->list);
    }

    if (c->status == EASY_CONN_CONNECTING)
        c->event_status = (EASY_EVENT_TIMEOUT | EASY_EVENT_WRITE);
    else
        c->event_status = (EASY_EVENT_TIMEOUT | EASY_EVENT_READ);

    easy_debug_log("reconnect to '%s' start\n", easy_connection_str(c));

    easy_connection_evio_start(c);
    return;
error_exit:
    easy_safe_close(c->fd);
    c->auto_reconn = 0;
    easy_connection_destroy(c);
}

char *easy_connection_str(easy_connection_t *c)
{
    static __thread char    buffer[64];

    if (!c)
        return "null";

    char                    str[32];
    lnprintf(buffer, 64, "%s_%d_%p", easy_inet_addr_to_str(&c->addr, str, 32), c->fd, c);
    return buffer;
}

static int easy_connection_process_request(easy_connection_t *c, easy_list_t *list)
{
    easy_request_t          *r, *rn;
    easy_message_t          *m = NULL;
    easy_list_t             request_list;
    int                     ret, cnt = 0;
    int                     max = (EASY_IOTH_SELF->eio->tcp_nodelay ? 4 : 128);

    assert(c->type == EASY_TYPE_SERVER);
    easy_list_movelist(list, &request_list);
    c->send_queue = (easy_hash_t *)&request_list;

    easy_list_for_each_entry_safe(r, rn, &request_list, request_list_node) {
        m = (easy_message_t *)r->ms;
        easy_list_del(&r->request_list_node);
        EASY_IOTH_SELF->done_request_count ++;

        // process
        ret = (c->handler->process)(r);

        if (ret == EASY_ABORT || ret == EASY_ASYNC || ret == EASY_ERROR) {
            goto error_exit;
        } else if (ret != EASY_OK) {
            continue;
        }

        if (easy_connection_request_done(r) == EASY_OK) {
            cnt ++;
        }

        // write to socket
        if (cnt >= max) {
            cnt = 0;

            if ((ret = easy_connection_write_socket(c)) == EASY_ABORT) {
                goto error_exit;
            }
        }

        // check request count
        if (m->request_list_count == 0 && m->status != EASY_MESG_READ_AGAIN) {
            easy_message_destroy(m, 1);
        }
    }
    c->send_queue = NULL;

    // 所有的request都有reply了,一起才响应
    if (easy_connection_write_socket(c) == EASY_ABORT) {
        return EASY_ERROR;
    }

    return EASY_OK;
error_exit:
    c->send_queue = NULL;

    if (ret != EASY_ASYNC) {
        easy_list_for_each_entry_safe(r, rn, &request_list, request_list_node) {
            easy_list_del(&r->request_list_node);
        }
        ret = EASY_ERROR;
    }

    return ret;
}

/**
 * 使用reuseport
 */
void easy_connection_reuseport(easy_io_t *eio, easy_listen_t *l, int idx)
{
    char                    buffer[32];
    int                     fd, udp;
    int                     flags = (eio->tcp_defer_accept ? EASY_FLAGS_DEFERACCEPT : 0);

    if (!l->reuseport) {
        return;
    }

    flags |= EASY_FLAGS_SREUSEPORT;
    udp = (l->handler && l->handler->is_udp ? 1 : 0);

    if ((fd = easy_socket_listen(udp, &l->addr, &flags, eio->listen_backlog)) < 0) {
        easy_error_log("easy_socket_listen failure: host=%s\n", easy_inet_addr_to_str(&l->addr, buffer, 32));
        return;
    }

    if (easy_atomic_add_return(&l->bind_port_cnt, 1) == eio->io_thread_count) {
        easy_safe_close(l->fd);
        l->fd = fd;
        easy_info_log("easy_socket_listen: host=%s, fd=%d, reuseport=%d", easy_inet_addr_to_str(&l->addr, buffer, 32), fd, l->reuseport);
    }

    // 初始化
    if (udp) {
        ev_io_init(&l->read_watcher[idx], easy_connection_on_udpread, fd, EV_READ | EV_CLEANUP);
    } else {
        ev_io_init(&l->read_watcher[idx], easy_connection_on_accept, fd, EV_READ | EV_CLEANUP);
    }
}

/**
 * udp read
 */
void easy_connection_on_udpread(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    int                     blen, size, n;
    easy_listen_simple_t    *listen;
    struct sockaddr_storage *addr;
    socklen_t               addr_len;
    easy_connection_t       *c;
    easy_message_t          *m;
    easy_request_t          *r;
    easy_pool_t             *pool;
    easy_buf_t              *input;
    void                    *packet;
#ifdef HAVE_RECVMMSG
    int                     i;
    struct iovec            *iovec;
    struct mmsghdr          *hdr;
#endif

    listen = (easy_listen_simple_t *) w->data;
    addr_len = sizeof(struct sockaddr_storage);

    // 让出来给其他的线程
    if (listen->is_simple == 0 && listen->reuseport == 0 && ioth->eio->listen_all == 0)
        easy_switch_listen(listen);

    // 为connection建pool
    blen = 4096;
#ifdef HAVE_RECVMMSG
    size = sizeof(easy_connection_t) + sizeof(easy_message_t) + sizeof(easy_buf_t) + (blen +
            sizeof(struct sockaddr_storage) + sizeof(struct mmsghdr) + sizeof(struct iovec)) * ioth->eio->recv_vlen;
#else
    size = sizeof(easy_connection_t) + sizeof(easy_message_t) + blen + sizeof(easy_buf_t) + sizeof(struct sockaddr_storage);
#endif

    if ((pool = easy_pool_create(size)) == NULL)
        return;

    // 创建easy_connection_t对象
    c = (easy_connection_t *) easy_pool_calloc(pool, sizeof(easy_connection_t));
    m = (easy_message_t *) easy_pool_calloc(pool, sizeof(easy_message_t));
#ifdef HAVE_RECVMMSG
    input = (easy_buf_t *)easy_pool_calloc(pool, sizeof(easy_buf_t));
    iovec = (struct iovec *)easy_pool_calloc(pool, ioth->eio->recv_vlen * (sizeof(struct iovec)));
    hdr = (struct mmsghdr *)easy_pool_calloc(pool, ioth->eio->recv_vlen * (sizeof(struct mmsghdr)));
    addr = (struct sockaddr_storage *)easy_pool_calloc(pool, ioth->eio->recv_vlen * (sizeof(struct sockaddr_storage)));
#else
    input = easy_buf_create(pool, blen);
    addr = (struct sockaddr_storage *)easy_pool_calloc(pool, sizeof(struct sockaddr_storage));
#endif

    if (c == NULL || m == NULL || input == NULL || addr == NULL
#ifdef HAVE_RECVMMSG
            || iovec == NULL || hdr == NULL
#endif
       ) {
        easy_error_log("easy_pool_calloc failure\n");
        goto error_exit;
    }

#ifdef HAVE_RECVMMSG

    for (i = 0; i < ioth->eio->recv_vlen; i++) {
        iovec[i].iov_base = (char *)easy_pool_alloc(pool, blen);
        iovec[i].iov_len = blen;
        hdr[i].msg_hdr.msg_iov = &iovec[i];
        hdr[i].msg_hdr.msg_iovlen = 1;
        hdr[i].msg_hdr.msg_name = (struct sockaddr *)(addr + i);
        hdr[i].msg_hdr.msg_namelen = addr_len;
    }

    if (ioth->eio->recv_vlen == 1) {
        n = recvfrom(w->fd, iovec[0].iov_base, blen, 0, (struct sockaddr *)addr, &addr_len);

        if (n <= 0) {
            goto error_exit;
        }

        hdr[0].msg_len = n;
        n = 1;
    } else {
        n = recvmmsg(w->fd, hdr, ioth->eio->recv_vlen, MSG_DONTWAIT, NULL);
    }

#else
    // recvfrom
    n = recvfrom(w->fd, input->last, blen, 0, (struct sockaddr *)addr, &addr_len);
#endif

    if (n <= 0) {
        goto error_exit;
    }

    c->pool = pool;
    c->loop = loop;
    c->handler = listen->handler;
    c->fd = w->fd;
    easy_list_init(&c->output);
    m->c = c;
    m->pool = pool;
    input->args = pool;
    easy_list_init(&input->node);
    m->input = input;

#ifdef HAVE_RECVMMSG

    for(i = 0; i < n; i++) {
        input->pos = iovec[i].iov_base;
        input->last = input->pos + hdr[i].msg_len;

        easy_inet_atoe(addr + i, &c->addr);
        m->user_data = NULL;
#else
    easy_inet_atoe(addr, &c->addr);
    input->last += n;
#endif

        if ((packet = (c->handler->decode)(m)) == NULL) {
            goto error_exit;
        }

        r = (easy_request_t *)easy_pool_calloc(m->pool, sizeof(easy_request_t));

        if (r == NULL) {
            goto error_exit;
        }

#ifdef EASY_DEBUG_DOING
        r->uuid = easy_atomic_add_return(&easy_debug_uuid, 1);
        EASY_PRINT_BT("doing_request_count_inc:%d,c:%s,r:%p,%ld.", c->doing_request_count, easy_connection_str(c), r, r->uuid);
#endif

        r->ms = (easy_message_session_t *)m;
        r->ipacket = packet;    //进来的数据包

        // process
        (c->handler->process)(r);

        // encode
        if (r->opacket) {
            (c->handler->encode)(r, r->opacket);
            easy_socket_usend(c, &c->output);
            r->opacket = NULL;
        }

#ifdef HAVE_RECVMMSG
    }

#endif

error_exit:
    easy_pool_destroy(pool);
}


static int easy_connection_sendsocket(easy_connection_t *c)
{
    // 等下次写出去
    if (c->status != EASY_CONN_OK || ev_is_active(&c->write_watcher))
        return EASY_OK;

    // 写出到socket
    if (easy_connection_write_socket(c) == EASY_ABORT) {
        easy_connection_destroy(c);
        return EASY_ABORT;
    }

    return EASY_OK;
}

/**
 * listen dispatch
 */
static int easy_connection_listen_dispatch(easy_io_t *eio, easy_addr_t addr, easy_listen_t *listen)
{
    int                     ret;
    easy_session_t          *s;

    s = easy_connection_connect_init(NULL, (easy_io_handler_pt *)listen, 0, NULL, 0, NULL);
    s->type = EASY_TYPE_LISTEN;

    if ((ret = easy_client_dispatch(eio, addr, s)) != EASY_OK)
        easy_session_destroy(s);

    return ret;
}

static void easy_connection_listen_watcher(easy_session_t *s)
{
    easy_listen_t           *l = (easy_listen_t *) s->thread_ptr;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    easy_io_t               *eio = ioth->eio;
    char                    buffer[32];

    if (eio->listen_all || eio->io_thread_count == 1 || l->reuseport) {
        easy_connection_reuseport(eio, l, ioth->idx);
        ev_io_start(ioth->loop, &l->read_watcher[ioth->idx]);
    } else {
        ev_timer_start (ioth->loop, &ioth->listen_watcher);
    }

    easy_spin_lock(&eio->lock);
    l->next = eio->listenadd;
    eio->listenadd = l;
    easy_spin_unlock(&eio->lock);

    easy_debug_log("add listen: %s\n", easy_inet_addr_to_str(&l->addr, buffer, 32));
}

int easy_connection_destroy_dispatch(easy_connection_t *c)
{
    shutdown(c->fd, SHUT_RD);
    return 0;
}

static int easy_connection_checkself(easy_connection_t *c)
{
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);

    // self connect self
    if (c->addr.family == AF_INET && getsockname(c->fd, (struct sockaddr *)&addr, &len) == 0) {
        if (addr.sin_port == c->addr.port && addr.sin_addr.s_addr == c->addr.u.addr) {
            easy_error_log("connect to %s failure, self connect self, %d-%d\n", 
                            easy_connection_str(c),
                            ntohs(addr.sin_port),
                            ntohs(c->addr.port));
            return EASY_ERROR;
        }
    }

    return EASY_OK;
}

static void easy_connection_dump_slow_request(easy_connection_t *c)
{
    easy_message_t *m, *mn;
    easy_request_t *r, *rn;
    char                    btmp[128];
    easy_error_log("start dump request on connection %s", easy_connection_str(c));
    easy_list_for_each_entry_safe(m, mn, &c->message_list, message_list_node) {
        easy_error_log("start dump request on message %p", m);
        easy_list_for_each_entry_safe(r, rn, &m->all_list, all_node) {
            easy_error_log("dump request r->retcode=%d", r->retcode);
            easy_error_log("dump request r->ipacket=%s", easy_string_tohex(r->ipacket, 32, btmp, 128));
        }
        easy_error_log("end dump request on message %p", m);
    }
    easy_error_log("end dump request on connection %s", easy_connection_str(c));
    c->slow_request_dumped = 1;
}

static void easy_connection_cleanup_buffer(easy_buf_t *b, void *args)
{
    easy_pool_t *pool = (easy_pool_t *) args;
    easy_pool_destroy(pool);
}

/**
 * 发送数据
 */
int easy_connection_write_buffer(easy_connection_t *c, const char *data, int len)
{
    easy_buf_t             *b;
    easy_io_thread_t       *ioth;
    easy_pool_t            *pool = NULL;
    easy_message_session_t *ms;

    if (c->status == EASY_CONN_CLOSE) {
        goto error_exit;
    }

    // pool create
    ioth = c->ioth;

    if (len == -1) len = strlen(data);

    if ((pool = easy_pool_create(len + 512)) == NULL) {
        goto error_exit;
    }

    // self thread
    if (ioth == (easy_io_thread_t *)easy_baseth_self) {
        if ((b = easy_buf_create(pool, len)) == NULL) {
            goto error_exit;
        }

        b->last = easy_memcpy(b->last, data, len);
        easy_list_add_tail(&b->node, &c->output);
        easy_buf_set_cleanup(b, easy_connection_cleanup_buffer, pool);
        return easy_connection_write_socket(c);
    } else {
        // easy_message_session_t + easy_buf_t + data
        ms = easy_pool_alloc(pool, sizeof(easy_message_session_t) + sizeof(easy_buf_t) + len);

        if (ms == NULL) {
            goto error_exit;
        }

        b = (easy_buf_t *)(ms + 1);
        memcpy((char *)(b + 1), data, len);
        easy_buf_set_data(pool, b, (char *)(b + 1), len);
        easy_buf_set_cleanup(b, easy_connection_cleanup_buffer, pool);
        ms->type = EASY_TYPE_WBUFFER;
        ms->c = c;
        ms->pool = pool;

        easy_spin_lock(&ioth->thread_lock);
        easy_list_add_tail(&ms->list_node, &ioth->session_list);
        easy_spin_unlock(&ioth->thread_lock);
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }

    return EASY_OK;
error_exit:

    if (pool) easy_pool_destroy(pool);

    return EASY_ERROR;
}

static void easy_connection_on_pause(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_connection_t *c = (easy_connection_t *)w->data;

    if (c->status == EASY_CONN_OK) {
        ev_io_start(c->loop, &c->read_watcher);
    }
}

int easy_connection_pause(easy_connection_t *c, int ms)
{
    easy_io_thread_t *ioth = c->ioth;

    if (ioth == (easy_io_thread_t *)easy_baseth_self) {
        ev_io_stop(c->loop, &c->read_watcher);

        if (ev_is_active(&c->pause_watcher)) {
            ev_timer_stop(c->loop, &c->pause_watcher);
        }

        ev_timer_set(&c->pause_watcher, ms / 1000.0, 0.0);
        c->pause_watcher.data = c;
        ev_timer_start(c->loop, &c->pause_watcher);
    } else {
        easy_pool_t *pool = easy_pool_create(0);
        easy_message_session_t *s = easy_pool_alloc(pool, sizeof(easy_message_session_t));
        s->type = EASY_TYPE_PAUSE;
        s->align = ms;
        s->c = c;
        s->pool = pool;

        easy_spin_lock(&ioth->thread_lock);
        easy_list_add_tail(&s->list_node, &ioth->session_list);
        easy_spin_unlock(&ioth->thread_lock);
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }

    return EASY_OK;
}
