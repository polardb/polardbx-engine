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


#ifndef EASY_UTHREAD_H
#define EASY_UTHREAD_H

#include <easy_pool.h>
#include <easy_list.h>
#include <ucontext.h>

/**
 * 创建一用户态线程
 */

EASY_CPP_START

#define EASY_UTHREAD_STACK         (65536-sizeof(easy_pool_t))

typedef void (easy_uthread_start_pt)(void *args);
typedef struct easy_uthread_t easy_uthread_t;
typedef struct easy_uthread_control_t easy_uthread_control_t;

struct easy_uthread_t {
    easy_list_t                runqueue_node;
    easy_list_t                thread_list_node;
    easy_pool_t                *pool;
    easy_uthread_start_pt      *startfn;
    void                       *startargs;

    uint32_t                   id;
    int8_t                     exiting;
    int8_t                     ready;
    int8_t                     errcode;
    uint32_t                   stksize;
    unsigned char              *stk;
    ucontext_t                 context;
};

struct easy_uthread_control_t {
    int                        gid;
    int                        nswitch;
    int16_t                    stoped;
    int16_t                    thread_count;
    int                        exit_value;
    easy_list_t                runqueue;
    easy_list_t                thread_list;
    easy_uthread_t             *running;
    ucontext_t                 context;
};

// 函数
void easy_uthread_init(easy_uthread_control_t *control);
void easy_uthread_destroy();
easy_uthread_t *easy_uthread_create(easy_uthread_start_pt *start, void *args, int stack_size);
easy_uthread_t *easy_uthread_current();
int easy_uthread_yield();
int easy_uthread_scheduler();
void easy_uthread_stop();
void easy_uthread_ready(easy_uthread_t *t);
void easy_uthread_switch();
void easy_uthread_needstack(int n);
void easy_uthread_ready(easy_uthread_t *t);
void easy_uthread_print(int sig);
int easy_uthread_get_errcode();
void easy_uthread_set_errcode(easy_uthread_t *t, int errcode);

//////////////////////
// 下面对main的处理
#define EASY_UTHREAD_RUN_MAIN(main_name)                                                \
    static int              easy_uthread_stacksize = 0;                                          \
    static int              easy_uthread_argc;                                                   \
    static char             **easy_uthread_argv;                                                 \
    static void easy_uthread_mainstart(void *v)                                         \
    {                                                                                   \
        main_name(easy_uthread_argc, easy_uthread_argv);                                \
    }                                                                                   \
    int main(int argc, char **argv)                                                     \
    {                                                                                   \
        int                     ret; struct sigaction sa, osa;                                              \
        easy_uthread_control_t  control;                                                 \
        memset(&sa, 0, sizeof sa);                                                      \
        sa.sa_handler = easy_uthread_print;                                             \
        sa.sa_flags = SA_RESTART;                                                       \
        sigaction(SIGQUIT, &sa, &osa);                                                  \
        easy_uthread_argc = argc;                                                       \
        easy_uthread_argv = argv;                                                       \
        if (easy_uthread_stacksize == 0) easy_uthread_stacksize = 256*1024;             \
        easy_uthread_init(&control);                                                    \
        easy_uthread_create(easy_uthread_mainstart, NULL, easy_uthread_stacksize);      \
        ret = easy_uthread_scheduler();                                                 \
        easy_uthread_destroy();                                                         \
        return ret;                                                                     \
    }

EASY_CPP_END

#endif
