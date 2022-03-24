#include <getopt.h>
#include <easy_io.h>
#include <easy_http_handler.h>
#include <fcntl.h>

// 命令行参数结构
typedef struct cmdline_param {
    int                     port;
    int                     io_thread_cnt;
    int                     file_thread_cnt;
    int                     print_stat;
    char                    root_dir[256];
    easy_thread_pool_t      *threads;
} cmdline_param;

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
cmdline_param           cp;
static void print_usage(char *prog_name);
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp);
static int easy_http_server_on_process(easy_request_t *r);
static void easy_buf_string_tobuf(easy_pool_t *pool, easy_list_t *bc, char *name, easy_buf_string_t *s);
static void easy_http_request_tobuf(easy_pool_t *pool, easy_list_t *bc, easy_http_request_t *p);
static int easy_http_server_request_process(easy_request_t *r, void *args);

__thread easy_client_wait_t client_wait = {0};
/**
 * 程序入口
 */
int main(int argc, char **argv)
{
    easy_listen_t           *listen;
    easy_io_handler_pt      io_handler;
    int                     ret;

    // default
    memset(&cp, 0, sizeof(cmdline_param));
    cp.io_thread_cnt = 1;
    cp.file_thread_cnt = 1;

    // parse cmd line
    if (parse_cmd_line(argc, argv, &cp) == EASY_ERROR)
        return EASY_ERROR;

    // 检查必需参数
    if (cp.port == 0) {
        print_usage(argv[0]);
        return EASY_ERROR;
    }

    // 对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(cp.io_thread_cnt)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    // 为监听端口设置处理函数，并增加一个监听端口
    easy_io_var.tcp_nodelay = 0;
    easy_io_var.tcp_cork = 0;
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = easy_http_server_on_process;
    cp.threads = easy_request_thread_create(cp.file_thread_cnt,
                                            easy_http_server_request_process, NULL);

    if ((listen = easy_io_add_listen("", cp.port, &io_handler)) == NULL) {
        easy_error_log("easy_io_add_listen error, port: %d, %s\n",
                       cp.port, strerror(errno));
        return EASY_ERROR;
    } else {
        easy_error_log("listen start, port = %d\n", cp.port);
    }

    // 起处理速度统计定时器
    if (cp.print_stat) {
        ev_timer                stat_watcher;
        easy_io_stat_t          iostat;
        easy_io_stat_watcher_start(&stat_watcher, 5.0, &iostat, NULL);
    }

    // 起线程并开始
    if (easy_io_start()) {
        easy_error_log("easy_io_start error.\n");
        return EASY_ERROR;
    }

    // 等待线程退出
    ret = easy_io_wait();
    easy_io_destroy();

    return ret;
}

/**
 * 命令行帮助
 */
static void print_usage(char *prog_name)
{
    fprintf(stderr, "%s -p port [-R root_dir] [-t thread_cnt]\n"
            "    -p, --port              server port\n"
            "    -R, --root_dir          root directory\n"
            "    -t, --io_thread_cnt     thread count for listen, default: 1\n"
            "    -D, --file_thread_cnt   thread count for disk io, default: 1\n"
            "    -s, --print_stat        print statistics\n"
            "    -h, --help              display this help and exit\n"
            "    -V, --version           version and build time\n\n"
            "eg: %s -p 5000\n\n", prog_name, prog_name);
}

/**
 * 解析命令行
 */
static int parse_cmd_line(int argc, char *const argv[], cmdline_param *cp)
{
    int                     opt;
    const char              *opt_string = "hVp:t:R:D:s";
    struct option           long_opts[] = {
        {"port", 1, NULL, 'p'},
        {"root_dir", 1, NULL, 'R'},
        {"io_thread_cnt", 1, NULL, 't'},
        {"file_thread_cnt", 1, NULL, 'D'},
        {"print_stat", 0, NULL, 's'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {0, 0, 0, 0}
    };

    opterr = 0;

    while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
        switch (opt) {
        case 'p':
            cp->port = atoi(optarg);
            break;

        case 'R':

            if (realpath(optarg, cp->root_dir) == NULL) {
                cp->root_dir[0] = '\0';
                fprintf(stderr, "directory: %s not found.\n", optarg);
                return EASY_ERROR;
            }

            break;

        case 't':
            cp->io_thread_cnt = atoi(optarg);
            break;

        case 'D':
            cp->file_thread_cnt = atoi(optarg);
            break;

        case 's':
            cp->print_stat = 1;
            break;

        case 'V':
            fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
            return EASY_ERROR;

        case 'h':
            print_usage(argv[0]);
            return EASY_ERROR;

        default:
            break;
        }
    }

    return EASY_OK;
}

static int easy_http_server_request_process_one(easy_request_t *r)
{
    easy_file_task_t        *ft = (easy_file_task_t *) r->args;
    easy_http_request_t     *p = (easy_http_request_t *) r->ipacket;
    int                     rc;

    while(ft->offset < ft->count) {
        easy_file_task_reset(ft, 0);
        rc = pread(ft->fd, ft->buffer, ft->bufsize, ft->offset);
        fprintf(stderr, "rc:%d\n", rc);

        if (rc <= 0)
            return EASY_ERROR;

        ft->b->last = ft->buffer + rc;
        easy_buf_chain_offer(&p->output, ft->b);
        r->opacket = p;

        if (ft->offset == 0) {
            p->content_length = ft->count;
            p->is_raw_header = 0;
        } else {
            p->is_raw_header = 1;
        }

        ft->offset += rc;
        easy_request_wakeup(r);

        // wait
        pthread_mutex_lock(&client_wait.mutex);
        pthread_cond_wait(&client_wait.cond, &client_wait.mutex);
        pthread_mutex_unlock(&client_wait.mutex);
    }

    close(ft->fd);

    return EASY_OK;
}

static int easy_http_server_request_process(easy_request_t *r, void *args)
{
    easy_file_task_t        *ft = (easy_file_task_t *) r->args;
    easy_http_request_t     *p = (easy_http_request_t *) r->ipacket;
    int                     rc = 0;
    easy_file_buf_t         *fb = NULL;

    if (p->str_query_string.len == 0) {
        easy_file_task_reset(ft, EASY_BUF_FILE);
        posix_fadvise(ft->fd, ft->offset, ft->bufsize, POSIX_FADV_WILLNEED);
        // set sendfile to output
        fb = (easy_file_buf_t *)ft->b;
        fb->fd = ft->fd;
        fb->offset = ft->offset;
        fb->count = ft->bufsize;
        rc = ft->bufsize;
    } else if (memcmp("one=y", p->str_query_string.data, 5) == 0) {
        easy_client_wait_init(&client_wait);
        r->retcode = EASY_AGAIN;
        r->client_wait = &client_wait;
        rc = easy_http_server_request_process_one(r);
        r->client_wait = NULL;
        easy_client_wait_cleanup(&client_wait);
        return rc;
    } else {
        easy_file_task_reset(ft, 0);
        rc = pread(ft->fd, ft->buffer, ft->bufsize, ft->offset);
        ft->b->last = ft->buffer + rc;
    }

    if (rc > 0) {
        easy_buf_chain_offer(&p->output, ft->b);
        r->opacket = p;
    }

    // first
    if (ft->offset == 0) {
        p->content_length = ft->count;
        p->is_raw_header = 0;
    } else {
        p->is_raw_header = 1;
    }

    // next
    ft->offset += rc;

    if (ft->offset >= ft->count || rc <= 0) {
        if (fb)
            easy_file_buf_set_close(fb);
        else
            close(ft->fd);

        return EASY_OK;
    } else if (ft->offset + ft->bufsize > ft->count) {
        ft->bufsize = ft->count - ft->offset;
    }

    return EASY_AGAIN;
}


/**
 * 处理函数
 */
static int easy_http_server_on_process(easy_request_t *r)
{
    if (r->retcode == EASY_AGAIN) {
        if (r->client_wait) {
            easy_client_wait_wakeup_request(r);
        } else {
            easy_thread_pool_push(cp.threads, r, easy_hash_key((uint64_t)(long)r));
        }

        return EASY_AGAIN;
    }

    easy_http_request_t         *p;
    int                         fd;
    char                        filename[128], url[128];

    p = (easy_http_request_t *)r->ipacket;

    if (!cp.root_dir[0]) {
        easy_http_request_tobuf(r->ms->pool, &p->output, p);
        p->content_length = 0;
    } else {
        // 处理文件名
        url[0] = '\0';

        if (p->str_path.len) {
            lnprintf(url, 128, "%s", easy_buf_string_ptr(&p->str_path));

            if (strchr(url, '?')) fprintf(stderr, "p->str_path.len: %d\n", p->str_path.len);
        }

        if (easy_http_merge_path(filename, 128, cp.root_dir, url)) {
            easy_buf_string_set(&p->status_line, EASY_HTTP_STATUS_400);
            return EASY_OK;
        }

        // 打开文件
        fd = open(filename, O_RDONLY);

        if (fd >= 0) {
            r->args = (void *)easy_file_task_create(r, fd, 0);
            easy_thread_pool_push(cp.threads, r, easy_hash_key((uint64_t)(long)r));
            return EASY_AGAIN;
        } else {
            easy_warn_log("open file failure: %s\n", filename);
            easy_buf_string_set(&p->status_line, EASY_HTTP_STATUS_404);
        }
    }

    r->opacket = p;

    return EASY_OK;
}

static void easy_buf_string_tobuf(easy_pool_t *pool, easy_list_t *bc, char *name, easy_buf_string_t *s)
{
    if (s->len) {
        easy_buf_t *b;

        if (s->len > 1000) {
            b = easy_buf_check_write_space(pool, bc, strlen(name) + 64);
            b->last += sprintf(b->last, "%s%d<br>\n", name, s->len);
        } else {
            b = easy_buf_check_write_space(pool, bc, s->len + strlen(name) + 16);
            b->last += sprintf(b->last, "%s%.*s<br>\n", name, s->len, easy_buf_string_ptr(s));
        }
    }
}

static void easy_http_request_tobuf(easy_pool_t *pool, easy_list_t *bc, easy_http_request_t *p)
{
    easy_string_pair_t      *t;

    easy_buf_string_tobuf(pool, bc, "path: ", &p->str_path);
    easy_buf_string_tobuf(pool, bc, "query_sring: ", &p->str_query_string);
    easy_buf_string_tobuf(pool, bc, "str_fragment: ", &p->str_fragment);
    easy_buf_string_tobuf(pool, bc, "str_body: ", &p->str_body);
    // headers
    easy_list_for_each_entry(t, &p->headers_in->list, list) {
        if (t->name.len && t->value.len) {
            easy_buf_t              *b = easy_buf_check_write_space(pool, bc, t->name.len + t->value.len + 16);
            b->last += sprintf(b->last, "%.*s: %.*s<br>\n",
                               t->name.len, easy_buf_string_ptr(&t->name),
                               t->value.len, easy_buf_string_ptr(&t->value));
        }
    }
}
