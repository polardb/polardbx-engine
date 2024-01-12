//
// Created by zzy on 2022/8/29.
//

#include <iostream>
#include <thread>

#include "easy_net_perf.h"

namespace polarx_rpc {

typedef struct cmdline_param {
  int port;
  int io_thread_cnt;
  int work_thread_cnt;
  easy_thread_pool_t *threads;
} cmdline_param;

static cmdline_param cp;

typedef struct echo_packet_t {
  int len;
  char *data;
  char buffer[0];
} echo_packet_t;

static void *echo_decode(easy_message_t *m) {
  echo_packet_t *packet;
  long request_size;

  if ((packet = (echo_packet_t *)easy_pool_calloc(
           m->pool, sizeof(echo_packet_t))) == NULL)
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

static int echo_encode(easy_request_t *r, void *data) {
  echo_packet_t *packet;
  easy_buf_t *b;

  packet = (echo_packet_t *)data;

  if ((b = easy_buf_create(r->ms->pool, packet->len)) == NULL)
    return EASY_ERROR;

  memcpy(b->pos, packet->data, packet->len);
  b->last += packet->len;

  easy_request_addbuf(r, b);

  return EASY_OK;
}

static int echo_async_process(easy_request_t *r) {
  easy_thread_pool_push(cp.threads, r, easy_hash_key((uint64_t)(long)r));
  return EASY_AGAIN;
}

static echo_packet_t *echo_packet_rnew(easy_request_t *r, int size) {
  echo_packet_t *packet;

  if ((packet = (echo_packet_t *)easy_pool_alloc(
           r->ms->pool, sizeof(echo_packet_t) + size)) == NULL)
    return NULL;

  packet->len = size;
  packet->data = &packet->buffer[0];

  return packet;
}

static int easy_async_request_process(easy_request_t *r, void *args) {
  echo_packet_t *ipacket = (echo_packet_t *)r->ipacket;
  echo_packet_t *opacket;

  opacket = echo_packet_rnew(r, ipacket->len);
  memcpy(opacket->data, ipacket->data, ipacket->len);
  r->opacket = opacket;

  return EASY_OK;
}

static int echo_process(easy_request_t *r) {
  // 直接用in packet
  r->opacket = r->ipacket;
  return EASY_OK;
}

static void init() {
  cp.port = 33661;
  cp.io_thread_cnt = 64;
  cp.work_thread_cnt = 64;
}

static int run() {
  easy_listen_t *listen;
  easy_io_handler_pt io_handler;
  int ret;

  // 检查必需参数
  if (cp.port == 0) {
    return EASY_ERROR;
  }

  // 对easy_io初始化, 设置io的线程数, file的线程数
  if (!easy_io_create(cp.io_thread_cnt)) {
    easy_error_log("easy_io_init error.\n");
    return EASY_ERROR;
  }

  // 为监听端口设置处理函数，并增加一个监听端口
  memset(&io_handler, 0, sizeof(io_handler));
  io_handler.decode = echo_decode;
  io_handler.encode = echo_encode;
  io_handler.process = echo_process;
  // io_handler.process = echo_async_process;
  cp.threads = easy_request_thread_create(cp.work_thread_cnt,
                                          easy_async_request_process, NULL);

  if ((listen = easy_io_add_listen(NULL, cp.port, &io_handler)) == NULL) {
    easy_error_log("easy_io_add_listen error, port: %d, %s\n", cp.port,
                   strerror(errno));
    return EASY_ERROR;
  } else {
    easy_error_log("listen start, port = %d\n", cp.port);
  }

  // 起处理速度统计定时器
  ev_timer stat_watcher;
  easy_io_stat_t iostat;
  easy_io_stat_watcher_start(&stat_watcher, 5.0, &iostat, NULL);

  std::cout << "libeasy start" << std::endl;

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

void easy_net_perf() {
  init();
  std::thread th(run);
  th.detach();
}

}  // namespace polarx_rpc
