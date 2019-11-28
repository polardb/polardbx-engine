/* Copyright (c) 2016, 2018, Alibaba and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef WIN32
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sys/wait.h>
#include <sys/epoll.h>

#include <chrono>
#include <functional>
#include <map>
#include <thread>

#include "keyring_rds_logger.h"
#include "my_popen.h"

/**
  Notes:

  We rewrote the series of popen functions in order to be able to control
  command execution time.
  Prevent RDS threads from being blocked for a very long time in extreme
  cases.
*/

/**
  A contianer that stores all the pipeline output FDs created by keyring_popen.
*/
class FD_Container {
 public:
  FD_Container() { pthread_mutex_init(&mylock, NULL); }
  ~FD_Container() { pthread_mutex_destroy(&mylock); }

  void add_fd(int fd) {
    pthread_mutex_lock(&mylock);
    fds.emplace(fd, 0);
    pthread_mutex_unlock(&mylock);
  }

  void del_fd(int fd) {
    pthread_mutex_lock(&mylock);
    fds.erase(fd);
    pthread_mutex_unlock(&mylock);
  }

  pthread_mutex_t mylock;
  std::map<int, int> fds;
};

static FD_Container fd_cache; /* The internal global instance */

/**
  Create a child process to execute the specified command,
  caller can read the output of this command from the pipeline.
*/
bool keyring_popen(const char *command, int *parent_fd, pid_t *child_pid) {
  int pfd[2];
  int parent_end, child_end;
  int flags;
  pid_t child;

  if (pipe(pfd) < 0) {
    keyring_rds::Logger::log(ERROR_LEVEL, "pipe failed, error [%d, %s]", errno,
                             strerror(errno));
    return true;
  }

  parent_end = pfd[0];
  child_end = pfd[1];

  child = fork();

  if (child < 0)
    keyring_rds::Logger::log(ERROR_LEVEL, "fork failed, error [%d, %s]", errno,
                             strerror(errno));

  if (child == 0) { /* Child process */
    close(parent_end);

    /* Directed to standard output stream */
    if (child_end != STDOUT_FILENO) {
      dup2(child_end, STDOUT_FILENO);
      close(child_end);
    }

    /* Close all descriptors in parent process */
    for (auto &elem : fd_cache.fds) {
      if (elem.first != STDOUT_FILENO) close(elem.first);
    }

    execl("/bin/sh", "sh", "-c", command, (char *)0);
    _exit(127);
  }

  close(child_end);

  if (child < 0) { /* fork() error */
    close(parent_end);
    return true;
  }

  *parent_fd = parent_end;
  *child_pid = child;

  fd_cache.add_fd(parent_end);

  /* No blocking mode */
  flags = fcntl(parent_end, F_GETFL, 0);
  if (flags == -1) flags = 0;
  fcntl(parent_end, F_SETFL, flags | O_NONBLOCK);

  return false;
}

/**
  Close the pipeline, and wait for the child process to exit.
  Especially, KILL the child process if it is still running.
*/
void keyring_pclose(int fd, pid_t child_pid) {
  close(fd);
  fd_cache.del_fd(fd);

  /* We expect the child process has been exited. If not, kill it */
  while (waitpid(child_pid, NULL, WNOHANG) == 0) {
    kill(child_pid, SIGKILL);
    /* wait a moment */
    std::chrono::milliseconds timespan(100);
    std::this_thread::sleep_for(timespan);
  }
}

/**
  Read data from the pipeline until it encounters a newline or EOF.
  Especially, we expect the reading to be completed in timeout_sec.
*/
size_t keyring_read(int fd, pid_t pid, char *buf, size_t buf_len,
                    int timeout_sec) {
  time_t start = time(0);
  size_t total_read = 0;
  int can_wait_sec = timeout_sec;
  int epfd = -1;
  int ret = -1;
  struct epoll_event ev_in, ev_out;

  epfd = epoll_create(1);
  if (epfd < 0) {
    keyring_rds::Logger::log(ERROR_LEVEL, "epoll_create error [%d, %s]",
                             errno, strerror(errno));
    buf[0] = 0;
    return 0;
  }

  ev_in.events = EPOLLIN | EPOLLET | EPOLLERR | EPOLLHUP;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev_in)) {
    keyring_rds::Logger::log(ERROR_LEVEL, "epoll_ctl error [%d, %s]",
                             errno, strerror(errno));
    close(epfd);
    buf[0] = 0;
    return 0;
  }

  while (can_wait_sec > 0 && total_read < (buf_len - 1)) {
    ret = epoll_wait(epfd, &ev_out, 1, 1000);

    if (ret < 0) {
      if (errno != EINTR) {
        keyring_rds::Logger::log(ERROR_LEVEL, "epoll_wait error [%d, %s]",
                                 errno, strerror(errno));
        break;
      }
    } else if (ret == 0) {
      /**
        Sometimes the child process has been exited, but the pipeline fd
        is still working without errors. To avoid unnecessary waiting, check if
        the child process is still running.
      */
      if (waitpid(pid, NULL, WNOHANG) == pid) break;
    } else {
      if (!(ev_out.events & EPOLLIN)) {
        if (ev_out.events & EPOLLERR)
          keyring_rds::Logger::log(ERROR_LEVEL,
                                   "epoll_wait EPOLLERR error [%d, %s]",
                                   errno, strerror(errno));
        break;
      }

      /* Now we can read from pipeline */
      bool stop_read = false;

      do {
        char *cur_ptr = buf + total_read;

        size_t bytes_read = read(fd, cur_ptr, buf_len - total_read - 1);

        if (bytes_read == 0) {
          stop_read = true; /* pipeline closed */
          break;
        }

        if (bytes_read == (size_t)-1) {
          if (errno != EAGAIN && errno != EINTR)
            stop_read = true; /* some error occurs */
          break;
        }

        /* Check if we have got a NewLine, if so then stop reading */
        *(cur_ptr + bytes_read) = 0;
        char *pos = (char *) strchr(cur_ptr, '\n');
        if (pos != NULL) {
          total_read += (size_t)(pos - cur_ptr);
          stop_read = true; /* We have got the first line */
          break;
        }

        total_read += bytes_read;
      } while (true);

      if (stop_read) break;
    }

    time_t diff;
    time_t now = time(0);
    if (now < start || (diff = (now - start)) >= INT_MAX)
      break; /* Should not be here */

    can_wait_sec -= (int)diff;
  }

  epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
  close(epfd);

  buf[total_read] = 0;
  return total_read;
}
#else
bool my_popen(const char *, int *, pid_t *) { return true; }
void my_pclose(int, pid_t) {}
size_t my_read(int, pid_t, char *, size_t, int) { return 0; }
#endif
