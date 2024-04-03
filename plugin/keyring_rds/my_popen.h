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

#ifndef INCLUDE_POPEN_WITH_TIME__
#define INCLUDE_POPEN_WITH_TIME__

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/**
  Create a child process to execute the specified command,
  caller can read the output of this command from the pipeline.

  @para[in]  command    Command to be executed
  @para[out] output_fd  File descriptor of the pipe output
  @para[out] child_pid  Process id of the child
  @return    true       Error occurs
  @return    false      Success
*/
extern bool keyring_popen(const char *command, int *output_fd,
                          pid_t *child_pid);

/**
  Close the pipeline, and wait for the child process to exit.
  Especially, KILL the child process if it is still running.

  @para[in]  output_fd  File descriptor of the pipe output
  @para[in]  child_pid  Process id of the child
*/
extern void keyring_pclose(int output_fd, pid_t child_pid);

/**
  Read data from the pipeline until it encounters a newline or EOF.
  Especially, we expect the reading to be completed in timeout_sec.

  @para[in]  output_fd   File descriptor of the pipe output
  @para[in]  child_pid   Process id of the child
  @para[out] buf         Reading buffer
  @para[in]  buf_len     Length of reading buffer
  @para[in]  timeout_sec Timeout in seconds
  @return    N           Bytes read successfully
*/
extern size_t keyring_read(int output_fd, pid_t child_pid, char *buf,
                           size_t buf_len, int timeout_sec);

#endif
