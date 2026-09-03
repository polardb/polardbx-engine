/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

#ifndef DISK_SENTINEL_INCLUDE
#define DISK_SENTINEL_INCLUDE

#include <atomic>
#include "mysql/components/services/bits/mysql_mutex_bits.h"
#include "mysql/components/services/bits/mysql_cond_bits.h"
#include "mysql/components/services/bits/my_thread_bits.h"
#include "mysql/components/services/bits/psi_mutex_bits.h"
#include "mysql/components/services/bits/psi_thread_bits.h"

#include "my_io.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key key_disk_writeable_sentinel;
extern PSI_mutex_key key_disk_hang_sentinel;
extern PSI_thread_key key_thread_disk_writeable_sentinel;
extern PSI_thread_key key_thread_disk_hang_sentinel;
#endif

/*
 * this module works on leader server
 * DiskSentinel manages a writeable thread and a hang thread.
 * writeable thread updates a timestamp to a file in datadir periodically and inc cnt.
 * hang thread check the cnt, abort the server if checker thread hangs or fail to update data.
 */
class DiskSentinel
{
public:
  DiskSentinel() = default;
  ~DiskSentinel() = default;

  int init();
  void cleanup();

  class DiskItem {
  public:
    DiskItem(const char *disk) : disk_name(disk) {}
    ~DiskItem() = default;

  void check_disk_writeable();
  void check_disk_hang();

  public:
    const char *disk_name{""};
    char file_name[FN_REFLEN]{'\0'};
    std::atomic<uint64_t> check_count{0};
    uint64_t last_write_delay{0};
    uint64_t fail_retries{0};
    uint64_t last_check_count{0};
  };

  class SentinelThread {
  public:
    SentinelThread(const char *name_str) : name(name_str) {}
    virtual ~SentinelThread() = default;
    int init(PSI_mutex_key &mutex_key, PSI_thread_key &thread_key);
    void cleanup();
    virtual void timeout_watcher() = 0;

  public:
    const char *name{""};
    mysql_mutex_t mutex;
    mysql_cond_t cond;
    my_thread_handle handler;
    std::atomic<bool> terminate{false};
  };

  class WriteableSentinel final : public SentinelThread {
  public:
    WriteableSentinel() : SentinelThread("disk_writeable_sent") {}
    virtual ~WriteableSentinel() = default;
    void timeout_watcher() override;
  };

  class HangSentinel final : public SentinelThread {
  public:
    HangSentinel() : SentinelThread("disk_hang_sent") {}
    virtual ~HangSentinel() = default;
    void timeout_watcher() override;
  };

private:
  std::atomic<bool> inited{false};

public:
  DiskItem data{"data_disk.sentinel"};
  DiskItem log{"log_disk.sentinel"};
  WriteableSentinel writeable_sentinel;
  HangSentinel hang_sentinel;
};

void *disk_sentinel_run(void *arg);
extern DiskSentinel global_disk_sentinel;

#endif /* DISK_SENTINEL_INCLUDE */
