/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sstream>

#include "aio_wrapper.h"
#include "db/version_set.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "util/testharness.h"
#include "storage/storage_logger.h"

using namespace xengine::storage;
using namespace xengine::common;
using namespace xengine::db;

namespace xengine {
namespace util {

#define OK(expr) ASSERT_EQ(Status::kOk, (expr))

TEST(AIO, simple)
{
  // AIOReq test
  AIOInfo info;
  AIOReq req;
  // invalid aio info
  ASSERT_EQ(req.prepare_read(info), Status::kInvalidArgument);
  info.fd_ = 1000;
  ASSERT_EQ(req.prepare_read(info), Status::kInvalidArgument);
  info.fd_ = 0;
  info.size_ = 1;
  ASSERT_EQ(req.prepare_read(info), Status::kInvalidArgument);
  info.fd_ = 1000;
  info.size_ = 1;
  char *data = nullptr;
  ASSERT_EQ(req.prepare_write(info, data), Status::kInvalidArgument);

  // AIOBuffer test
  AIOBuffer aio_buf;
  // reentrant reset
  aio_buf.reset();
  aio_buf.reset();
  // read io buf
  ASSERT_EQ(aio_buf.prepare_read_io_buf(0, 0), Status::kInvalidArgument);
  OK(aio_buf.prepare_read_io_buf(0, 1));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  ASSERT_EQ(aio_buf.prepare_read_io_buf(0, 1), Status::kErrorUnexpected);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(0, DIO_ALIGN_SIZE - 1));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(0, DIO_ALIGN_SIZE));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(0, DIO_ALIGN_SIZE + 1));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE * 2);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(10, DIO_ALIGN_SIZE - 1));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE * 2);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(10, DIO_ALIGN_SIZE));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE * 2);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(10, DIO_ALIGN_SIZE + 1));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE * 2);
  ASSERT_EQ(aio_buf.aio_offset(), 0);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(DIO_ALIGN_SIZE, DIO_ALIGN_SIZE + 1));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE * 2);
  ASSERT_EQ(aio_buf.aio_offset(), DIO_ALIGN_SIZE);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  OK(aio_buf.prepare_read_io_buf(DIO_ALIGN_SIZE + 1, DIO_ALIGN_SIZE * 2));
  ASSERT_EQ(aio_buf.aio_size(), DIO_ALIGN_SIZE * 3);
  ASSERT_EQ(aio_buf.aio_offset(), DIO_ALIGN_SIZE);
  ASSERT_TRUE(uintptr_t(aio_buf.aio_buf()) % DIO_ALIGN_SIZE == 0);
  aio_buf.reset();

  // write io buf
  data = new char[5000];
  memset(data, 'a', DIO_ALIGN_SIZE);

  ASSERT_EQ(aio_buf.prepare_write_io_buf(0, DIO_ALIGN_SIZE - 1, data), Status::kInvalidArgument);
  ASSERT_EQ(aio_buf.prepare_write_io_buf(1, DIO_ALIGN_SIZE, data), Status::kInvalidArgument);
  OK(aio_buf.prepare_write_io_buf(0, DIO_ALIGN_SIZE, data));
  ASSERT_EQ(memcmp(aio_buf.aio_buf(), data, DIO_ALIGN_SIZE), 0);
  delete[] data;
}

TEST(AIO, singe_request)
{
  std::string tmpdir = test::TmpDir();
  std::string filename = tmpdir + "/singe_request";
  int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0600);
  ASSERT_GT(fd, -1);

  const int size = DIO_ALIGN_SIZE;
  int ret = fallocate(fd, 0, 0, size);
  ASSERT_EQ(ret, 0);

  char *wbuf = nullptr;
  ret = posix_memalign(reinterpret_cast<void **>(&wbuf), DIO_ALIGN_SIZE, size);
  ASSERT_EQ(ret, 0);
  memset(wbuf, 'a', size);

  // 1. init an AIO object
  AIO *aio = AIO::get_instance();

  // 2. init an AIOReq object, don't free it until it's done/reaped
  void *aio_data = reinterpret_cast<void *>(&filename);
  AIOReq wreq;
  AIOInfo w_info(fd, 0, size);
  OK(wreq.prepare_write(w_info, wbuf));

  // 3. optionally we can attach more information to aio_data,
  //    it can be checked again on i/o completion
  wreq.aio_data_ = aio_data;

  // 4. then submit the request to aio
  OK(aio->submit(&wreq, 1));

  // 5. wait until this specific request done, or if we just want to wait
  //    for completion of any i/o in order to free the resources, like
  //    libaio's io_getevents, use reap_some instead
  OK(aio->reap(&wreq));
  ASSERT_EQ(wreq.status_, Status::kOk);
  ASSERT_EQ(wreq.aio_data_, reinterpret_cast<void *>(&filename));

  AIOReq rreq;
  // empty req
  ASSERT_EQ(aio->submit(&rreq, 1), Status::kInvalidArgument);
  // normal
  AIOInfo r_info(fd, 0, size);
  OK(rreq.prepare_read(r_info));

  OK(aio->submit(&rreq, 1));
  OK(aio->reap(&rreq));
  ASSERT_EQ(rreq.status_, Status::kOk);
  ASSERT_EQ(memcmp(wbuf, rreq.aio_buf_.data(), size), 0);

  free(wbuf);
  unlink(filename.c_str());
}

void prepare_file(int fd, const int page_cnt, char *buf)
{
  const int size = DIO_ALIGN_SIZE * page_cnt;
  ASSERT_EQ(fallocate(fd, 0, 0, size), 0);

  // prepare requests
  const char c = 'a';
  AIOReq wreqs[page_cnt];
  for (int i = 0; i < page_cnt; i++) {
    char *wbuf = buf + i * DIO_ALIGN_SIZE;
    memset(wbuf, c + i, DIO_ALIGN_SIZE);
    AIOInfo info(fd, i * DIO_ALIGN_SIZE /*offset*/, DIO_ALIGN_SIZE /*size*/);
    OK(wreqs[i].prepare_write(info, wbuf));
  }

  AIO *aio = AIO::get_instance();
  OK(aio->submit(wreqs, page_cnt));

  int total_reap_num = 0;
  struct AIOReq **reap_reqs = new AIOReq *[page_cnt];
  while (total_reap_num < page_cnt) {
    int reap_num = page_cnt;
    OK(aio->reap_some(reap_num, reap_reqs));
    for (int i = 0; i < reap_num; i++) {
      OK(reap_reqs[i]->status_);
    }
    total_reap_num += reap_num;
  }
  ASSERT_EQ(total_reap_num, page_cnt);
  delete[] reap_reqs;
}

TEST(AIO, multi_request_reap) {
  std::string tmpdir = test::TmpDir();
  std::string filename = tmpdir + "/aio_reap";
  int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0600);
  ASSERT_GT(fd, -1);

  const int page_cnt = 10;
  char *wbuf = nullptr;
  ASSERT_EQ(posix_memalign(reinterpret_cast<void **>(&wbuf), DIO_ALIGN_SIZE, DIO_ALIGN_SIZE * page_cnt), 0);

  prepare_file(fd, page_cnt, wbuf);

  AIOReq reqs[page_cnt];
  AIOInfo infos[page_cnt];

  AIO *aio = AIO::get_instance();

  infos[0].offset_ = 0;
  infos[0].size_ = DIO_ALIGN_SIZE;

  infos[1].offset_ = 0;
  infos[1].size_ = DIO_ALIGN_SIZE - 1;

  infos[2].offset_ = 0;
  infos[2].size_ = DIO_ALIGN_SIZE + 1;

  infos[3].offset_ = DIO_ALIGN_SIZE - 1;
  infos[3].size_ = DIO_ALIGN_SIZE;

  infos[4].offset_ = DIO_ALIGN_SIZE - 1;
  infos[4].size_ = DIO_ALIGN_SIZE * 2;

  infos[5].offset_ = DIO_ALIGN_SIZE;
  infos[5].size_ = DIO_ALIGN_SIZE * 2;

  infos[6].offset_ = DIO_ALIGN_SIZE + 1;
  infos[6].size_ = DIO_ALIGN_SIZE * 2;

  infos[7].offset_ = DIO_ALIGN_SIZE;
  infos[7].size_ = DIO_ALIGN_SIZE * 3 + 1;

  infos[8].offset_ = DIO_ALIGN_SIZE;
  infos[8].size_ = DIO_ALIGN_SIZE * 3 - 1;

  infos[9].offset_ = DIO_ALIGN_SIZE * 3 + 1;
  infos[9].size_ = DIO_ALIGN_SIZE * 3 - 1;

  for (int i = 0; i < page_cnt; i++) {
    infos[i].fd_ = fd;
    OK(reqs[i].prepare_read(infos[i]));
  }

  OK(aio->submit(reqs, page_cnt));

  for (int i = page_cnt - 1; i >= 0; i--) {
    OK(aio->reap(&reqs[i]));
    OK(reqs[i].status_) << i;
    ASSERT_EQ(memcmp(wbuf + infos[i].offset_, reqs[i].aio_buf_.data(), infos[i].size_) , 0) << i;
  }

  free(wbuf);
  unlink(filename.c_str());
}

TEST(AIO, multi_request_reap_some) {
  std::string tmpdir = test::TmpDir();
  std::string filename = tmpdir + "/aio_reap_some";
  int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0600);
  ASSERT_GT(fd, -1);

  const int page_cnt = 10;
  char *wbuf = nullptr;
  ASSERT_EQ(posix_memalign(reinterpret_cast<void **>(&wbuf), DIO_ALIGN_SIZE, DIO_ALIGN_SIZE * page_cnt), 0);

  prepare_file(fd, page_cnt, wbuf);

  AIOReq reqs[page_cnt];
  AIOInfo infos[page_cnt];

  AIO *aio = AIO::get_instance();

  infos[0].offset_ = 0;
  infos[0].size_ = DIO_ALIGN_SIZE;

  infos[1].offset_ = 0;
  infos[1].size_ = DIO_ALIGN_SIZE - 1;

  infos[2].offset_ = 0;
  infos[2].size_ = DIO_ALIGN_SIZE + 1;

  infos[3].offset_ = DIO_ALIGN_SIZE - 1;
  infos[3].size_ = DIO_ALIGN_SIZE;

  infos[4].offset_ = DIO_ALIGN_SIZE - 1;
  infos[4].size_ = DIO_ALIGN_SIZE * 2;

  infos[5].offset_ = DIO_ALIGN_SIZE;
  infos[5].size_ = DIO_ALIGN_SIZE * 2;

  infos[6].offset_ = DIO_ALIGN_SIZE + 1;
  infos[6].size_ = DIO_ALIGN_SIZE * 2;

  infos[7].offset_ = DIO_ALIGN_SIZE;
  infos[7].size_ = DIO_ALIGN_SIZE * 3 + 1;

  infos[8].offset_ = DIO_ALIGN_SIZE;
  infos[8].size_ = DIO_ALIGN_SIZE * 3 - 1;

  infos[9].offset_ = DIO_ALIGN_SIZE * 3 + 1;
  infos[9].size_ = DIO_ALIGN_SIZE * 3 - 1;

  for (int i = 0; i < page_cnt; i++) {
    infos[i].fd_ = fd;
    OK(reqs[i].prepare_read(infos[i]));
  }

  OK(aio->submit(reqs, page_cnt));

  int total_reap_num = 0;
  struct AIOReq **reap_reqs = new AIOReq *[page_cnt];
  while (total_reap_num < page_cnt) {
    int reap_num = page_cnt;
    OK(aio->reap_some(reap_num, reap_reqs));
    for (int i = 0; i < reap_num; i++) {
      OK(reap_reqs[i]->status_);
    }
    total_reap_num += reap_num;
  }
  ASSERT_EQ(total_reap_num, page_cnt);

  for (int i = 0; i < page_cnt; i++) {
    ASSERT_EQ(memcmp(wbuf + infos[i].offset_, reqs[i].aio_buf_.data(), infos[i].size_) , 0) << i;
  }

  free(wbuf);
  unlink(filename.c_str());
  delete[] reap_reqs;
}

// TODO: need to reopen
TEST(AIO, DISABLED_extent) {
  std::string dbname = test::TmpDir() + "/aio_test";
  DBOptions options;
  options.env->CreateDir(dbname);
  StorageLogger storage_logger;
  options.db_paths.emplace_back(dbname, 0);
  unique_ptr<storage::ExtentSpaceManager> space_manager;
  FileNumber file_number(2000);
  space_manager.reset(new storage::ExtentSpaceManager(options.env, EnvOptions(options), options));

  Status s = space_manager->init(&storage_logger);
  EXPECT_TRUE(s.ok()) << s.ToString();
  WritableExtent we;
  s = space_manager->allocate(0, storage::HOT_EXTENT_SPACE, we);
  EXPECT_TRUE(s.ok()) << s.ToString();

  const int size = MAX_EXTENT_SIZE;
  char *wbuf = nullptr;
  int ret = posix_memalign(reinterpret_cast<void **>(&wbuf), DIO_ALIGN_SIZE, size);
  ASSERT_EQ(ret, 0);
  memset(wbuf, 'b', size);

  AIO *aio = AIO::get_instance();

  // write it out
  AIOReq wreq;
  // only extent has the real information of fd/offset
  s = we.AsyncAppend(*aio, &wreq, Slice(wbuf, size));
  EXPECT_TRUE(s.ok()) << s.ToString();
  ret = aio->reap(&wreq);
  ASSERT_EQ(ret, Status::kOk);

  // read it back
  std::unique_ptr<storage::RandomAccessExtent> extent(new storage::RandomAccessExtent());
  ASSERT_TRUE(extent.get());
  ExtentId eid(we.get_extent_id());
  AsyncRandomAccessExtent re;
  s = space_manager->get_random_access_extent(eid, re);
  EXPECT_TRUE(s.ok()) << s.ToString();

  re.prefetch();

  Slice rbuf;
  s = re.Read(0, size, &rbuf, nullptr);
  EXPECT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(ret, Status::kOk);

  // compare
  ASSERT_EQ(memcmp(wbuf, rbuf.data(), size), 0);

  //options.env->stop();

  std::vector<std::string> filenames;
  options.env->GetChildren(dbname, &filenames);
  for (const auto &file : filenames) {
    options.env->DeleteFile(dbname + "/" + file);
  }
  options.env->DeleteDir(dbname);
}

}  // namespace util
}  // namespace xengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
