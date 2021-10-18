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
#pragma once
#include <gmock/gmock.h>
#include "db/log_writer.h"
#include "env/io_posix.h"
#include "util/file_reader_writer.h"
namespace xengine {
namespace storage {
class ConcurrentDirectFileWriterMock : public util::ConcurrentDirectFileWriter {
 public:
  ConcurrentDirectFileWriterMock(std::unique_ptr<util::WritableFile> &&file,
                                 const util::EnvOptions &options)
      : ConcurrentDirectFileWriter(file.release(), options) {};

  MOCK_METHOD1(sync, common::Status(bool));
};

class WriterMock : public db::log::Writer {
 public:
  WriterMock(std::unique_ptr<storage::ConcurrentDirectFileWriterMock> &&writer)
      : db::log::Writer(writer.release(), 0, false, true) {};
  MOCK_METHOD1(AddRecord, common::Status(const common::Slice &));
};

class PosixSequentialFileMock : public util::PosixSequentialFile {
 public:
  PosixSequentialFileMock(const std::string &fname, FILE *file, int fd,
                          const util::EnvOptions &options)
      : PosixSequentialFile(fname, file, fd, options) {};
  MOCK_METHOD3(Read, common::Status(size_t, common::Slice *, char *));
  common::Status idle() { return util::IOError("idle", 0); }
};
}
}
