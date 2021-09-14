//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "util/file_util.h"

#include <algorithm>
#include <string>

#include "util/file_reader_writer.h"
#include "util/sst_file_manager_impl.h"
#include "xengine/env.h"

using namespace xengine::common;

namespace xengine {
namespace util {
static const uint64_t PAGE_SIZE = 4096;
// Utility function to copy a file up to a specified length
Status CopyFile(Env* env, const std::string& source,
                const std::string& destination, 
                uint64_t size, bool use_fsync, uint64_t offset) {
  const EnvOptions soptions;
  Status s;
  unique_ptr<SequentialFile, memory::ptr_destruct_delete<SequentialFile>> srcfile_ptr;
  unique_ptr<SequentialFileReader, memory::ptr_destruct_delete<SequentialFileReader>> src_reader;
  unique_ptr<WritableFileWriter, memory::ptr_destruct_delete<WritableFileWriter>> dest_writer;
  unique_ptr<WritableFile, memory::ptr_destruct_delete<WritableFile>> destfile_ptr;

  {
    SequentialFile *srcfile = nullptr;
    s = env->NewSequentialFile(source, srcfile, soptions);
    srcfile_ptr.reset(srcfile);
    WritableFile *destfile = nullptr;
    if (s.ok()) {
      s = env->NewWritableFile(destination, destfile, soptions);
      destfile_ptr.reset(destfile);
    } else {
      return s;
    }

    src_reader.reset(MOD_NEW_OBJECT(memory::ModId::kDefaultMod, SequentialFileReader, srcfile_ptr.release()));
    dest_writer.reset(MOD_NEW_OBJECT(memory::ModId::kDefaultMod, WritableFileWriter, destfile_ptr.release(), soptions));
  }

  char buffer[4096];
  Slice slice;
  if (offset > 0) { // read from here
    src_reader->Skip(offset);
  }
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    if (s.ok()) {
      s = src_reader->Read(bytes_to_read, &slice, buffer);
    }
    if (s.ok()) {
      if (slice.size() == 0) {
        return Status::Corruption("file too small");
      }
      s = dest_writer->Append(slice);
    }
    if (!s.ok()) {
      return s;
    }
    size -= slice.size();
  }
  dest_writer->Sync(use_fsync);
  return Status::OK();
}

// Utility function to copy a file up to a specified length
int  append_file(Env* env, const std::string& source,
                const std::string& destination, 
                uint64_t size, uint64_t offset) {
  EnvOptions soptions;
  Status s;
  unique_ptr<SequentialFileReader, memory::ptr_destruct_delete<SequentialFileReader>> src_reader;
  unique_ptr<SequentialFile, memory::ptr_destruct_delete<SequentialFile>> srcfile_ptr;
  WritableFile  *destfile = nullptr;
  {
    SequentialFile *srcfile = nullptr;
    s = env->NewSequentialFile(source, srcfile, soptions);
    srcfile_ptr.reset(srcfile);
    if (s.ok()) {
      soptions.use_direct_writes = true;
      s = env->NewWritableFile(destination, destfile, soptions);
      if (!s.ok()) {
        fprintf(stderr, "Create the file writer for append file failed");
        return s.code();
      }
    } else {
      return s.code();
    }

    if (size == 0) {
      // default argument means copy everything
      if (s.ok()) {
        s = env->GetFileSize(source, &size);
      } else {
        return s.code();
      }
    }
//    src_reader.reset(new SequentialFileReader(srcfile));
    src_reader.reset(MOD_NEW_OBJECT(memory::ModId::kEnv, SequentialFileReader, srcfile));
  }

  char *ptr = nullptr;
  if (posix_memalign(reinterpret_cast<void **>(&ptr), PAGE_SIZE, PAGE_SIZE) !=
      0) {
    fprintf(stderr, "Malloc aligned buffer for append file failed");
    return Status::kMemoryLimit;
  } 
  std::unique_ptr<char> buffer(ptr);
  Slice slice;
  while (size > 0) {
    size_t bytes_to_read = std::min(PAGE_SIZE, static_cast<size_t>(size));
    if (s.ok()) {
      s = src_reader->Read(bytes_to_read, &slice, buffer.get());
    }
    if (s.ok()) {
      if (slice.size() == 0) {
        fprintf(stderr, "The file is too small\n");
        return Status::kCorruption;
      }
      s = destfile->PositionedAppend(slice, offset);
    }
    if (!s.ok()) {
      return s.code();
    }
    size -= slice.size();
    offset += slice.size();
  }
  destfile->Sync();
  MOD_DELETE_OBJECT(WritableFile, destfile);
  return Status::kOk;
}

// Utility function to create a file with the provided contents
Status CreateFile(Env* env, const std::string& destination,
                  const std::string& contents) {
  const EnvOptions soptions;
  Status s;
  unique_ptr<WritableFileWriter, memory::ptr_destruct_delete<WritableFileWriter>> dest_writer;
  unique_ptr<WritableFile, memory::ptr_destruct_delete<WritableFile>> destfile_ptr;

  WritableFile *destfile = nullptr;
  s = env->NewWritableFile(destination, destfile, soptions);
  destfile_ptr.reset(destfile);
  if (!s.ok()) {
    return s;
  }
//  dest_writer.reset(new WritableFileWriter(destfile, soptions));
  dest_writer.reset(MOD_NEW_OBJECT(memory::ModId::kEnv, WritableFileWriter, destfile, soptions));
  return dest_writer->Append(Slice(contents));
}

Status DeleteSSTFile(const ImmutableDBOptions* db_options,
                     const std::string& fname, uint32_t path_id) {
// TODO(tec): support sst_file_manager for multiple path_ids
#ifndef ROCKSDB_LITE
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options->sst_file_manager.get());
  if (sfm && path_id == 0) {
    return sfm->ScheduleFileDeletion(fname);
  } else {
    return db_options->env->DeleteFile(fname);
  }
#else
  // SstFileManager is not supported in ROCKSDB_LITE
  return db_options->env->DeleteFile(fname);
#endif
}

}  // namespace util
}  // namespace xengine
