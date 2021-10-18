/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2015, Red Hat, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// MirrorEnv is an Env implementation that mirrors all file-related
// operations to two backing Env's (provided at construction time).
// Writes are mirrored.  For read operations, we do the read from both
// backends and assert that the results match.
//
// This is useful when implementing a new Env and ensuring that the
// semantics and behavior are correct (in that they match that of an
// existing, stable Env, like the default POSIX one).

#pragma once

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <iostream>
#include <vector>
#include "xengine/env.h"

namespace xengine {
namespace util {

class SequentialFileMirror;
class RandomAccessFileMirror;
class WritableFileMirror;

class EnvMirror : public EnvWrapper {
  Env *a_, *b_;
  bool free_a_, free_b_;

 public:
  EnvMirror(Env* a, Env* b, bool free_a = false, bool free_b = false)
      : EnvWrapper(a), a_(a), b_(b), free_a_(free_a), free_b_(free_b) {}
  ~EnvMirror() {
    if (free_a_) delete a_;
    if (free_b_) delete b_;
  }

  common::Status NewSequentialFile(const std::string& f,
                                   SequentialFile *&r,
                                   const EnvOptions& options) override;
  common::Status NewRandomAccessFile(const std::string& f,
                                     RandomAccessFile *&r,
                                     const EnvOptions& options) override;
  common::Status NewWritableFile(const std::string& f,
                                 WritableFile *&r,
                                 const EnvOptions& options) override;
  common::Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   WritableFile *&r,
                                   const EnvOptions& options) override;
  virtual common::Status NewDirectory(const std::string& name,
                                      Directory *&result) override {
//    unique_ptr<Directory> br;
    Directory *br = nullptr;
    common::Status as = a_->NewDirectory(name, result);
    common::Status bs = b_->NewDirectory(name, br);
    assert(as == bs);
    return as;
  }
  common::Status FileExists(const std::string& f) override {
    common::Status as = a_->FileExists(f);
    common::Status bs = b_->FileExists(f);
    assert(as == bs);
    return as;
  }
  common::Status GetChildren(const std::string& dir,
                             std::vector<std::string>* r) override {
    std::vector<std::string> ar, br;
    common::Status as = a_->GetChildren(dir, &ar);
    common::Status bs = b_->GetChildren(dir, &br);
    assert(as == bs);
    std::sort(ar.begin(), ar.end());
    std::sort(br.begin(), br.end());
    if (!as.ok() || ar != br) {
      assert(0 == "getchildren results don't match");
    }
    *r = ar;
    return as;
  }
  common::Status DeleteFile(const std::string& f) override {
    common::Status as = a_->DeleteFile(f);
    common::Status bs = b_->DeleteFile(f);
    assert(as == bs);
    return as;
  }
  common::Status CreateDir(const std::string& d) override {
    common::Status as = a_->CreateDir(d);
    common::Status bs = b_->CreateDir(d);
    assert(as == bs);
    return as;
  }
  common::Status CreateDirIfMissing(const std::string& d) override {
    common::Status as = a_->CreateDirIfMissing(d);
    common::Status bs = b_->CreateDirIfMissing(d);
    assert(as == bs);
    return as;
  }
  common::Status DeleteDir(const std::string& d) override {
    common::Status as = a_->DeleteDir(d);
    common::Status bs = b_->DeleteDir(d);
    assert(as == bs);
    return as;
  }
  common::Status GetFileSize(const std::string& f, uint64_t* s) override {
    uint64_t asize, bsize;
    common::Status as = a_->GetFileSize(f, &asize);
    common::Status bs = b_->GetFileSize(f, &bsize);
    assert(as == bs);
    assert(!as.ok() || asize == bsize);
    *s = asize;
    return as;
  }

  common::Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override {
    uint64_t amtime, bmtime;
    common::Status as = a_->GetFileModificationTime(fname, &amtime);
    common::Status bs = b_->GetFileModificationTime(fname, &bmtime);
    assert(as == bs);
    assert(!as.ok() || amtime - bmtime < 10000 || bmtime - amtime < 10000);
    *file_mtime = amtime;
    return as;
  }

  common::Status RenameFile(const std::string& s,
                            const std::string& t) override {
    common::Status as = a_->RenameFile(s, t);
    common::Status bs = b_->RenameFile(s, t);
    assert(as == bs);
    return as;
  }

  common::Status LinkFile(const std::string& s, const std::string& t) override {
    common::Status as = a_->LinkFile(s, t);
    common::Status bs = b_->LinkFile(s, t);
    assert(as == bs);
    return as;
  }

  class FileLockMirror : public FileLock {
   public:
    FileLock *a_, *b_;
    FileLockMirror(FileLock* a, FileLock* b) : a_(a), b_(b) {}
  };

  common::Status LockFile(const std::string& f, FileLock** l) override {
    FileLock *al, *bl;
    common::Status as = a_->LockFile(f, &al);
    common::Status bs = b_->LockFile(f, &bl);
    assert(as == bs);
    if (as.ok()) *l = new FileLockMirror(al, bl);
    return as;
  }

  common::Status UnlockFile(FileLock* l) override {
    FileLockMirror* ml = static_cast<FileLockMirror*>(l);
    common::Status as = a_->UnlockFile(ml->a_);
    common::Status bs = b_->UnlockFile(ml->b_);
    assert(as == bs);
    delete ml;
    return as;
  }
};

}  // namespace util
}  // namespace xengine

#endif  // ROCKSDB_LITE
