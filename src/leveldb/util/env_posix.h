// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <unistd.h>
#include "leveldb/env.h"

namespace leveldb {

namespace {
size_t GetLogicalBufferSize(int fd);
}

class PosixWritableFile : public WritableFile {
 private:
  // buf_[0, pos_-1] contains data to be written to fd_.
  std::string filename_;
  int fd_;
  size_t pos_;
  bool is_dio_;
  size_t buffer_size_;
  size_t align_size_;
  char* buf_ = NULL;

 public:
  PosixWritableFile(const std::string& fname, int fd, const EnvOptions& options);

  virtual ~PosixWritableFile();

  virtual Status Append(const Slice& data);

  virtual Status Close();

  virtual Status Flush();

  Status SyncDirIfManifest();

  virtual Status Sync();

  Status LeaveDio();

 private:
  Status FlushBuffered();

  Status WriteRaw(const char* p, size_t n);

  std::string GetFileName() const override { return filename_; }
};
}