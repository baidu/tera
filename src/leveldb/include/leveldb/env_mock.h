// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LEVELDB_ENV_TEST_H_
#define TERA_LEVELDB_ENV_TEST_H_

#include <stdio.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>

#include "leveldb/dfs.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {

class MockEnv : public EnvWrapper {
 public:
  MockEnv();

  virtual ~MockEnv();
  void SetPrefix(const std::string& p);
  void ResetMock();

  void SetNewSequentialFileFailedCallback(bool (*p)(int32_t i, const std::string& fname));
  void SetSequentialFileReadCallback(bool (*p)(int32_t i, char* scratch, size_t* mock_size));
  virtual Status NewSequentialFile(const std::string& fname, SequentialFile** result);

  virtual Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result,
                                     const EnvOptions& options);

  virtual Status NewWritableFile(const std::string& fname, WritableFile** result,
                                 const EnvOptions& options);

  virtual Status FileExists(const std::string& fname);

  bool CheckDelete(const std::string& fname, std::vector<std::string>* flags);

  void SetGetChildrenCallback(bool (*p)(int32_t i, const std::string& fname));
  virtual Status GetChildren(const std::string& path, std::vector<std::string>* result);

  virtual Status DeleteFile(const std::string& fname);

  virtual Status CreateDir(const std::string& name);

  virtual Status DeleteDir(const std::string& name);

  virtual Status CopyFile(const std::string& from, const std::string& to);

  virtual Status GetFileSize(const std::string& fname, uint64_t* size);

  virtual Status RenameFile(const std::string& src, const std::string& target);

  virtual Status LockFile(const std::string& fname, FileLock** lock);

  virtual Status UnlockFile(FileLock* lock);

  virtual Env* CacheEnv() { return this; }

  static uint64_t gettid() {
    pid_t tid = syscall(SYS_gettid);
    return tid;
  }
};

Env* NewMockEnv();

}  // namespace leveldb

#endif  // TERA_LEVELDB_ENV_DFS_H_
