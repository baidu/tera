// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_ASYNC_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_ASYNC_WRITER_H_

#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include "db/log_format.h"
#include "db/log_writer.h"
#include "port/port_posix.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {

class AsyncWriter {
 public:
  enum Mode {
    kNoAction,
    kAddRecord,
    kSync
  };
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit AsyncWriter(WritableFile* dest, bool async_mode);
  ~AsyncWriter();

  // Does the same thing as AddRecord except that this
  // is a async function which calls TreadFuncAddRecor
  // to do the actual work
  void AddRecord(const Slice& slice);
  void Sync(bool sync_or_flush = true);

  // wait_sec can be:
  // positive (wait for wait_sec seconds),
  // or 0 (do not wait),
  // or negative (wait until the function returns)
  Status WaitDone(int32_t wait_sec);

  // Makes the async calls stop waiting and
  // automaticly delete this object.
  // 'is_blocked' indicates whether current writter
  // is blocked so the destructor knows whether it
  // should decrease 'block_log_number'
  void Stop(bool is_blocked);

  static void BlockLogNumInc();
  static int BlockLogNum() { return block_log_number; }

 private:
  static void* ThreadFunc(void* arg);
  void ThreadFuncCall();

  bool async_mode_;
  Writer writer_;
  WritableFile* dest_;
  int mode_;
  port::Mutex mutex_;
  port::CondVar work_done_;
  port::CondVar can_work_;
  bool finished_;
  std::string* slice_data_;
  bool stop_;
  bool blocked_;    // whether the current writter blocked
  Status s_;

  static int block_log_number;
  static port::Mutex log_mutex;

  // No copying allowed
  AsyncWriter(const AsyncWriter&);
  void operator=(const AsyncWriter&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_ASYNC_WRITER_H_
