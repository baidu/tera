// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_async_writer.h"

#include "leveldb/env.h"
#include "util/mutexlock.h"

namespace leveldb {
namespace log {

int AsyncWriter::block_log_number = 0;
port::Mutex AsyncWriter::log_mutex;

AsyncWriter::AsyncWriter(WritableFile* dest, bool async_mode)
    : async_mode_(async_mode),
      writer_(dest),
      dest_(dest),
      mode_(kNoAction),
      work_done_(&mutex_),
      can_work_(&mutex_),
      finished_(false),
      slice_data_(NULL),
      stop_(false),
      blocked_(false) {
  if (async_mode) {
    pthread_t ptid;
    int err = pthread_create(&ptid, NULL, ThreadFunc, static_cast<void*>(this));
    assert(err == 0);
    pthread_detach(ptid);
  }
}

AsyncWriter::~AsyncWriter() {
  if (blocked_) {
    MutexLock lock(&log_mutex);
    --block_log_number;
  }
}

void AsyncWriter::AddRecord(const Slice& slice) {
  if (!async_mode_) {
    s_ = writer_.AddRecord(slice);
    finished_ = true;
  } else {
    MutexLock lock(&mutex_);
    mode_ = AsyncWriter::kAddRecord;
    slice_data_ = new std::string(slice.data(), slice.size());
    finished_ = false;
    can_work_.Signal();
  }
}

void AsyncWriter::Sync(bool sync_or_flush) {
  if (!async_mode_) {
    if (sync_or_flush) {
      s_ = dest_->Sync();
    } else {
      s_ = dest_->Flush();
    }
    finished_ = true;
  } else {
    MutexLock lock(&mutex_);
    if (sync_or_flush) {
      mode_ = AsyncWriter::kSync;
    } else {
      mode_ = AsyncWriter::kFlush;
    }
    finished_ = false;
    can_work_.Signal();
  }
}

Status AsyncWriter::WaitDone(int32_t wait_sec) {
  MutexLock lock(&mutex_);
  if (wait_sec == 0) {
    if (!finished_) {
      return Status::TimeOut("time out: ");
    }
  }
  while (!finished_) {
    if (wait_sec < 0) {
      work_done_.Wait();
    } else {
      work_done_.Wait(wait_sec * 1000);
      if (!finished_) {
        return Status::TimeOut("time out: ");
      }
    }
  }
  return s_;
}

void AsyncWriter::Stop(bool is_blocked) {
  blocked_ = is_blocked;
  if (!async_mode_) {
    delete dest_;
    delete this;
  } else {
    MutexLock lock(&mutex_);
    stop_ = true;
    can_work_.Signal();
  }
}

void AsyncWriter::BlockLogNumInc() {
  MutexLock lock(&log_mutex);
  ++block_log_number;
}


void* AsyncWriter::ThreadFunc(void* arg) {
  static_cast<AsyncWriter*>(arg)->ThreadFuncCall();
  return NULL;
}

void AsyncWriter::ThreadFuncCall() {
  while (true) {
    MutexLock lock(&mutex_);
    while (mode_ == kNoAction && !stop_) {
      can_work_.Wait();
    }
    if (stop_) {
      break;
    }
    if (mode_ == kAddRecord) {
      mutex_.Unlock();
      s_ = writer_.AddRecord(*(slice_data_));
      mutex_.Lock();
      delete slice_data_;
      slice_data_ = NULL;
    } else if (mode_ == kSync) {
      mutex_.Unlock();
      s_ = dest_->Sync();
      mutex_.Lock();
    } else if (mode_ == kFlush) {
      mutex_.Unlock();
      s_ = dest_->Flush();
      mutex_.Lock();
    }
    finished_ = true;
    mode_ = kNoAction;
    work_done_.Signal();
  }
  delete dest_;
  delete this;
}

}  // namespace log
}  // namespace leveldb
