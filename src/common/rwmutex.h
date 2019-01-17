// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <pthread.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include "mutex.h"

namespace common {

class RWMutex {
 public:
  RWMutex() { PthreadCall("init mutex", pthread_rwlock_init(&mu_, nullptr)); }
  ~RWMutex() { PthreadCall("destroy mutex", pthread_rwlock_destroy(&mu_)); }

  void ReadLock() { PthreadCall("read lock", pthread_rwlock_rdlock(&mu_)); }

  void WriteLock() { PthreadCall("write lock", pthread_rwlock_wrlock(&mu_)); }

  void ReadUnlock() { PthreadCall("read unlock", pthread_rwlock_unlock(&mu_)); }

  void WriteUnlock() { PthreadCall("write unlock", pthread_rwlock_unlock(&mu_)); }

  void AssertHeld() {}

  RWMutex(const RWMutex &) = delete;

  RWMutex &operator=(const RWMutex &) = delete;

 private:
  pthread_rwlock_t mu_;
};

// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
class ReadLock {
 public:
  explicit ReadLock(RWMutex *mu) : mu_(mu) { this->mu_->ReadLock(); }

  ~ReadLock() { this->mu_->ReadUnlock(); }

  ReadLock(const ReadLock &) = delete;

  ReadLock &operator=(const ReadLock &) = delete;

 private:
  RWMutex *const mu_;
};

// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
class WriteLock {
 public:
  explicit WriteLock(RWMutex *mu) : mu_(mu) { this->mu_->WriteLock(); }

  ~WriteLock() { this->mu_->WriteUnlock(); }

  WriteLock(const WriteLock &) = delete;

  WriteLock &operator=(const WriteLock &) = delete;

 private:
  RWMutex *const mu_;
};

//
// Automatically unlock a locked mutex when the object is destroyed
//
class ReadUnlock {
 public:
  explicit ReadUnlock(RWMutex *mu) : mu_(mu) { mu->AssertHeld(); }

  ~ReadUnlock() { mu_->ReadUnlock(); }

  // No copying allowed
  ReadUnlock(const ReadUnlock &) = delete;

  ReadUnlock &operator=(const ReadUnlock &) = delete;

 private:
  RWMutex *const mu_;
};
}  // namespace common

using common::RWMutex;
using common::WriteLock;
using common::ReadLock;
using common::ReadUnlock;
