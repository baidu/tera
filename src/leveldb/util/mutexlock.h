// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

/*
 * This class is designed for locking guard of a mutex. The right way to lock a critical region is
 * that using the constructor to construct an object in the stack, and the automatic unlock will be
 * done at the end of the critical region by the destructor.
 *
 * However, there are so many wrong uses in our code. We hope we can optimized these codes one by
 * one from now on. And we will use this class in the right way when we add new codes in the
 * future.
 *
 * Here, the Lock and Unlock member functions are designed just for the bad coded function
 * DBImpl::GetProperty(). They are NOT recommended to use at any other places.
 */
class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu), hold_mutex_(true) {
    mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() {
    if (hold_mutex_) {
      mu_->Unlock();
    }
  }

  void Lock() {
    hold_mutex_ = true;
    mu_->Lock();
  }
  void Unlock() {
    hold_mutex_ = false;
    mu_->Unlock();
  }

 private:
  port::Mutex *const mu_;
  bool hold_mutex_;
  // No copying allowed
  MutexLock(const MutexLock &);
  void operator=(const MutexLock &);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
