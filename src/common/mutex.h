// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com
#pragma once

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <cassert>
#include "common/timer.h"

namespace common {

// #define MUTEX_DEBUG

static void PthreadCall(const char *label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

// A Mutex represents an exclusive lock.
class Mutex {
 public:
  Mutex() : owner_(0), msg_(NULL), msg_threshold_(0), lock_time_(0) {
    pthread_mutexattr_t attr;
    PthreadCall("init mutexattr", pthread_mutexattr_init(&attr));
    PthreadCall("set mutexattr", pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK));
    PthreadCall("init mutex", pthread_mutex_init(&mu_, &attr));
    PthreadCall("destroy mutexattr", pthread_mutexattr_destroy(&attr));
  }
  ~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }
  // Lock the mutex.
  // Will deadlock if the mutex is already locked by this thread.
  void Lock(const char *msg = NULL, int64_t msg_threshold = 100) {
#ifdef MUTEX_DEBUG
    int64_t s = 0;
    if (msg) {
      s = get_micros();
    }
#endif
    PthreadCall("mutex lock", pthread_mutex_lock(&mu_));
    AfterLock(msg, msg_threshold);
#ifdef MUTEX_DEBUG_
    if (msg && lock_time_ - s > msg_threshold) {
      printf("%s wait lock %.3f ms\n", msg, (lock_time_ - s) / 1000.0);
    }
#endif
  }
  // Unlock the mutex.
  void Unlock() {
    BeforeUnlock();
    PthreadCall("mutex unlock", pthread_mutex_unlock(&mu_));
  }
  // Crash if this thread does not hold this mutex.
  void AssertHeld() {
    if (0 == pthread_equal(owner_, pthread_self())) {
      abort();
    }
  }

 private:
  void AfterLock(const char *msg, int64_t msg_threshold) {
#ifdef MUTEX_DEBUG
    msg_ = msg;
    msg_threshold_ = msg_threshold;
    if (msg_) {
      lock_time_ = get_micros();
    }
#endif
    owner_ = pthread_self();
  }
  void BeforeUnlock() {
#ifdef MUTEX_DEBUG
    if (msg_ && get_micros() - lock_time_ > msg_threshold_) {
      printf("%s locked %.3f ms\n", msg_, (get_micros() - lock_time_) / 1000.0);
    }
    msg_ = NULL;
#endif
    owner_ = 0;
  }

 private:
  friend class CondVar;
  Mutex(const Mutex &);
  void operator=(const Mutex &);
  pthread_mutex_t mu_;
  pthread_t owner_;
  const char *msg_;
  int64_t msg_threshold_;
  int64_t lock_time_;
};

// Mutex lock guard
class MutexLock {
 public:
  explicit MutexLock(Mutex *mu, const char *msg = NULL) : mu_(mu) { mu_->Lock(msg); }
  ~MutexLock() { mu_->Unlock(); }

 private:
  Mutex *const mu_;
  MutexLock(const MutexLock &);
  void operator=(const MutexLock &);
};

// Conditional variable
class CondVar {
 public:
  explicit CondVar(Mutex *mu) : mu_(mu) {
    // use monotonic clock
    PthreadCall("condattr init ", pthread_condattr_init(&attr_));
    PthreadCall("condattr setclock ", pthread_condattr_setclock(&attr_, CLOCK_MONOTONIC));
    PthreadCall("condvar init with attr", pthread_cond_init(&cond_, &attr_));
  }
  ~CondVar() {
    PthreadCall("condvar destroy", pthread_cond_destroy(&cond_));
    PthreadCall("condattr destroy", pthread_condattr_destroy(&attr_));
  }
  void Wait(const char *msg = NULL) {
    int64_t msg_threshold = mu_->msg_threshold_;
    mu_->BeforeUnlock();
    PthreadCall("condvar wait", pthread_cond_wait(&cond_, &mu_->mu_));
    mu_->AfterLock(msg, msg_threshold);
  }
  // Time wait in us
  // timeout < 0 would cause ETIMEOUT and return false immediately
  bool TimeWaitInUs(int64_t timeout, const char *msg = NULL) {
    // ref:
    // http://www.qnx.com/developers/docs/6.5.0SP1.update/com.qnx.doc.neutrino_lib_ref/p/pthread_cond_timedwait.html
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t nsec = timeout * 1000 + ts.tv_nsec;

    assert(nsec > 0);

    ts.tv_sec += nsec / 1000000000;
    ts.tv_nsec = nsec % 1000000000;

    int64_t msg_threshold = mu_->msg_threshold_;
    mu_->BeforeUnlock();
    int ret = pthread_cond_timedwait(&cond_, &mu_->mu_, &ts);
    mu_->AfterLock(msg, msg_threshold);
    return (ret == 0);
  }
  // Time wait in ms
  // timeout < 0 would cause ETIMEOUT and return false immediately
  bool TimeWait(int timeout, const char *msg = NULL) { return TimeWaitInUs(timeout * 1000LL, msg); }
  void Signal() { PthreadCall("signal", pthread_cond_signal(&cond_)); }
  void Broadcast() { PthreadCall("broadcast", pthread_cond_broadcast(&cond_)); }

 private:
  CondVar(const CondVar &);
  void operator=(const CondVar &);
  Mutex *mu_;
  pthread_cond_t cond_;
  pthread_condattr_t attr_;
};

}  // namespace common

using common::Mutex;
using common::MutexLock;
using common::CondVar;
