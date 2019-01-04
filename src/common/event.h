// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef TERA_COMMON_EVENT_H_
#define TERA_COMMON_EVENT_H_

#include "mutex.h"

namespace common {

class AutoResetEvent {
 public:
  AutoResetEvent() : cv_(&mutex_), signaled_(false) {}
  /// Wait for signal
  void Wait() {
    MutexLock lock(&mutex_);
    while (!signaled_) {
      cv_.Wait();
    }
    signaled_ = false;
  }
  bool TimeWait(int64_t timeout) {
    MutexLock lock(&mutex_);
    if (!signaled_) {
      cv_.TimeWait(timeout);
    }
    bool ret = signaled_;
    signaled_ = false;
    return ret;
  }
  /// Signal one
  void Set() {
    MutexLock lock(&mutex_);
    signaled_ = true;
    cv_.Signal();
  }

 private:
  Mutex mutex_;
  CondVar cv_;
  bool signaled_;
};

class CompletedEvent {
 public:
  CompletedEvent() : cv_(&mutex_), cnt_(0), triggered_(false) {}

  CompletedEvent(int64_t task_cnt) : cv_(&mutex_), cnt_(task_cnt), triggered_(false) {}

  // add event source,
  // tasks maybe add while others finished or doing, like a task queue
  void AddEventSources(int64_t task_cnt) {
    MutexLock lock(&mutex_);
    if (!triggered_) {
      cnt_ += task_cnt;
    }
  }

  // call after all tasks added to EventSource,
  // trigger other thread's Wait() function take effect.
  void Trigger() {
    MutexLock lock(&mutex_);
    triggered_ = true;
    if (cnt_ <= 0) {
      cv_.Signal();
    }
  }

  // wait until cnt_ == 0 and triggered_ == true
  void Wait() {
    MutexLock lock(&mutex_);
    // cnt_ > 0
    while (cnt_ > 0 || !triggered_) {
      cv_.Wait();
    }
  }

  // wait for 'timeout' ms, don't careful cnt_ and triggered_
  // if last event source completed, this will returned early 'timeout'
  bool TimeWait(int64_t timeout) {
    MutexLock lock(&mutex_);
    if (cnt_ > 0 || !triggered_) {
      cv_.TimeWait(timeout);
    }
    return cnt_ > 0 ? false : true;
  }

  // last event source complated and triggered_ == true, will be notify
  // Wait or TimeWait
  void Complete(int64_t task_cnt = 1) {
    MutexLock lock(&mutex_);
    cnt_ -= task_cnt;
    // use 'triggered_' to make sure all tasks call 'AddEventSources'
    if (cnt_ <= 0 && triggered_) {
      cv_.Signal();
    }
  }

 private:
  CompletedEvent(const CompletedEvent &) = delete;
  CompletedEvent &operator=(const CompletedEvent &) = delete;
  Mutex mutex_;
  CondVar cv_;
  int64_t cnt_;
  bool triggered_;
};

}  // namespace common

using common::AutoResetEvent;
using common::CompletedEvent;

#endif  // TERA_COMMON_EVENT_H_
