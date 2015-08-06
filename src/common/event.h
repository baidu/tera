// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_COMMON_EVENT_H_
#define  TERA_COMMON_EVENT_H_

#include "mutex.h"

namespace common {

class AutoResetEvent {
public:
    AutoResetEvent()
      : cv_(&mutex_), signaled_(false) {
    }
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

} // namespace common

using common::AutoResetEvent;

#endif  // TERA_COMMON_EVENT_H_
