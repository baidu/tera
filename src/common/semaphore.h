// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include "mutex.h"

namespace common {

class Semaphore {
public:
    Semaphore(const Semaphore&) = delete;
    Semaphore& operator=(const Semaphore&) = delete;
    Semaphore(Semaphore&&) = delete;
    Semaphore& operator=(Semaphore&&) = delete;

    explicit Semaphore(int64_t counter)
      : cv_(&mutex_), counter_(counter) {
    }
    ~Semaphore() {}

    void Acquire() {
        MutexLock lock(&mutex_);
        while (counter_ <= 0) {
            cv_.Wait();
        }
        --counter_;
    }
    void Release() {
        MutexLock lock(&mutex_);
        ++counter_;
        cv_.Signal();
    }

private:
    Mutex mutex_;
    CondVar cv_;
    int64_t counter_;
};

} // namespace common
