// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/thread.h"

namespace leveldb {

Thread::Thread()
    : started_(false) {}

Thread::~Thread() {
    Cancel();
    Join();
}

bool Thread::Start() {
    {
        MutexLock lock(&mutex_);
        if (!started_) {
            started_ = true;
        } else {
            return false;
        }
    }

    if (0 != pthread_create(&id_, NULL, StartRunner, this)) {
        started_ = false;
        return false;
    }

    return true;
}

void Thread::Join() {
    if (started_) {
        pthread_join(id_, NULL);
    }
}

void Thread::Cancel() {
    if (started_) {
        pthread_cancel(id_);
    }
}

void Thread::Stop() {
    MutexLock lock(&mutex_);
    started_ = false;
}

void* Thread::StartRunner(void* params) {
    Thread* runner = static_cast<Thread*>(params);
    runner->Run(params);
    runner->Stop();
    return NULL;
}

} // namespace leveldb
