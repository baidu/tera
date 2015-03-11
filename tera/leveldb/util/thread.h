// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef LEVELDB_UTIL_THREAD_H_
#define LEVELDB_UTIL_THREAD_H_

#include <pthread.h>

#include "util/mutexlock.h"

namespace leveldb {

class Thread {
public:
    Thread();
    virtual ~Thread();

    bool Start();
    void Join();
    void Cancel();
    pthread_t Id() const;
    bool IsRunning() const;

    virtual void Run(void* params) = 0;

private:
    void Stop();
    static void* StartRunner(void* params);

private:
    bool started_;
    pthread_t id_;
    mutable port::Mutex mutex_;
};

inline pthread_t Thread::Id() const {
    return id_;
}

inline bool Thread::IsRunning() const {
    MutexLock lock(&mutex_);
    return started_;
}

} // namespace leveldb

#endif // LEVELDB_UTIL_THREAD_H_
