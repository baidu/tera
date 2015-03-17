// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_THIS_THREAD_H_
#define  COMMON_THIS_THREAD_H_

#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <syscall.h>
#include <pthread.h>  

namespace common {

class ThisThread {
public:
    /// Sleep in ms
    static void Sleep(int64_t time_ms) {
        if (time_ms > 0) {
            timespec ts = {time_ms / 1000, (time_ms % 1000) * 1000000 };
            nanosleep(&ts, &ts);
        }
    }
    /// Get thread id
    static int GetId() {
        return syscall(__NR_gettid);
    }
    /// Yield cpu
    static void Yield() {
        sched_yield();
    }
};

} // namespace common

using common::ThisThread;

#endif  // COMMON_THIS_THREAD_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
