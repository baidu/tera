// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_COMMON_TIMER_H_
#define  TERA_COMMON_TIMER_H_

#include <stdio.h>
#include <sys/time.h>

namespace common {
namespace timer {

static inline int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

class AutoTimer {
public:
    AutoTimer(int64_t timeout_ms, const char* msg1, const char* msg2 = NULL)
        : timeout_(timeout_ms),
          msg1_(msg1),
          msg2_(msg2) {
        start_ = get_micros();
    }
    ~AutoTimer() {
        int64_t end = get_micros();
        if (end - start_ > timeout_ * 1000) {
            double t = (end - start_) / 1000.0;
            if (!msg2_) {
                fprintf(stderr, "[AutoTimer] %s use %.3f ms\n",
                    msg1_, t);
            } else {
                fprintf(stderr, "[AutoTimer] %s %s use %.3f ms\n",
                    msg1_, msg2_, t);
            }
        }
    }

private:
    int64_t start_;
    int64_t timeout_;
    const char* msg1_;
    const char* msg2_;
};

}  // namespace timer
}  // namespace common

#endif  // TERA_COMMON_TIMER_H_
