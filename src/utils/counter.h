// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_UTILS_COUNTER_H_
#define  TERA_UTILS_COUNTER_H_

#include <stdio.h>

#include "atomic.h"
#include "timer.h"

namespace tera {

class Counter {
public:
    Counter() : val_(0) {}
    int64_t Add(int64_t v) {
        return atomic_add64(&val_, v) + v;
    }
    int64_t Sub(int64_t v) {
        return atomic_add64(&val_, -v) - v;
    }
    int64_t Inc() {
        return atomic_add64(&val_, 1) + 1;
    }
    int64_t Dec() {
        return atomic_add64(&val_, -1) - 1;
    }
    int64_t Get() {
        return val_;
    }
    int64_t Set(int64_t v) {
        return atomic_swap64(&val_, v);
    }
    int64_t Clear() {
        return atomic_swap64(&val_, 0);
    }

private:
    volatile int64_t val_;
};

class AutoCounter {
public:
    AutoCounter(Counter* counter, const char* msg1, const char* msg2 = NULL)
        : counter_(counter),
          msg1_(msg1),
          msg2_(msg2) {
        start_ = get_micros();
        counter_->Inc();
    }
    ~AutoCounter() {
        int64_t end = get_micros();
        if (end - start_ > 5000000) {
            int64_t t = (end - start_) / 1000000;
            if (!msg2_) {
                fprintf(stderr, "%s [AutoCounter] %s hang for %ld s\n",
                    get_curtime_str().data(), msg1_, t);
            } else {
                fprintf(stderr, "%s [AutoCounter] %s %s hang for %ld s\n",
                    get_curtime_str().data(), msg1_, msg2_, t);
            }
        }
        counter_->Dec();
    }

private:
    Counter* counter_;
    int64_t start_;
    const char* msg1_;
    const char* msg2_;
};
}

#endif  // TERA_UTILS_COUNTER_H_
