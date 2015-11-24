// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_COMMON_TIMER_H_
#define  TERA_COMMON_TIMER_H_

#include <stdio.h>
#include <sys/time.h>
#include <string>

namespace common {
namespace timer {

static inline std::string get_time_str(int64_t timestamp) {
    struct tm tt;
    char buf[20];
    time_t t = timestamp;
    strftime(buf, 20, "%Y%m%d-%H:%M:%S", localtime_r(&t, &tt));
    return std::string(buf, 17);
}

static inline std::string get_curtime_str() {
    return get_time_str(time(NULL));
}

static inline int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

static inline int64_t get_unique_micros(int64_t ref) {
    struct timeval tv;
    int64_t now;
    do {
        gettimeofday(&tv, NULL);
        now = static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    } while (now == ref);
    return now;
}

}  // namespace timer
}  // namespace common

#endif  // TERA_COMMON_TIMER_H_
