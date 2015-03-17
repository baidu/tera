// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  __TERA_TIMER_H_
#define  __TERA_TIMER_H_

#include <sys/time.h>
#include <string>

namespace tera {

static inline std::string get_curtime_str() {
    struct tm tt;
    char buf[20];
    time_t t = time(NULL);
    strftime(buf, 20, "%Y%m%d-%H:%M:%S", localtime_r(&t, &tt));
    return std::string(buf, 17);
}

static inline long get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<long>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

static inline long get_unique_micros(long ref) {
    struct timeval tv;
    long now;
    do {
        gettimeofday(&tv, NULL);
        now = static_cast<long>(tv.tv_sec) * 1000000 + tv.tv_usec;
    } while (now == ref);
    return now;
}

static inline long GetTimeStampInUs() {
    return get_micros();
}

static inline long GetTimeStampInMs() {
    return get_micros() / 1000;
}

}

#endif  //__TERA_TIMER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
