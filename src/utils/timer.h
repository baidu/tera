// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_UTILS_TIMER_H_
#define  TERA_UTILS_TIMER_H_

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

static inline std::string get_curtime_str_plain() {
    struct tm tt;
    char buf[20];
    time_t t = time(NULL);
    strftime(buf, 20, "%Y%m%d%H%M%S", localtime_r(&t, &tt));
    return std::string(buf);
}

static inline int64_t get_micros() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec) / 1000;
}

static inline int64_t get_millis() {
    return get_micros() / 1000;
}

static inline int64_t get_unique_micros(int64_t ref) {
    int64_t now;
    do {
        now = get_micros();
    } while (now == ref);
    return now;
}

static inline int64_t GetTimeStampInUs() {
    return get_micros();
}

static inline int64_t GetTimeStampInMs() {
    return get_millis();
}

}  // namespace tera

#endif  // TERA_UTILS_TIMER_H_
