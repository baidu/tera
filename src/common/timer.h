#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdio.h>
#include <sys/time.h>
#include <string>
#include <cstring>

namespace tera {

static inline int64_t get_timestamp_from_str(const std::string& time) {
  struct tm tm;
  memset(&tm, 0, sizeof(tm));

  sscanf(time.c_str(), "%4d%2d%2d-%d:%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour,
         &tm.tm_min, &tm.tm_sec);

  tm.tm_year -= 1900;
  tm.tm_mon--;

  return mktime(&tm);
}

static inline std::string get_time_str(int64_t timestamp) {
  struct tm tt;
  char buf[20];
  time_t t = timestamp;
  strftime(buf, 20, "%Y%m%d-%H:%M:%S", localtime_r(&t, &tt));
  return std::string(buf, 17);
}

static inline std::string get_curtime_str() { return get_time_str(time(NULL)); }

static inline std::string get_curtime_str_plain() {
  struct tm tt;
  char buf[20];
  time_t t = time(NULL);
  strftime(buf, 20, "%Y%m%d%H%M%S", localtime_r(&t, &tt));
  return std::string(buf);
}

static inline int64_t get_micros() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec) / 1000;
}

static inline int64_t get_millis() { return get_micros() / 1000; }

static inline int64_t get_unique_micros(int64_t ref) {
  int64_t now;
  do {
    now = get_micros();
  } while (now == ref);
  return now;
}

static inline int64_t GetTimeStampInUs() { return get_micros(); }

static inline int64_t GetTimeStampInMs() { return get_millis(); }
}
