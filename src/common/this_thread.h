// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef TERA_COMMON_THIS_THREAD_H_
#define TERA_COMMON_THIS_THREAD_H_

#include <pthread.h>
#include <stdint.h>
#include <syscall.h>
#include <time.h>
#include <unistd.h>
#include <random>

namespace common {

class ThisThread {
 public:
  /// Sleep in ms
  static void Sleep(int64_t time_ms) {
    if (time_ms > 0) {
      timespec ts = {time_ms / 1000, (time_ms % 1000) * 1000000};
      nanosleep(&ts, &ts);
    }
  }
  /// Get thread id
  static int GetId() { return syscall(__NR_gettid); }

  /// Yield cpu
  static void Yield() { sched_yield(); }

  /// Thread-safe random generator
  template <class T>
  static T GetRandomValue(T min, T max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 gen(rd());
    std::uniform_int_distribution<T> dist(min, max);
    return dist(gen);
  }
};

}  // namespace common

using common::ThisThread;

#endif  // TERA_COMMON_THIS_THREAD_H_
