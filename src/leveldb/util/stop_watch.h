// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once
#include "leveldb/env.h"

namespace leveldb {
// Auto-scoped.
// Records the measure time into the corresponding histogram if statistics
// is not nullptr. It is also saved into *elapsed if the pointer is not nullptr
// and overwrite is true, it will be added to *elapsed if overwrite is false.

// a nano second precision stopwatch
class StopWatchMicro {
 public:
  explicit StopWatchMicro(Env* const env, bool auto_start = false) : env_(env), start_(0) {
    if (auto_start) {
      Start();
    }
  }

  void Start() { start_ = env_->NowMicros(); }

  uint64_t ElapsedMicros(bool reset = false) {
    auto now = env_->NowMicros();
    auto elapsed = now - start_;
    if (reset) {
      start_ = now;
    }
    return elapsed;
  }

  uint64_t ElapsedMicrosSafe(bool reset = false) {
    return (env_ != nullptr) ? ElapsedMicros(reset) : 0U;
  }

 private:
  Env* const env_;
  uint64_t start_;
};

}  // namespace leveldb
