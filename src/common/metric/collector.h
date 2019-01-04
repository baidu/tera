#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <cstdint>
#include <memory>

namespace tera {
class Collector {
 public:
  virtual ~Collector() {}
  // return a instant value of the metric for tera to dump log and other usage
  virtual int64_t Collect() = 0;
};
}
