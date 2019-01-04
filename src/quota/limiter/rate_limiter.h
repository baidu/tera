// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

namespace tera {
namespace quota {

class RateLimiter {
 public:
  // Reset the limit and period_sec
  virtual void Reset(int64_t limit, int64_t period_sec) = 0;

  // If reach the period of quota, will reset the avail_ to limit_
  // then check user request amount greater then
  // avail_(means out of quota, return false) or not(retrun true).
  virtual bool RefillAndCheck(int64_t amount) = 0;

  // if io pass quota limiter, consume io amount
  virtual void Consume(int64_t amount) = 0;
};
}
}
