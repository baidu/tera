// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <cstdint>
#include "quota/limiter/rate_limiter.h"
#include "common/counter.h"
#include "common/metric/metric_counter.h"
#include "proto/quota.pb.h"

namespace tera {
namespace quota {

// thread safe, only int64_t type value change. Doesn't need lock
class GeneralRateLimiter : public RateLimiter {
 public:
  explicit GeneralRateLimiter(const std::string& table_name, QuotaOperationType type);
  virtual ~GeneralRateLimiter() {}

  void Reset(int64_t limit, int64_t period_sec) override;

  // If reach the period of quota, will reset the avail_ to limit_
  // then check user request amount greater then
  // avail_(means out of quota, return false) or not(retrun true).
  bool RefillAndCheck(int64_t amount) override;

  void Consume(int64_t amount) override;

 private:
  void RefillAvail();

 private:
  std::string quota_type_;
  Counter limit_;
  Counter avail_;
  tera::MetricCounter limit_per_sec_;
  int64_t period_sec_;
  int64_t next_refill_ms_;
  std::string table_name_;
};
}
}
