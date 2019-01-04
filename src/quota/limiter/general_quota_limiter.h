// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <map>
#include <memory>
#include "quota/limiter/quota_limiter.h"
#include "quota/limiter/rate_limiter.h"
#include "common/rwmutex.h"

namespace tera {
namespace quota {

using RateLimiterPtr = std::unique_ptr<RateLimiter>;

// Cause of copy shared_ptr<QuotaLimiter> for read,
// and new a Quotalimiter for write to swap,
// so CheckAndConsume will happened after Reset and
// won't have any conflict, DOESN'T NEED ANY synchronization primitive.
class GeneralQuotaLimiter : public QuotaLimiter {
 public:
  explicit GeneralQuotaLimiter(const std::string& table_name);
  virtual ~GeneralQuotaLimiter() {}

  void Reset(const TableQuota& table_quota) override;

  // if quota limited, return false
  // otherwise, consume the quota and return true
  bool CheckAndConsume(const Throttle& throttle) override;

 private:
  std::string table_name_;
  std::map<QuotaOperationType, RateLimiterPtr> op_rate_limiters_;
  RWMutex rw_mutex_;
};
}
}
