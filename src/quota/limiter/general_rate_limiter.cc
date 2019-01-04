// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "quota/limiter/general_rate_limiter.h"
#include "quota/helpers/quota_utils.h"
#include <algorithm>
#include <limits>
#include <glog/logging.h>
#include "common/timer.h"

namespace tera {
namespace quota {

GeneralRateLimiter::GeneralRateLimiter(const std::string& table_name, QuotaOperationType type)
    : quota_type_(QuotaUtils::GetQuotaOperation(type)),
      limit_per_sec_(quota_type_, LabelStringBuilder().Append("table", table_name).ToString(),
                     {SubscriberType::LATEST}, false),
      period_sec_(0),
      next_refill_ms_(0),
      table_name_(table_name) {}

void GeneralRateLimiter::Reset(int64_t limit, int64_t period_sec) {
  limit_.Set(limit);
  avail_.Set(limit);
  limit_per_sec_.Set(limit / period_sec);
  period_sec_ = period_sec;
  next_refill_ms_ = 0;

  VLOG(7) << "reset quota " << table_name_ << " " << quota_type_ << " " << limit_.Get() << "/"
          << period_sec_;
}

bool GeneralRateLimiter::RefillAndCheck(int64_t amount) {
  if (limit_.Get() < 0 || amount < 0) {
    VLOG(25) << "[" << quota_type_ << "] quota limit_[" << limit_.Get() << "] amount[" << amount
             << "] but let it pass";
    return true;
  }
  RefillAvail();
  if (amount > 0) {
    VLOG(7) << "[" << table_name_ << " " << quota_type_ << "] quota Avail:" << avail_.Get()
            << " RequestAmount:" << amount;
  }
  if (avail_.Get() < amount) {
    VLOG(25) << "[" << quota_type_ << "] quota reach limit";
    return false;
  }
  return true;
}

void GeneralRateLimiter::Consume(int64_t amount) {
  if (limit_.Get() < 0 || amount <= 0) {
    return;
  }
  if (amount >= avail_.Get()) {
    avail_.Clear();
  } else {
    avail_.Sub(amount);
  }
}

void GeneralRateLimiter::RefillAvail() {
  // refill limit after fixed interval (seconds)
  int64_t cur_ms = get_micros() / 1000;  // ms
  if (cur_ms < next_refill_ms_) {        // 1ms precision
    return;
  }
  next_refill_ms_ = cur_ms + period_sec_ * 1000;
  avail_.Set(limit_.Get());
}
}
}
