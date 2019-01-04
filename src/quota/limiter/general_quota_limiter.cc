// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "quota/limiter/general_quota_limiter.h"
#include <limits>
#include "quota/limiter/general_rate_limiter.h"

namespace tera {
namespace quota {

namespace {
static const int64_t unlimited_quota = -1;
static const int64_t period_one_sec = 1;
}

GeneralQuotaLimiter::GeneralQuotaLimiter(const std::string& table_name) : table_name_(table_name) {
  op_rate_limiters_[kQuotaWriteReqs].reset(new GeneralRateLimiter(table_name_, kQuotaWriteReqs));
  op_rate_limiters_[kQuotaWriteBytes].reset(new GeneralRateLimiter(table_name_, kQuotaWriteBytes));
  op_rate_limiters_[kQuotaReadReqs].reset(new GeneralRateLimiter(table_name_, kQuotaReadReqs));
  op_rate_limiters_[kQuotaReadBytes].reset(new GeneralRateLimiter(table_name_, kQuotaReadBytes));
  op_rate_limiters_[kQuotaScanReqs].reset(new GeneralRateLimiter(table_name_, kQuotaScanReqs));
  op_rate_limiters_[kQuotaScanBytes].reset(new GeneralRateLimiter(table_name_, kQuotaScanBytes));
}

void GeneralQuotaLimiter::Reset(const TableQuota& table_quota) {
  WriteLock l(&rw_mutex_);
  for (auto& op_rate_limiter : op_rate_limiters_) {
    op_rate_limiter.second->Reset(unlimited_quota, period_one_sec);
  }
  for (int i = 0; i < table_quota.quota_infos_size(); ++i) {
    int64_t limit = table_quota.quota_infos(i).limit();
    int64_t period = table_quota.quota_infos(i).period();
    QuotaOperationType type = table_quota.quota_infos(i).type();
    op_rate_limiters_[type]->Reset(limit, period);
  }
}

bool GeneralQuotaLimiter::CheckAndConsume(const Throttle& throttle) {
  ReadLock l(&rw_mutex_);
  if (!op_rate_limiters_[kQuotaWriteReqs]->RefillAndCheck(throttle.write_reqs) ||
      !op_rate_limiters_[kQuotaWriteBytes]->RefillAndCheck(throttle.write_bytes) ||
      !op_rate_limiters_[kQuotaReadReqs]->RefillAndCheck(throttle.read_reqs) ||
      !op_rate_limiters_[kQuotaReadBytes]->RefillAndCheck(throttle.read_bytes) ||
      !op_rate_limiters_[kQuotaScanReqs]->RefillAndCheck(throttle.scan_reqs) ||
      !op_rate_limiters_[kQuotaScanBytes]->RefillAndCheck(throttle.scan_bytes)) {
    return false;
  }
  op_rate_limiters_[kQuotaWriteReqs]->Consume(throttle.write_reqs);
  op_rate_limiters_[kQuotaWriteBytes]->Consume(throttle.write_bytes);
  op_rate_limiters_[kQuotaReadReqs]->Consume(throttle.read_reqs);
  op_rate_limiters_[kQuotaReadBytes]->Consume(throttle.read_bytes);
  op_rate_limiters_[kQuotaScanReqs]->Consume(throttle.scan_reqs);
  op_rate_limiters_[kQuotaScanBytes]->Consume(throttle.scan_bytes);
  return true;
}
}
}
