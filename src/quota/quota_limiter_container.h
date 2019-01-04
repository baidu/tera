// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <memory>
#include <unordered_map>
#include "common/rwmutex.h"
#include "proto/quota.pb.h"
#include "quota/limiter/quota_limiter.h"

namespace tera {
namespace quota {

using QuotaLimiterPtr = std::shared_ptr<QuotaLimiter>;

class QuotaLimiterContainer {
 public:
  explicit QuotaLimiterContainer() {}
  virtual ~QuotaLimiterContainer() {}

  bool GetTableLimiter(const std::string& table_name, QuotaLimiterPtr* limiter) const;

  bool ResetQuota(const TableQuota& table_quota);

 private:
  std::unordered_map<std::string, QuotaLimiterPtr> table_quotas_;
  mutable RWMutex rw_mutex_;
};
}
}
