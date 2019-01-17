// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "quota/quota_limiter_container.h"
#include "quota/limiter/limiter_factory.h"
#include <glog/logging.h>

DECLARE_string(tera_quota_limiter_type);

namespace tera {
namespace quota {

bool QuotaLimiterContainer::GetTableLimiter(const std::string& table_name,
                                            QuotaLimiterPtr* limiter) const {
  ReadLock l(&rw_mutex_);
  auto it = table_quotas_.find(table_name);
  if (it != table_quotas_.end()) {
    *limiter = it->second;
    return true;
  }
  return false;
}

bool QuotaLimiterContainer::ResetQuota(const TableQuota& table_quota) {
  WriteLock l(&rw_mutex_);
  const std::string& table_name = table_quota.table_name();
  auto it = table_quotas_.find(table_name);
  if (TableQuota::kDelQuota == table_quota.type()) {
    if (it != table_quotas_.end()) {
      table_quotas_.erase(it);
      VLOG(7) << "del quota " << table_name;
    }
    return true;
  }
  if (it == table_quotas_.end()) {
    QuotaLimiterPtr new_limiter(
        LimiterFactory::CreateQuotaLimiter(FLAGS_tera_quota_limiter_type, table_name));
    if (!new_limiter) {
      VLOG(30) << "quota table[" << table_name
               << "] QuotaLimiterContainer CreateQuotaLimiter failed!";
      return false;
    }
    table_quotas_.emplace(std::make_pair(table_name, new_limiter));
    VLOG(30) << "quota setting table[" << table_name << "] first time";
  }
  table_quotas_[table_name]->Reset(table_quota);
  return true;
}
}  // namespace quota
}  // namespace tera
