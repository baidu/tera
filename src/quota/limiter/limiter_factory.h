// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <glog/logging.h>
#include "quota/limiter/general_quota_limiter.h"

namespace tera {
namespace quota {

static const std::string general_quota_limiter_type = "general_quota_limiter";

class LimiterFactory {
 public:
  LimiterFactory() {}
  ~LimiterFactory() {}
  static QuotaLimiter* CreateQuotaLimiter(const std::string& limiter_type,
                                          const std::string& table_name) {
    if (general_quota_limiter_type == limiter_type) {
      return new GeneralQuotaLimiter(table_name);
    } else {
      LOG(ERROR) << "Not surport limit_type = " << limiter_type;
      return nullptr;
    }
  }
};
}
}
