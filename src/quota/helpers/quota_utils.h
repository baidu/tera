// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>
#include "proto/quota.pb.h"

namespace tera {
namespace quota {

class QuotaUtils {
 public:
  static std::string DebugPrintTableQuota(const TableQuota& table_quota);
  static std::string GetQuotaOperation(QuotaOperationType type);
};
}
}
