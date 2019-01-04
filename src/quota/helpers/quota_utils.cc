// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <memory>
#include <sstream>
#include "quota/helpers/quota_utils.h"

namespace tera {
namespace quota {

std::string QuotaUtils::GetQuotaOperation(QuotaOperationType type) {
  static const char* msg[] = {"QuotaWriteReqs", "QuotaWriteBytes", "QuotaReadReqs",
                              "QuotaReadBytes", "QuotaScanReqs",   "QuotaScanBytes",
                              "QuotaUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  using QuotaOpType = std::underlying_type<QuotaOperationType>::type;
  uint32_t index = static_cast<QuotaOpType>(type) - static_cast<QuotaOpType>(kQuotaWriteReqs);
  index = index < msg_size ? index : (msg_size - 1);
  return msg[index];
}

std::string QuotaUtils::DebugPrintTableQuota(const TableQuota& table_quota) {
  std::ostringstream table_quota_info;
  table_quota_info << "table[" << table_quota.table_name() << "] :\n";
  int quota_infos_size = table_quota.quota_infos_size();
  for (int quota_infos_index = 0; quota_infos_index < quota_infos_size; ++quota_infos_index) {
    const QuotaInfo& quota_info = table_quota.quota_infos(quota_infos_index);
    table_quota_info << "QuotaOperationType[" << GetQuotaOperation(quota_info.type()) << "], limit["
                     << quota_info.limit() << "], period[" << quota_info.period() << "]\n";
  }
  return table_quota_info.str();
}
}
}
