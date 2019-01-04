// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>
#include "proto/quota.pb.h"

namespace tera {

namespace master {
struct MetaWriteRecord;
}

namespace quota {

class MasterQuotaHelper {
 public:
  // New**() func will new a instance in heap, it's memory should delete by user.
  static master::MetaWriteRecord* NewMetaRecordFromQuota(const TableQuota& table_quota);
  static TableQuota* NewTableQuotaFromMetaValue(const std::string& value);

  static void PackDeleteQuotaRecords(const std::string& table_name,
                                     std::vector<master::MetaWriteRecord>& records);

  static std::string GetTableNameFromMetaKey(const std::string& key);

  // Merge table_quota to target_table_quota,
  // It'll be incremental merge, that means quota update incremental
  static bool MergeTableQuota(const TableQuota& table_quota, TableQuota* target_table_quota);

  // default value for quota info, period = 1, limit = -1
  static void SetDefaultQuotaInfo(QuotaInfo* quota_info, QuotaOperationType type);
};
}
}
