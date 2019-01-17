// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "quota/helpers/master_quota_helper.h"
#include "master/master_env.h"
#include <memory>

namespace tera {
namespace quota {

master::MetaWriteRecord* MasterQuotaHelper::NewMetaRecordFromQuota(const TableQuota& table_quota) {
  if (table_quota.quota_infos_size() <= 0) {
    return nullptr;
  }
  std::unique_ptr<master::MetaWriteRecord> meta_write_record(new master::MetaWriteRecord);
  meta_write_record->is_delete = false;
  meta_write_record->key = std::string("|10") + table_quota.table_name();
  if (!table_quota.SerializeToString(&meta_write_record->value)) {
    return nullptr;
  }
  return meta_write_record.release();
}

void MasterQuotaHelper::PackDeleteQuotaRecords(const std::string& table_name,
                                               std::vector<master::MetaWriteRecord>& records) {
  std::string key = std::string("|10") + table_name;
  records.emplace_back(master::MetaWriteRecord(key, "", true));
}

std::string MasterQuotaHelper::GetTableNameFromMetaKey(const std::string& key) {
  if (key.length() <= 3 || key[1] != '1' || key[2] != '0') {
    return "";
  }
  return key.substr(3);
}

TableQuota* MasterQuotaHelper::NewTableQuotaFromMetaValue(const std::string& value) {
  if (value.size() <= 0) {
    return nullptr;
  }
  std::unique_ptr<TableQuota> table_quota(new TableQuota);
  if (!table_quota->ParseFromString(value)) {
    return nullptr;
  }
  return table_quota.release();
}

static bool CompareAndSetQuotaInfo(const QuotaInfo& quota_info, TableQuota* target_table_quota) {
  bool found_delta = false;
  int target_quota_infos_size = target_table_quota->quota_infos_size();
  for (int i = 0; i < target_quota_infos_size; ++i) {
    QuotaInfo* target_quota_info = target_table_quota->mutable_quota_infos(i);
    if (quota_info.type() == target_quota_info->type()) {
      if (quota_info.limit() != target_quota_info->limit()) {
        target_quota_info->set_limit(quota_info.limit());
        found_delta = true;
      }
      if (quota_info.period() != target_quota_info->period()) {
        target_quota_info->set_period(quota_info.period());
        found_delta = true;
      }
    }
  }
  return found_delta;
}

bool MasterQuotaHelper::MergeTableQuota(const TableQuota& table_quota,
                                        TableQuota* target_table_quota) {
  bool found_delta = false;
  int quota_infos_size = table_quota.quota_infos_size();
  for (int quota_info_index = 0; quota_info_index < quota_infos_size; ++quota_info_index) {
    const QuotaInfo& quota_info = table_quota.quota_infos(quota_info_index);
    found_delta |= CompareAndSetQuotaInfo(quota_info, target_table_quota);
  }
  return found_delta;
}

void MasterQuotaHelper::SetDefaultQuotaInfo(QuotaInfo* quota_info, QuotaOperationType type) {
  quota_info->set_type(type);
  quota_info->set_limit(-1);
  quota_info->set_period(1);
}
}
}
