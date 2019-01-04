// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "quota/quota_entry.h"
#include "quota/helpers/quota_utils.h"
#include "ts_write_flow_controller.h"
#include <glog/logging.h>

DECLARE_bool(tera_quota_enabled);
DECLARE_int64(tera_quota_normal_estimate_value);
DECLARE_double(tera_quota_adjust_estimate_ratio);

namespace tera {
namespace quota {

bool QuotaEntry::CheckAndConsume(const std::string& table_name,
                                 const OpTypeAmountList& op_type_amount_list) {
  if (!FLAGS_tera_quota_enabled) {
    VLOG(25) << "tera quota disabled";
    return true;
  }
  QuotaLimiterPtr limiter;
  if (!quota_limiter_container_.GetTableLimiter(table_name, &limiter)) {
    VLOG(25) << "quota couldn't find specified table[" << table_name << "] setting";
    return true;
  }
  return CheckAndConsumeInternal(table_name, op_type_amount_list, limiter);
}

bool QuotaEntry::Reset(const TableQuota& table_quota) {
  std::vector<std::string> key_list(3);
  GetQuotaOperationKey(table_quota.table_name(), kQuotaReadBytes, &key_list[0]);
  GetQuotaOperationKey(table_quota.table_name(), kQuotaScanReqs, &key_list[1]);
  GetQuotaOperationKey(table_quota.table_name(), kQuotaScanBytes, &key_list[2]);
  {
    WriteLock l(&rw_mutex_);
    for (auto& key : key_list) {
      auto iter = estimate_rows_bytes_opkey_.find(key);
      if (iter == estimate_rows_bytes_opkey_.end()) {
        estimate_rows_bytes_opkey_.emplace(key, FLAGS_tera_quota_normal_estimate_value);
      }
    }
  }

  VLOG(25) << "Reset table[" << table_quota.table_name()
           << "] quota :" << QuotaUtils::DebugPrintTableQuota(table_quota);
  return quota_limiter_container_.ResetQuota(table_quota);
}

bool QuotaEntry::CheckAndConsumeInternal(const std::string& table_name,
                                         const OpTypeAmountList& op_type_amount_list,
                                         const QuotaLimiterPtr& limiter) {
  Throttle throttle;
  for (auto& op_type_amount : op_type_amount_list) {
    switch (op_type_amount.first) {
      case kQuotaWriteReqs:
        throttle.write_reqs = op_type_amount.second;
        break;
      case kQuotaWriteBytes:
        throttle.write_bytes = op_type_amount.second;
        break;
      case kQuotaReadReqs:
        throttle.read_reqs = op_type_amount.second;
        throttle.read_bytes = Estimate(table_name, kQuotaReadBytes, op_type_amount.second);
        break;
      case kQuotaScanReqs:
        throttle.scan_reqs = Estimate(table_name, kQuotaScanReqs, op_type_amount.second);
        throttle.scan_bytes = Estimate(table_name, kQuotaScanBytes, op_type_amount.second);
        break;
      case kQuotaReadBytes:
      case kQuotaScanBytes:
        break;
      default:
        // error type, no limit
        VLOG(25) << "Set wrong quota_op_type[" << op_type_amount.first
                 << "], return no limit for table[" << table_name << "]";
        return true;
    }
  }

  VLOG(25) << "QuotaCheckAndConsume details WriteReqs : " << throttle.write_reqs
           << ", WriteBytes : " << throttle.write_bytes << ", ReadReqs : " << throttle.read_reqs
           << ", ReadBytes : " << throttle.read_bytes << ", ScanReqs : " << throttle.scan_reqs
           << ", ScanBytes : " << throttle.scan_bytes;

  if (!limiter->CheckAndConsume(throttle)) {
    VLOG(7) << "Quota reach limit for table[" << table_name << "]";
    return false;
  }
  return true;
}

void QuotaEntry::GetQuotaOperationKey(const std::string& table_name, QuotaOperationType type,
                                      std::string* key) {
  key->clear();
  *key = table_name;
  key->push_back('|');
  key->append(QuotaUtils::GetQuotaOperation(type));
}

int64_t QuotaEntry::Estimate(const std::string& table_name, QuotaOperationType type, int64_t reqs) {
  std::string key;
  GetQuotaOperationKey(table_name, type, &key);
  int64_t estimate_value = 0;
  {
    ReadLock l(&rw_mutex_);
    auto iter = estimate_rows_bytes_opkey_.find(key);
    if (iter != estimate_rows_bytes_opkey_.end()) {
      estimate_value = iter->second;
    }
  }
  if (estimate_value <= 0) {
    estimate_value = FLAGS_tera_quota_normal_estimate_value;
  }
  VLOG(25) << "table_name[" << table_name << "] Estimate : " << key << " estimate_value["
           << estimate_value << "]";
  return estimate_value * reqs;
}

std::string QuotaEntry::DebugEstimateBytes() {
  std::ostringstream output;
  for (auto it = estimate_rows_bytes_opkey_.begin(); it != estimate_rows_bytes_opkey_.end(); ++it) {
    output << it->first << " : " << it->second << "\n";
  }
  return output.str();
}

void QuotaEntry::Adjust(const std::string& table_name, QuotaOperationType type,
                        int64_t estimate_value) {
  std::string key;
  GetQuotaOperationKey(table_name, type, &key);
  ReadLock l(&rw_mutex_);
  auto iter = estimate_rows_bytes_opkey_.find(key);
  if (iter != estimate_rows_bytes_opkey_.end()) {
    if (iter->second <= 0) {
      iter->second = FLAGS_tera_quota_normal_estimate_value;
    }
    iter->second = iter->second * FLAGS_tera_quota_adjust_estimate_ratio +
                   estimate_value * (1 - FLAGS_tera_quota_adjust_estimate_ratio);
    VLOG(7) << "Adjust : " << DebugEstimateBytes();
  }
}

void QuotaEntry::Update(const QueryRequest* request, QueryResponse* response) {
  if (request->has_slowdown_write_ratio()) {
    TsWriteFlowController::Instance().SetSlowdownMode(request->slowdown_write_ratio());
  } else {
    TsWriteFlowController::Instance().ResetSlowdownMode();
  }

  if (request->has_dfs_write_throughput_hard_limit()) {
    LOG(WARNING) << "Set dfs write hard limit to " << request->dfs_write_throughput_hard_limit()
                 << "bytes/s.";
    DfsWriteThroughputHardLimiter().EnterFlowControlMode(
        request->dfs_write_throughput_hard_limit());
  } else {
    DfsWriteThroughputHardLimiter().LeaveFlowControlMode();
  }

  if (request->has_dfs_read_throughput_hard_limit()) {
    LOG(WARNING) << "Set dfs read hard limit to " << request->dfs_read_throughput_hard_limit()
                 << "bytes/s.";
    DfsReadThroughputHardLimiter().EnterFlowControlMode(request->dfs_read_throughput_hard_limit());
  } else {
    DfsReadThroughputHardLimiter().LeaveFlowControlMode();
  }

  if (request->has_quota_version()) {
    VLOG(25) << "ts quota version : " << version_recorder_.GetVersion()
             << ", QueryRequest version : " << request->quota_version();
    int32_t table_quotas_size = request->table_quotas_size();
    bool reset_success = true;
    for (int32_t table_quotas_index = 0; table_quotas_index < table_quotas_size;
         ++table_quotas_index) {
      if (!Reset(request->table_quotas(table_quotas_index))) {
        reset_success = false;
        break;
      }
    }
    if (reset_success) {
      version_recorder_.SetVersion(request->quota_version());
    }
  }
  response->set_quota_version(version_recorder_.GetVersion());
}
}  // namespace quota
}  // namespace tera
