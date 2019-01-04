// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <deque>
#include <mutex>
#include <unordered_map>
#include "access/helpers/version_recorder.h"
#include "common/rwmutex.h"
#include "proto/quota.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "quota/quota_limiter_container.h"
#include "quota/flow_controller.h"

namespace tera {

namespace quota {

using OpTypeAmountPair = std::pair<QuotaOperationType, int64_t>;
using OpTypeAmountList = std::vector<OpTypeAmountPair>;

class QuotaEntry {
 public:
  QuotaEntry() {}
  virtual ~QuotaEntry() {}

  QuotaEntry(QuotaEntry&) = delete;
  QuotaEntry& operator=(const QuotaEntry&) = delete;

  bool CheckAndConsume(const std::string& table_name, const OpTypeAmountList& op_type_amount_list);

  void Update(const QueryRequest* request, QueryResponse* response);

  // Adjust read and scan bytes&reqs per request every time
  void Adjust(const std::string& table_name, QuotaOperationType type, int64_t estimate_value);

 private:
  // clear quota and set a new one
  bool Reset(const TableQuota& table_quota);

  // if out of quota , return false
  // otherwise, consume quota and return true
  bool CheckAndConsumeInternal(const std::string& table_name,
                               const OpTypeAmountList& op_type_amount_list,
                               const QuotaLimiterPtr& limiter);

  void GetQuotaOperationKey(const std::string& table_name, QuotaOperationType type,
                            std::string* key);

  // estimate read/scan throughput
  int64_t Estimate(const std::string& table_name, QuotaOperationType type, int64_t reqs);

  std::string DebugEstimateBytes();

 private:
  QuotaLimiterContainer quota_limiter_container_;

  // <QuotaOperationKey, EstimateBytes/Reqs>
  // QuotaOperationKey : table_name|type, only for read/scan operation
  //        key                   |           value
  // table_name|kQuotaReadBytes   |   estimate bytes for each read rpc
  // table_name|kQuotaScanReqs    |  estimate row num for each scan rpc
  // table_name|kQuotaScanBytes   |   estimate bytes for each scan rpc
  // Notice: scan reqs has error about 15% cause by int64_t
  // If need more precise, should use double
  std::unordered_map<std::string, int64_t> estimate_rows_bytes_opkey_;
  mutable RWMutex rw_mutex_;

  auth::VersionRecorder version_recorder_;
};
}  // namespace quota
}  // namespace tera
