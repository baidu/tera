// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_perf.h"

#include "gflags/gflags.h"

#include "common/metric/metric_counter.h"
#include "sdk/sdk_metric_name.h"

namespace tera {
namespace sdk {

void PerfCollecter::DumpLog() {
  std::shared_ptr<CollectorReport> latest_report =
      CollectorReportPublisher::GetInstance().GetCollectorReport();
  int64_t interval = latest_report->interval_ms;
  if (interval <= 0) {
    // maybe happen at first report, the metric values must be 0
    // set to any non-zero value to avoid div 0
    VLOG(16) << "Metric Report interval is 0";
    interval = 1000;
  }
  int64_t read_delay = latest_report->FindMetricValue(kGTxnReadDelayMetric, kGTxnLabelRead);
  int64_t read_cnt = latest_report->FindMetricValue(kGTxnReadCountMetric, kGTxnLabelRead);
  read_delay = read_cnt > 0 ? read_delay / read_cnt : 0;

  LOG(INFO) << "[perf][gtxn] "
            << "read_delay " << read_delay << " read_cnt " << read_cnt << " read_fail "
            << latest_report->FindMetricValue(kGTxnReadFailCountMetric, kGTxnLabelRead)
            << " read_retry_cnt "
            << latest_report->FindMetricValue(kGTxnReadRetryCountMetric, kGTxnLabelRead)
            << " read_rollback_cnt "
            << latest_report->FindMetricValue(kGTxnReadRollBackCountMetric, kGTxnLabelRead)
            << " read_rollforward_cnt "
            << latest_report->FindMetricValue(kGTxnReadRollForwardCountMetric, kGTxnLabelRead);

  int64_t commit_delay = latest_report->FindMetricValue(kGTxnCommitDelayMetric, kGTxnLabelCommit);
  int64_t commit_cnt = latest_report->FindMetricValue(kGTxnCommitCountMetric, kGTxnLabelCommit);
  commit_delay = commit_cnt > 0 ? commit_delay / commit_cnt : 0;

  int64_t prewrite_delay =
      latest_report->FindMetricValue(kGTxnPrewriteDelayMetric, kGTxnLabelCommit);
  int64_t prewrite_cnt = latest_report->FindMetricValue(kGTxnPrewriteCountMetric, kGTxnLabelCommit);
  prewrite_delay = prewrite_cnt > 0 ? prewrite_delay / prewrite_cnt : 0;

  int64_t primary_delay = latest_report->FindMetricValue(kGTxnPrimaryDelayMetric, kGTxnLabelCommit);
  int64_t primary_cnt = latest_report->FindMetricValue(kGTxnPrimaryCountMetric, kGTxnLabelCommit);
  primary_delay = primary_cnt > 0 ? primary_delay / primary_cnt : 0;

  int64_t secondaries_delay =
      latest_report->FindMetricValue(kGTxnSecondariesDelayMetric, kGTxnLabelCommit);
  int64_t secondaries_cnt =
      latest_report->FindMetricValue(kGTxnSecondariesCountMetric, kGTxnLabelCommit);
  secondaries_delay = secondaries_cnt > 0 ? secondaries_delay / secondaries_cnt : 0;

  LOG(INFO) << "[perf][gtxn] "
            << "commit_delay " << commit_delay << " commit_cnt " << commit_cnt << " commit_fail "
            << latest_report->FindMetricValue(kGTxnCommitFailCountMetric, kGTxnLabelCommit)
            << " prew_delay " << prewrite_delay << " prew_cnt " << prewrite_cnt << " prew_fail "
            << latest_report->FindMetricValue(kGTxnPrewriteFailCountMetric, kGTxnLabelCommit)
            << " pri_delay " << primary_delay << " pri_cnt " << primary_cnt << " pri_fail "
            << latest_report->FindMetricValue(kGTxnPrimaryFailCountMetric, kGTxnLabelCommit)
            << " se_delay " << secondaries_delay << " se_cnt " << secondaries_cnt << " se_fail "
            << latest_report->FindMetricValue(kGTxnSecondariesFailCountMetric, kGTxnLabelCommit);

  int64_t tso_delay = latest_report->FindMetricValue(kGTxnTsoDelayMetric, kGTxnLabelTso);
  int64_t tso_cnt = latest_report->FindMetricValue(kGTxnTsoRequestCountMetric, kGTxnLabelTso);
  tso_delay = tso_cnt > 0 ? tso_delay / tso_cnt : 0;
  LOG(INFO) << "[perf][gtxn] tso_delay " << tso_delay << " tso_cnt " << tso_cnt;

  int64_t notify_delay = latest_report->FindMetricValue(kGTxnNotifiesDelayMetric, kGTxnLabelCommit);
  int64_t notify_cnt = latest_report->FindMetricValue(kGTxnNotifiesCountMetric, kGTxnLabelCommit);
  notify_delay = notify_cnt > 0 ? notify_delay / notify_cnt : 0;

  int64_t ack_delay = latest_report->FindMetricValue(kGTxnAcksDelayMetric, kGTxnLabelCommit);
  int64_t ack_cnt = latest_report->FindMetricValue(kGTxnAcksCountMetric, kGTxnLabelCommit);
  ack_delay = ack_cnt > 0 ? ack_delay / ack_cnt : 0;

  LOG(INFO) << "[perf][gtxn] "
            << "notify_delay " << notify_delay << " notify_cnt " << notify_cnt << " notify_fail "
            << latest_report->FindMetricValue(kGTxnNotifiesFailCountMetric, kGTxnLabelCommit)
            << " ack_delay " << ack_delay << " ack_cnt " << ack_cnt << " ack_fail "
            << latest_report->FindMetricValue(kGTxnAcksFailCountMetric, kGTxnLabelCommit);
}

}  // namespace sdk
}  // namespace tera
