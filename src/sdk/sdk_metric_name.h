// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_METRIC_NAME_H_
#define TERA_SDK_METRIC_NAME_H_

#include <string>

#include "common/metric/hardware_collectors.h"

namespace tera {

// global transaction labels
const char* const kGTxnLabelRead = "gtxn:read";
const char* const kGTxnLabelCommit = "gtxn:commit";
const char* const kGTxnLabelTso = "gtxn:tso";

// glabel transaction read metric names
const char* const kGTxnReadDelayMetric = "tera_sdk_gtxn_read_delay_us";
const char* const kGTxnReadCountMetric = "tera_sdk_gtxn_read_count";
const char* const kGTxnReadFailCountMetric = "tera_sdk_gtxn_read_fail_count";
const char* const kGTxnReadRetryCountMetric = "tera_sdk_gtxn_read_retry_count";
const char* const kGTxnReadRollBackCountMetric = "tera_sdk_gtxn_read_rollback_count";
const char* const kGTxnReadRollForwardCountMetric = "tera_sdk_gtxn_read_rollforward_count";

// global transaction commit metric names
const char* const kGTxnCommitDelayMetric = "tera_sdk_gtxn_commit_delay_us";
const char* const kGTxnCommitCountMetric = "tera_sdk_gtxn_commit_count";
const char* const kGTxnCommitFailCountMetric = "tera_sdk_gtxn_commit_fail_count";

const char* const kGTxnPrewriteDelayMetric = "tera_sdk_gtxn_prewrite_delay_us";
const char* const kGTxnPrewriteCountMetric = "tera_sdk_gtxn_prewrite_count";
const char* const kGTxnPrewriteFailCountMetric = "tera_sdk_gtxn_prewrite_fail_count";

const char* const kGTxnPrimaryDelayMetric = "tera_sdk_gtxn_primary_delay_us";
const char* const kGTxnPrimaryCountMetric = "tera_sdk_gtxn_primary_count";
const char* const kGTxnPrimaryFailCountMetric = "tera_sdk_gtxn_primary_fail_count";

const char* const kGTxnSecondariesDelayMetric = "tera_sdk_gtxn_secondaries_delay_us";
const char* const kGTxnSecondariesCountMetric = "tera_sdk_gtxn_secondaries_count";
const char* const kGTxnSecondariesFailCountMetric = "tera_sdk_gtxn_secondaries_fail_count";

const char* const kGTxnAcksDelayMetric = "tera_sdk_gtxn_acks_delay_us";
const char* const kGTxnAcksCountMetric = "tera_sdk_gtxn_acks_count";
const char* const kGTxnAcksFailCountMetric = "tera_sdk_gtxn_acks_fail_count";

const char* const kGTxnNotifiesDelayMetric = "tera_sdk_gtxn_notifies_delay_us";
const char* const kGTxnNotifiesCountMetric = "tera_sdk_gtxn_notifies_count";
const char* const kGTxnNotifiesFailCountMetric = "tera_sdk_gtxn_notifies_fail_count";

const char* const kGTxnTsoDelayMetric = "tera_sdk_gtxn_tso_delay_us";
const char* const kGTxnTsoRequestCountMetric = "tera_sdk_gtxn_tso_request_count";
}  // end namespace tera

#endif  // TERA_SDK_METRIC_NAME_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
