// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TYPES_H_
#define TERA_TYPES_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "common/base/stdint.h"

namespace tera {

typedef int32_t TabletNodeId;

const int32_t kInvalidId = -1;
const uint64_t kSequenceIDStart = 0;
const int64_t kInvalidTimerId = 0;
const uint32_t kUnknownId = -1U;
const uint32_t kInvalidSessionId = -1U;
const std::string kUnknownAddr = "255.255.255.255:0000";
const uint64_t kMaxTimeStamp = (1ULL << 56) - 1;
const uint32_t kMaxHostNameSize = 255;
const std::string kMasterNodePath = "/master";
const std::string kMasterLockPath = "/master-lock";
const std::string kTsListPath = "/ts";
const std::string kKickPath = "/kick";
const std::string kRootTabletNodePath = "/root_table";
const std::string kSafeModeNodePath = "/safemode";
const std::string kSms = "[SMS] ";
const std::string kMail = "[MAIL] ";
const int64_t kLatestTs = INT64_MAX;
const int64_t kOldestTs = INT64_MIN;
const int32_t kMaxRpcSize = (16 << 20);
const uint64_t kRowkeySize = (64 << 10);       // 64KB
const uint64_t kQualifierSize = (64 << 10);    // 64KB
const uint64_t kValueSize = (32 << 20);        // 32MB

} // namespace tera

#endif // TERA_TYPES_H_
