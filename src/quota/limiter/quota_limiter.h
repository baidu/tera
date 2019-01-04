// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "proto/quota.pb.h"

namespace tera {
namespace quota {

struct Throttle {
  Throttle()
      : write_reqs(0), write_bytes(0), read_reqs(0), read_bytes(0), scan_reqs(0), scan_bytes(0) {}
  int64_t write_reqs;
  int64_t write_bytes;
  int64_t read_reqs;
  int64_t read_bytes;
  int64_t scan_reqs;
  int64_t scan_bytes;
};

class QuotaLimiter {
 public:
  virtual void Reset(const TableQuota& table_quota) = 0;

  // if quota limited, return false
  // otherwise, consume the quota and return true
  virtual bool CheckAndConsume(const Throttle& throttle) = 0;
};
}
}
