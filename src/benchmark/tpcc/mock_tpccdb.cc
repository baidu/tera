// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "benchmark/tpcc/mock_tpccdb.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {
namespace tpcc {

MockTpccDb::MockTpccDb() : flag_(true) {}

}  // namespace tpcc
}  // namespace tera
