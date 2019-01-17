// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#include <string>
#include <memory>
#include "tera.h"
#include "proto/filter.pb.h"
#include "sdk/filter_list_base.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace tera {
namespace filter {

FilterListBase::~FilterListBase() {}

bool FilterListBase::IsEmpty() { return filters_.empty(); }

const std::vector<FilterPtr>& FilterListBase::GetFilters() { return filters_; }

}  // namesapce filter
}  // namesapce tera
