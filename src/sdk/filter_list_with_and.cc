// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#include <string>
#include <memory>
#include "sdk/filter_list_with_and.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace tera {
namespace filter {

FilterListWithAND::FilterListWithAND() {}

FilterListWithAND::~FilterListWithAND() {}

void FilterListWithAND::AddFilter(const FilterPtr& filter) { filters_.push_back(filter); }

void FilterListWithAND::Reset() {
  for (auto it = filters_.begin(); it != filters_.end(); ++it) {
    FilterPtr filter = *it;
    filter->Reset();
  }
}

Filter::ReturnCode FilterListWithAND::FilterCell(const std::string& column_family,
                                                 const std::string& column_qualifier,
                                                 const std::string& value) {
  if (IsEmpty()) {
    return kIncludeCurCell;
  }
  ReturnCode rc = kIncludeCurCell;
  for (auto it = filters_.begin(); it != filters_.end(); ++it) {
    FilterPtr filter = *it;
    ReturnCode cur_rc;
    cur_rc = filter->FilterCell(column_family, column_qualifier, value);
    rc = MergeReturnCode(rc, cur_rc);
  }
  return rc;
}

bool FilterListWithAND::FilterRow() {
  if (IsEmpty()) {
    return false;
  }
  for (auto it = filters_.begin(); it != filters_.end(); ++it) {
    FilterPtr filter = *it;
    if (filter->FilterRow()) {
      return true;
    }
  }
  return false;
}

Filter::ReturnCode FilterListWithAND::MergeReturnCode(ReturnCode rc, ReturnCode cur_rc) {
  switch (cur_rc) {
    case kIncludeCurCell:
      return rc;
    case kNotIncludeCurAndLeftCellOfRow:
      return kNotIncludeCurAndLeftCellOfRow;
    default:
      LOG(ERROR) << "not support ReturnCode of curRC";
      return kNotIncludeCurAndLeftCellOfRow;
  }
}

}  // namesapce filter
}  // namesapce tera
