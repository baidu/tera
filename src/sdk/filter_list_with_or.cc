// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#include <string>
#include <memory>
#include "sdk/filter_list_with_or.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace tera {
namespace filter {

FilterListWithOR::FilterListWithOR() {}

FilterListWithOR::~FilterListWithOR() {}

void FilterListWithOR::AddFilter(const FilterPtr& filter) {
  filters_.push_back(filter);
  prev_filter_rc_list_.resize(filters_.size());
}

void FilterListWithOR::Reset() {
  for (auto it = filters_.begin(); it != filters_.end(); ++it) {
    FilterPtr filter = *it;
    filter->Reset();
  }
  for (auto it = prev_filter_rc_list_.begin(); it != prev_filter_rc_list_.end(); ++it) {
    *it = kUndefinedRC;
  }
}

bool FilterListWithOR::NeedFilter(ReturnCode prevRC) {
  if (prevRC == kUndefinedRC) {
    return true;
  }
  switch (prevRC) {
    case kIncludeCurCell:
      return true;
    case kNotIncludeCurAndLeftCellOfRow:
      return false;
    default:
      LOG(ERROR) << "illegal ReturnCode";
      return false;
  }
}

Filter::ReturnCode FilterListWithOR::FilterCell(const std::string& column_family,
                                                const std::string& column_qualifier,
                                                const std::string& value) {
  if (IsEmpty()) {
    return kIncludeCurCell;
  }
  ReturnCode rc = kIncludeCurCell;
  std::vector<ReturnCode>::iterator prev_filter_rc_list_it = prev_filter_rc_list_.begin();
  for (auto it = filters_.begin(); it != filters_.end(); ++it) {
    ReturnCode prevRC = *prev_filter_rc_list_it;
    if (!NeedFilter(prevRC)) {
      ++prev_filter_rc_list_it;
      continue;
    }
    FilterPtr filter = *it;
    ReturnCode cur_rc;
    cur_rc = filter->FilterCell(column_family, column_qualifier, value);
    *prev_filter_rc_list_it = cur_rc;
    ++prev_filter_rc_list_it;
    rc = MergeReturnCode(rc, cur_rc);
  }
  return rc;
}

bool FilterListWithOR::FilterRow() {
  if (IsEmpty()) {
    return false;
  }
  for (auto it = filters_.begin(); it != filters_.end(); ++it) {
    FilterPtr filter = *it;
    if (!filter->FilterRow()) {
      return false;
    }
  }
  return true;
}

Filter::ReturnCode FilterListWithOR::MergeReturnCode(ReturnCode rc, ReturnCode cur_rc) {
  switch (cur_rc) {
    case kIncludeCurCell:
      return kIncludeCurCell;
    case kNotIncludeCurAndLeftCellOfRow:
      if (rc == kIncludeCurCell || rc == kNotIncludeCurAndLeftCellOfRow) {
        return rc;
      } else {
        LOG(ERROR) << "not support ReturnCode of rc";
        return kNotIncludeCurAndLeftCellOfRow;
      }
    default:
      LOG(ERROR) << "not support ReturnCode of curRC";
      return kNotIncludeCurAndLeftCellOfRow;
  }
}

}  // namesapce filter
}  // namesapce tera
