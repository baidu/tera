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
#include "filter_utils.h"

namespace tera {
namespace filter {

FilterBase::FilterBase() {}

FilterBase::~FilterBase() {}

FilterType FilterBase::Type() { return kUnDefinedFilter; }

void FilterBase::Reset() {}

Filter::ReturnCode FilterBase::FilterCell(const std::string& column_family,
                                          const std::string& column_qualifier,
                                          const std::string& value) {
  return kIncludeCurCell;
}

bool FilterBase::FilterRow() { return false; }

bool FilterBase::SerializeTo(std::string* serialized_filter) { return false; }

bool FilterBase::ParseFrom(const std::string& serialized_filter) { return false; }

void FilterBase::GetAllColumn(ColumnSet* filter_column_set) {}

}  // namesapce filter
}  // namesapce tera
