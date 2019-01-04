// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#pragma once

#include <string>
#include <memory>
#include "sdk/filter_list_base.h"

namespace tera {
namespace filter {

class FilterListWithAND : public FilterListBase {
 public:
  FilterListWithAND();
  virtual ~FilterListWithAND();
  void Reset();
  virtual ReturnCode FilterCell(const std::string& column_family,
                                const std::string& column_qualifier, const std::string& value);
  virtual bool FilterRow();
  virtual void AddFilter(const FilterPtr& filter);

 private:
  ReturnCode MergeReturnCode(ReturnCode rc, ReturnCode localRC);
};

}  // namesapce filter
}  // namesapce tera
