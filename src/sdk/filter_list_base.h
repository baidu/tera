// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#pragma once

#include <string>
#include <vector>
#include <memory>
#include "tera/filter.h"

namespace tera {
namespace filter {

class FilterListBase : public FilterBase {
 public:
  virtual ~FilterListBase();
  virtual void AddFilter(const FilterPtr& filter) = 0;
  const std::vector<FilterPtr>& GetFilters();

 protected:
  bool IsEmpty();

 protected:
  std::vector<FilterPtr> filters_;
};

}  // namesapce filter
}  // namesapce tera
