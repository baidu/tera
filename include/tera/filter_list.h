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
#include "tera/filter.h"

namespace tera {
namespace filter {

class FilterList;
using FilterListPtr = std::shared_ptr<FilterList>;

class FilterListBase;
class FilterList : public FilterBase {
  // User interface
 public:
  /*
   * if you want to output the rows in which there is a value of family "cf1" and qualifier "qu1",
   * which is >2 and <5, in scanning, you can New a filter list with AND Operator, which have
   * two value filters, one with a CompareOperator GREATER and a IntegerComparator with
   * a ref_value 2, the other with a CompareOperator LESS and a IntegerComparator with
   * a ref_value 5.
   */
  enum Operator {
    kAnd,       // sub_filter_1 && sub_filter_2 && ..., all sub filters connected with AND
    kOr,        // sub_filter_1 || sub_filter_2 || ..., all sub filters connected with OR
    kInvalidOp  // invalid op
  };

  /*
   * User must New a filter list object by Using this methed, and must use std::make_shared method
   * and assign the object to FilterListPtr.
   */
  explicit FilterList(Operator op);

  /*
   * Use this method to add filter to this filter list. Add filters one by one.
   */
  bool AddFilter(const FilterPtr& filter);

  // internal use
 public:
  FilterList();
  virtual ~FilterList();
  virtual FilterType Type();
  virtual void Reset();
  virtual ReturnCode FilterCell(const std::string& column_family,
                                const std::string& column_qualifier, const std::string& value);
  virtual bool FilterRow();
  virtual bool SerializeTo(std::string* serialized_filter);
  virtual bool ParseFrom(const std::string& serialized_filter);
  virtual void GetAllColumn(ColumnSet* filter_column_set);

 private:
  Operator op_;
  FilterListBase* filter_list_base_;
};

}  // namesapce filter
}  // namesapce tera
