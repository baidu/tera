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
#include "tera/filter_comparator.h"

namespace tera {
namespace filter {

class ValueFilter;
using ValueFilterPtr = std::shared_ptr<ValueFilter>;

/*
 *  User can use this class to make a value filter.
 *  Please use std::make_shared method to New a value filter object, and assign the object to
 *  the ValueFilterPtr.
 */
class ValueFilter : public FilterBase {
  // User interface
 public:
  /*
   * New a value filter by using this method, And assign the object to a ValueFilterPtr.
   * Note that a comparator object should be exist before you new a value filter. You can refer to
   * the filter_comparator.h for how to new a comparator object. CompareOperator is also defined
   * in the filter_comparator.h
   */
  ValueFilter(CompareOperator op, const FilterComparatorPtr& comparator);

  /*
   * User can set the column family in which the value cell should be execute filter.
   * If not set, the family will be "".
   */
  void SetColumnFamily(const std::string& column_family);

  /*
   * User can set the qualifier in which the value cell should be execute filter.
   * If not set, the qualifier will be "".
   */
  void SetColumnQualifier(const std::string& column_qualifier);

  /*
   * filter_if_missing is true means that if there is no family and qualifier matched in this row,
   * which the filter specified, the row will be filtered out (not output), or the row will be not
   * filtered (the row will be output).
   *
   * Default is false.
   */
  void SetFilterIfMissing(bool filter_if_missing);

  /*
   * User do NOT need to use the interfaces below. The internal of tera will use them.
   */
 public:
  ValueFilter();
  virtual ~ValueFilter();
  virtual FilterType Type();
  virtual void Reset();
  virtual ReturnCode FilterCell(const std::string& column_family,
                                const std::string& column_qualifier, const std::string& value);
  virtual bool FilterRow();
  virtual bool SerializeTo(std::string* serialized_filter);
  virtual bool ParseFrom(const std::string& serialized_filter);
  virtual void GetAllColumn(ColumnSet* column_set);

 private:
  virtual ReturnCode FilterCellWithEmptyQualifier(const std::string& column_family,
                                                  const std::string& column_qualifier,
                                                  const std::string& value);
  virtual ReturnCode FilterCellWithNotEmptyQualifier(const std::string& column_family,
                                                     const std::string& column_qualifier,
                                                     const std::string& value);
  bool MatchValue(const std::string& value);
  bool MatchOp(int compare_result);

 private:
  enum MatchStatus {
    kNotMatchAnything,        // not match cf and qu yet
    kMatchColumnButNotValue,  // matched cf and qu, but not match the value of the cf and qu
    kMatchColumnAndValue      // matched cf and qu and the value of them
  };

 private:
  std::string column_family_;
  std::string column_qualifier_;
  CompareOperator op_;
  FilterComparatorPtr comparator_;
  bool filter_if_missing_;
  MatchStatus match_status_;
};

}  // namesapce filter
}  // namesapce tera
