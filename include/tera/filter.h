// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#pragma once

#include <string>
#include <set>
#include <memory>
#include "tera/error_code.h"

namespace tera {
namespace filter {

/*
 * How to use filter:
 * 1. New a concrete filter object. A concrete filter may be a value filter or a filter list,
 *    please refer to value_filter.h and filter_list.h
 * 2. Set the filter to the ScanDescriptor by using SetFilter method.
 * 3. Then the filter will work when scanning.
 */

enum FilterType {
  kFilterList,      // filter list type
  kValueFilter,     // value filter type
  kUnDefinedFilter  // undefined type
};

class Filter;

using FilterPtr = std::shared_ptr<Filter>;

using ColumnPair = std::pair<std::string, std::string>;
using ColumnSet = std::set<ColumnPair>;

/*
 * User do NOT need to use this class.
 * This is a base filter class, all filter classes inherit from this class.
 * The internal of tera will use this class.
 */
class Filter {
 public:
  enum ReturnCode {
    kIncludeCurCell,                 // current cell included, user can use filter for next cell
    kNotIncludeCurAndLeftCellOfRow,  // current cell not included, left cells of current row also
                                     // not included, user can use filter for next row
    kUndefinedRC                     // invalid
  };

 public:
  /*
   * before filter one row, this method must be used to clean or reinitialize the env of the filter.
   * if not do this, the filter will do the wrong behavior for the row.
   */
  virtual void Reset() = 0;

  /*
   * for each cell, all filters will use this method, do the real filter behavior, and set some
   * internal member of the filter. The ReturnCode will specify that how the filter will travel
   * in the current row: the filter can finish travel this row, or travel to the next cell of
   * this row for example.
   */
  virtual ReturnCode FilterCell(const std::string& column_family,
                                const std::string& column_qualifier, const std::string& value) = 0;

  /*
   * for each row, after using FilterCell for each cell of the row, in the end, this method will be
   * used to justify whether this row should be filtered. Return true if this row should be
   * filtered, that means not output in scanning or reading, or return false.
   */
  virtual bool FilterRow() = 0;

  /*
   * these methods below are used in transmitting filters from sdk to tabletserver
   */
  virtual FilterType Type() = 0;
  virtual bool SerializeTo(std::string* serialized_filter) = 0;
  virtual bool ParseFrom(const std::string& serialized_filter) = 0;
  virtual void GetAllColumn(ColumnSet* filter_column_set) = 0;
};

class FilterBase : public Filter {
 public:
  FilterBase();
  virtual ~FilterBase();
  virtual void Reset();
  virtual ReturnCode FilterCell(const std::string& column_family,
                                const std::string& column_qualifier, const std::string& value);
  virtual bool FilterRow();
  virtual FilterType Type();
  virtual bool SerializeTo(std::string* serialized_filter);
  virtual bool ParseFrom(const std::string& serialized_filter);
  virtual void GetAllColumn(ColumnSet* filter_column_set);
};

}  // namespace filter
}  // namesapce tera
