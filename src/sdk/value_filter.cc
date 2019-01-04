// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>
#include <memory>
#include "tera.h"
#include "proto/filter.pb.h"
#include "filter_utils.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace tera {
namespace filter {

ValueFilter::ValueFilter()
    : op_(CompareOperator::kNoOp), filter_if_missing_(false), match_status_(kNotMatchAnything) {}

ValueFilter::ValueFilter(CompareOperator op, const FilterComparatorPtr& comparator)
    : column_family_(""),
      column_qualifier_(""),
      op_(CompareOperator::kNoOp),
      comparator_(comparator),
      filter_if_missing_(false),
      match_status_(kNotMatchAnything) {
  if (op == CompareOperator::kLess || op == CompareOperator::kLessOrEqual ||
      op == CompareOperator::kEqual || op == CompareOperator::kNotEqual ||
      op == CompareOperator::kGreaterOrEqual || op == CompareOperator::kGreater) {
    op_ = op;
  }
}
ValueFilter::~ValueFilter() {}

FilterType ValueFilter::Type() { return kValueFilter; }

void ValueFilter::Reset() { match_status_ = kNotMatchAnything; }

bool ValueFilter::MatchOp(int compare_result) {
  switch (op_) {
    case CompareOperator::kLess:
      return compare_result < 0;
    case CompareOperator::kLessOrEqual:
      return compare_result <= 0;
    case CompareOperator::kEqual:
      return compare_result == 0;
    case CompareOperator::kNotEqual:
      return compare_result != 0;
    case CompareOperator::kGreaterOrEqual:
      return compare_result >= 0;
    case CompareOperator::kGreater:
      return compare_result > 0;
    default:
      LOG(ERROR) << "not support CompareOperator";
      return false;
  }
}

bool ValueFilter::MatchValue(const std::string& value) {
  int compare_result = comparator_->CompareWith(value);
  return MatchOp(compare_result);
}

Filter::ReturnCode ValueFilter::FilterCell(const std::string& column_family,
                                           const std::string& column_qualifier,
                                           const std::string& value) {
  /*
   * The behavior of ValueFilter is different then qu is empty or not.
   */
  if (column_qualifier_.empty()) {
    /*
     * The behavior for empty qu is compatible with old filter. The FilterCell will compare
     * the value of each qu for the specified cf till the condition is not satisfied.
     */
    return FilterCellWithEmptyQualifier(column_family, column_qualifier, value);
  } else {
    /*
     * When the qu is specified, not empty. The FilterCell just does the comparation for the value
     * of the specified cf and qu.
     */
    return FilterCellWithNotEmptyQualifier(column_family, column_qualifier, value);
  }
}

Filter::ReturnCode ValueFilter::FilterCellWithEmptyQualifier(const std::string& column_family,
                                                             const std::string& column_qualifier,
                                                             const std::string& value) {
  if (match_status_ == kMatchColumnButNotValue) {
    return kNotIncludeCurAndLeftCellOfRow;
  }
  if (column_family != column_family_) {
    return kIncludeCurCell;
  }
  if (MatchValue(value)) {
    match_status_ = kMatchColumnAndValue;
    return kIncludeCurCell;
  } else {
    match_status_ = kMatchColumnButNotValue;
    return kNotIncludeCurAndLeftCellOfRow;
  }
}

Filter::ReturnCode ValueFilter::FilterCellWithNotEmptyQualifier(const std::string& column_family,
                                                                const std::string& column_qualifier,
                                                                const std::string& value) {
  if (match_status_ == kMatchColumnAndValue) {
    return kIncludeCurCell;
  } else if (match_status_ == kMatchColumnButNotValue) {
    return kNotIncludeCurAndLeftCellOfRow;
  }
  if (column_family != column_family_ || column_qualifier != column_qualifier_) {
    return kIncludeCurCell;
  }
  if (MatchValue(value)) {
    match_status_ = kMatchColumnAndValue;
    return kIncludeCurCell;
  } else {
    match_status_ = kMatchColumnButNotValue;
    return kNotIncludeCurAndLeftCellOfRow;
  }
}

bool ValueFilter::FilterRow() {
  if (match_status_ == kNotMatchAnything) {
    return filter_if_missing_;
  } else if (match_status_ == kMatchColumnButNotValue) {
    return true;
  } else {
    return false;
  }
}

bool ValueFilter::SerializeTo(std::string* serialized_filter) {
  ValueFilterDesc value_filter_desc;
  value_filter_desc.set_column_family(column_family_);
  value_filter_desc.set_column_qualifier(column_qualifier_);
  if (op_ == CompareOperator::kNoOp) {
    LOG(ERROR) << "not support CompareOperator";
    return false;
  }
  value_filter_desc.set_compare_op(TransCompareOperator(op_));
  ComparatorDesc* comparator_desc = new ComparatorDesc();
  ComparatorDesc::ComparatorType comp_type = TransComparatorType(comparator_->Type());
  if (comp_type == ComparatorDesc::kUnknownComparator) {
    return false;
  }
  comparator_desc->set_type(comp_type);
  int ret = comparator_->SerializeTo(comparator_desc->mutable_serialized_comparator());
  if (!ret) {
    return false;
  }
  value_filter_desc.set_allocated_comparator(comparator_desc);
  value_filter_desc.set_filter_if_missing(filter_if_missing_);
  return value_filter_desc.SerializeToString(serialized_filter);
}

bool ValueFilter::ParseFrom(const std::string& serialized_filter) {
  ValueFilterDesc value_filter_desc;
  int ret = value_filter_desc.ParseFromString(serialized_filter);
  if (!ret) {
    return false;
  }
  column_family_ = value_filter_desc.column_family();
  column_qualifier_ = value_filter_desc.column_qualifier();
  op_ = TransCompareType(value_filter_desc.compare_op());
  if (op_ == CompareOperator::kNoOp) {
    LOG(ERROR) << "not support CompareOperator";
    return false;
  }
  switch (value_filter_desc.comparator().type()) {
    case ComparatorDesc::kIntegerComparator:
      comparator_ = std::make_shared<IntegerComparator>();
      break;
    case ComparatorDesc::kDecimalComparator:
      comparator_ = std::make_shared<DecimalComparator>();
      break;
    case ComparatorDesc::kBinaryComparator:
      comparator_ = std::make_shared<BinaryComparator>();
      break;
    default:
      LOG(WARNING) << "not support comparator type";
      return false;
  }
  ret = comparator_->ParseFrom(value_filter_desc.comparator().serialized_comparator());
  if (!ret) {
    return false;
  }
  filter_if_missing_ = value_filter_desc.filter_if_missing();
  return true;
}

void ValueFilter::SetColumnFamily(const std::string& column_family) {
  column_family_ = column_family;
}

void ValueFilter::SetColumnQualifier(const std::string& column_qualifier) {
  column_qualifier_ = column_qualifier;
}

void ValueFilter::SetFilterIfMissing(bool filter_if_missing) {
  filter_if_missing_ = filter_if_missing;
}

void ValueFilter::GetAllColumn(ColumnSet* column_set) {
  column_set->insert(std::make_pair(column_family_, column_qualifier_));
}

}  // namesapce filter
}  // namesapce tera
