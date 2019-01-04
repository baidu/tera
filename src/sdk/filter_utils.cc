// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "filter_utils.h"

#include <stdint.h>

#include <sstream>

#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "io/coding.h"
#include "sdk/sdk_utils.h"

namespace tera {

bool CheckFilterString(const string& filter_str) {
  // check filter string syntax
  return true;
}

string RemoveInvisibleChar(const string& schema) {
  string ret;
  for (size_t i = 0; i < schema.size(); ++i) {
    if (schema[i] != '\n' && schema[i] != '\t' && schema[i] != ' ') {
      ret.append(1, schema[i]);
    }
  }
  return ret;
}

bool TransBinCompOp(BinCompOp bin_comp_op, filter::CompareOperator* op) {
  switch (bin_comp_op) {
    case EQ:
      *op = filter::CompareOperator::kEqual;
      return true;
    case NE:
      *op = filter::CompareOperator::kNotEqual;
      return true;
    case LT:
      *op = filter::CompareOperator::kLess;
      return true;
    case LE:
      *op = filter::CompareOperator::kLessOrEqual;
      return true;
    case GT:
      *op = filter::CompareOperator::kGreater;
      return true;
    case GE:
      *op = filter::CompareOperator::kGreaterOrEqual;
      return true;
    default:
      LOG(ERROR) << "not support BinCompOp";
      return false;
  }
}

namespace filter {
CompareType TransCompareOperator(CompareOperator op) {
  switch (op) {
    case CompareOperator::kLess:
      return kLess;
    case CompareOperator::kLessOrEqual:
      return kLessOrEqual;
    case CompareOperator::kEqual:
      return kEqual;
    case CompareOperator::kNotEqual:
      return kNotEqual;
    case CompareOperator::kGreaterOrEqual:
      return kGreaterOrEqual;
    case CompareOperator::kGreater:
      return kGreater;
    default:
      LOG(ERROR) << "not support CompareOperator";
      return kNoOp;
  }
}

CompareOperator TransCompareType(CompareType op) {
  switch (op) {
    case kLess:
      return CompareOperator::kLess;
    case kLessOrEqual:
      return CompareOperator::kLessOrEqual;
    case kEqual:
      return CompareOperator::kEqual;
    case kNotEqual:
      return CompareOperator::kNotEqual;
    case kGreaterOrEqual:
      return CompareOperator::kGreaterOrEqual;
    case kGreater:
      return CompareOperator::kGreater;
    default:
      LOG(ERROR) << "not support CompareType";
      return CompareOperator::kNoOp;
  }
}

FilterDesc::FilterType TransFilterType(FilterType type) {
  switch (type) {
    case kFilterList:
      return FilterDesc::kFilterList;
    case kValueFilter:
      return FilterDesc::kValueFilter;
    default:
      LOG(ERROR) << "not support FilterType";
      return FilterDesc::kUnknownType;
  }
}

ComparatorDesc::ComparatorType TransComparatorType(ComparatorType type) {
  switch (type) {
    case ComparatorType::kIntegerComparator:
      return ComparatorDesc::kIntegerComparator;
    case ComparatorType::kDecimalComparator:
      return ComparatorDesc::kDecimalComparator;
    case ComparatorType::kBinaryComparator:
      return ComparatorDesc::kBinaryComparator;
    default:
      LOG(ERROR) << "not support ComparatorType";
      return ComparatorDesc::kUnknownComparator;
  }
}

FilterValueType TransIntegerValueType(IntegerValueType type) {
  switch (type) {
    case IntegerValueType::kInt64:
      return kINT64;
    case IntegerValueType::kUint64:
      return kUINT64;
    case IntegerValueType::kInt32:
      return kINT32;
    case IntegerValueType::kUint32:
      return kUINT32;
    case IntegerValueType::kInt16:
      return kINT16;
    case IntegerValueType::kUint16:
      return kUINT16;
    case IntegerValueType::kInt8:
      return kINT8;
    case IntegerValueType::kUint8:
      return kUINT8;
    default:
      LOG(ERROR) << "not support IntegerValueType";
      return kUnknownValueType;
  }
}

IntegerValueType TransFilterValueType(FilterValueType type) {
  switch (type) {
    case kINT64:
      return IntegerValueType::kInt64;
    case kUINT64:
      return IntegerValueType::kUint64;
    case kINT32:
      return IntegerValueType::kInt32;
    case kUINT32:
      return IntegerValueType::kUint32;
    case kINT16:
      return IntegerValueType::kInt16;
    case kUINT16:
      return IntegerValueType::kUint16;
    case kINT8:
      return IntegerValueType::kInt8;
    case kUINT8:
      return IntegerValueType::kUint8;
    default:
      LOG(ERROR) << "not support FilterValueType";
      return IntegerValueType::kUnknown;
  }
}

FilterListDesc::Operator TransFilterListOp(FilterList::Operator op) {
  switch (op) {
    case FilterList::kAnd:
      return FilterListDesc::kAnd;
    case FilterList::kOr:
      return FilterListDesc::kOr;
    default:
      LOG(ERROR) << "not support Operator in FilterList";
      return FilterListDesc::kInvalidOp;
  }
}

FilterList::Operator TransFilterListDescOp(FilterListDesc::Operator op) {
  switch (op) {
    case FilterListDesc::kAnd:
      return FilterList::kAnd;
    case FilterListDesc::kOr:
      return FilterList::kOr;
    default:
      LOG(ERROR) << "not support Operator in FilterListDesc";
      return FilterList::kInvalidOp;
  }
}

}  // namespace filter
}  // namespace tera
