// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_FILTER_UTILS_H_
#define TERA_SDK_FILTER_UTILS_H_

#include "proto/tabletnode_rpc.pb.h"
#include "proto/filter.pb.h"
#include "tera.h"

using std::string;

namespace tera {

bool CheckFilterString(const string& filter_str);
string RemoveInvisibleChar(const string& schema);

bool TransBinCompOp(BinCompOp bin_comp_op, filter::CompareOperator* op);

namespace filter {
CompareType TransCompareOperator(CompareOperator op);
CompareOperator TransCompareType(CompareType op);

FilterDesc::FilterType TransFilterType(FilterType type);
ComparatorDesc::ComparatorType TransComparatorType(ComparatorType type);

FilterValueType TransIntegerValueType(IntegerValueType type);
IntegerValueType TransFilterValueType(FilterValueType type);

FilterListDesc::Operator TransFilterListOp(FilterList::Operator op);
FilterList::Operator TransFilterListDescOp(FilterListDesc::Operator op);

}  // namespace filter
}  // namespace tera
#endif  // TERA_SDK_FILTER_UTILS_H_
