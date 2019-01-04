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
#include "sdk/filter_utils.h"
#include "sdk/filter_list_with_and.h"
#include "sdk/filter_list_with_or.h"
#include "glog/logging.h"

namespace tera {
namespace filter {

FilterList::FilterList(Operator op) : op_(kInvalidOp), filter_list_base_(NULL) {
  if (op == kAnd) {
    filter_list_base_ = new FilterListWithAND();
    op_ = op;
  } else if (op == kOr) {
    filter_list_base_ = new FilterListWithOR();
    op_ = op;
  }
}

FilterList::FilterList() {}

FilterList::~FilterList() {
  if (filter_list_base_) {
    delete filter_list_base_;
  }
}

bool FilterList::AddFilter(const FilterPtr& filter) {
  if (filter_list_base_) {
    filter_list_base_->AddFilter(filter);
    return true;
  } else {
    return false;
  }
}

FilterType FilterList::Type() { return kFilterList; }

void FilterList::Reset() {
  if (filter_list_base_) {
    filter_list_base_->Reset();
  }
}

Filter::ReturnCode FilterList::FilterCell(const std::string& column_family,
                                          const std::string& column_qualifier,
                                          const std::string& value) {
  if (!filter_list_base_) {
    return kUndefinedRC;
  }
  return filter_list_base_->FilterCell(column_family, column_qualifier, value);
}

bool FilterList::FilterRow() {
  if (!filter_list_base_) {
    return false;
  }
  return filter_list_base_->FilterRow();
}

bool FilterList::SerializeTo(std::string* serialized_filter) {
  FilterListDesc filter_list_desc;
  FilterListDesc::Operator op = TransFilterListOp(op_);
  if (op == FilterListDesc::kInvalidOp) {
    return false;
  }
  filter_list_desc.set_op(op);
  if (!filter_list_base_) {
    return false;
  }
  const std::vector<FilterPtr>& filters = filter_list_base_->GetFilters();
  for (auto it = filters.begin(); it != filters.end(); ++it) {
    FilterPtr filter = *it;
    FilterDesc* filter_desc = filter_list_desc.add_filters();
    FilterDesc::FilterType filter_type = TransFilterType(filter->Type());
    if (filter_type == FilterDesc::kUnknownType) {
      return false;
    }
    filter_desc->set_type(filter_type);
    int ret = filter->SerializeTo(filter_desc->mutable_serialized_filter());
    if (!ret) {
      return false;
    }
  }
  return filter_list_desc.SerializeToString(serialized_filter);
}

bool FilterList::ParseFrom(const std::string& serialized_filter) {
  FilterListDesc filter_list_desc;
  int ret = filter_list_desc.ParseFromString(serialized_filter);
  if (!ret) {
    LOG(ERROR) << "filter_list ParseFromString failed";
    return false;
  }
  op_ = TransFilterListDescOp(filter_list_desc.op());
  if (op_ == kAnd) {
    filter_list_base_ = new FilterListWithAND();
  } else if (op_ == kOr) {
    filter_list_base_ = new FilterListWithOR();
  } else {
    LOG(ERROR) << "not support Operator";
    return false;
  }
  for (int i = 0; i < filter_list_desc.filters_size(); ++i) {
    const FilterDesc& filter_desc = filter_list_desc.filters(i);
    std::shared_ptr<Filter> filter;
    switch (filter_desc.type()) {
      case FilterDesc::kFilterList:
        filter = std::make_shared<FilterList>();
        break;
      case FilterDesc::kValueFilter:
        filter = std::make_shared<ValueFilter>();
        break;
      default:
        filter.reset();
        break;
    }
    if (filter) {
      ret = filter->ParseFrom(filter_desc.serialized_filter());
      if (ret) {
        filter_list_base_->AddFilter(filter);
      } else {
        LOG(ERROR) << "filter_list ParseFrom sub_filter " << i << " failed";
        return false;
      }
    }
  }
  return true;
}

void FilterList::GetAllColumn(ColumnSet* filter_column_set) {
  if (filter_list_base_) {
    const std::vector<FilterPtr>& filters = filter_list_base_->GetFilters();
    for (auto it = filters.begin(); it != filters.end(); ++it) {
      FilterPtr filter = *it;
      filter->GetAllColumn(filter_column_set);
    }
  }
}

}  // namesapce filter
}  // namesapce tera
