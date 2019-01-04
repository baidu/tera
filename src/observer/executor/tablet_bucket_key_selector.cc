// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/executor/tablet_bucket_key_selector.h"

#include <cmath>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "types.h"

DECLARE_string(flagfile);

namespace tera {
namespace observer {

TabletBucketKeySelector::TabletBucketKeySelector(int32_t bucket_id, int32_t bucket_cnt)
    : tables_(new std::map<std::string, std::vector<tera::TabletInfo>>),
      bucket_id_(bucket_id),
      bucket_cnt_(bucket_cnt) {
  tera::ErrorCode err;
  client_ = tera::Client::NewClient(FLAGS_flagfile, &err);
}

TabletBucketKeySelector::~TabletBucketKeySelector() {
  if (client_ != NULL) {
    delete client_;
  }
}

bool TabletBucketKeySelector::SelectRange(std::string* table_name, std::string* start_key,
                                          std::string* end_key) {
  LOG(INFO) << "select range";
  srand((unsigned)time(NULL));

  if (tables_->size() == 0) {
    return false;
  }

  // random table
  uint32_t table_no = rand() % observe_tables_.size();
  *table_name = observe_tables_[table_no];

  // random key
  size_t tablet_num = (*tables_)[*table_name].size();
  if (0 == tablet_num) {
    LOG(ERROR) << "No tablet";
    return false;
  }

  uint32_t start_tablet_no, end_tablet_no;
  int bucket_size = tablet_num / bucket_cnt_;
  int remainder = tablet_num % bucket_cnt_;
  bucket_size = bucket_id_ < remainder ? bucket_size + 1 : bucket_size;
  start_tablet_no = bucket_size * bucket_id_ + (bucket_id_ < remainder ? 0 : remainder);
  if (start_tablet_no > tablet_num - 1) {
    VLOG(13) << "this bucket_id[ " << bucket_id_ << " ] not cover this bucket."
             << " bucket_cnt[ " << bucket_cnt_ << " ]";
    return false;
  }
  *start_key = (*tables_)[*table_name][start_tablet_no].start_key;
  end_tablet_no = start_tablet_no + bucket_size;  // can't reach this tablet
  if (end_tablet_no >= tablet_num - 1) {
    *end_key = "";
    end_tablet_no = tablet_num;
  } else {
    *end_key = (*tables_)[*table_name][end_tablet_no].start_key;
  }

  VLOG(13) << "Select Range=[" << *start_key << " .. " << *end_key << ") TabletRange=["
           << start_tablet_no << ", " << end_tablet_no << "]";
  return true;
}

ErrorCode TabletBucketKeySelector::Observe(const std::string& table_name) {
  LOG(INFO) << "tablet bucket key selector observe";
  tera::ErrorCode err;

  if (tables_->find(table_name) == tables_->end()) {
    std::vector<tera::TabletInfo> tablets;
    client_->GetTabletLocation(table_name, &tablets, &err);
    LOG(ERROR) << "find tablet count = " << tablets.size();
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "Observe table failed, " << err.ToString();
      return err;
    }
    observe_tables_.push_back(table_name);
    (*tables_)[table_name] = tablets;
  }
  return err;
}

}  // namespace observer
}  // namespace tera
