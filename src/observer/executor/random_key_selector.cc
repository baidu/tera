// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/executor/random_key_selector.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "types.h"

DECLARE_string(flagfile);
DECLARE_int64(observer_update_table_info_period_s);

namespace tera {
namespace observer {

RandomKeySelector::RandomKeySelector()
    : tables_(new std::map<std::string, std::vector<tera::TabletInfo>>),
      quit_(false),
      cond_(&quit_mutex_) {
  tera::ErrorCode err;
  client_ = tera::Client::NewClient(FLAGS_flagfile, &err);
  update_thread_ = std::thread{&RandomKeySelector::Update, this};
}

RandomKeySelector::~RandomKeySelector() {
  {
    MutexLock locker(&quit_mutex_);
    quit_ = true;
    cond_.Broadcast();
  }

  update_thread_.join();
  if (client_ != NULL) {
    delete client_;
  }
}

bool RandomKeySelector::SelectRange(std::string* table_name, std::string* start_key,
                                    std::string* end_key) {
  srand((unsigned)time(NULL));

  std::shared_ptr<std::map<std::string, std::vector<tera::TabletInfo>>> table_read_copy;
  {
    MutexLock locker(&table_mutex_);
    // copy for copy-on-write， ref +1
    table_read_copy = tables_;
  }

  if (table_read_copy->size() == 0) {
    return false;
  }

  // random table
  uint32_t table_no = rand() % observe_tables_.size();
  *table_name = observe_tables_[table_no];

  // random key
  size_t tablet_num = (*table_read_copy)[*table_name].size();
  if (0 == tablet_num) {
    LOG(ERROR) << "No tablet";
    return false;
  }

  uint32_t tablet_no = rand() % tablet_num;
  *start_key = (*table_read_copy)[*table_name][tablet_no].start_key;
  *end_key = "";

  VLOG(25) << "Random StartKey=" << *start_key << " TabletNo=" << tablet_no;
  return true;
}

ErrorCode RandomKeySelector::Observe(const std::string& table_name) {
  tera::ErrorCode err;

  MutexLock locker(&table_mutex_);

  if (!tables_.unique()) {
    // In this case threads may reading this copy.
    // Shared_ptr construct a new copy from the original one.
    // Later requests will operate on the new copy.
    tables_.reset(new std::map<std::string, std::vector<tera::TabletInfo>>(*tables_));
  }
  if (tables_->find(table_name) == tables_->end()) {
    std::vector<tera::TabletInfo> tablets;
    client_->GetTabletLocation(table_name, &tablets, &err);
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "Observe table failed, " << err.ToString();
      return err;
    }
    observe_tables_.push_back(table_name);
    (*tables_)[table_name] = tablets;
  }
  return err;
}

void RandomKeySelector::Update() {
  tera::ErrorCode err;
  while (true) {
    {
      MutexLock locker(&quit_mutex_);
      if (quit_) {
        return;
      }
      cond_.TimeWaitInUs(FLAGS_observer_update_table_info_period_s * 1000000);
    }

    // update data first
    std::shared_ptr<std::map<std::string, std::vector<tera::TabletInfo>>> table_update_copy(
        new std::map<std::string, std::vector<tera::TabletInfo>>);

    // updated table
    for (uint32_t i = 0; i < observe_tables_.size(); ++i) {
      std::string table_name = observe_tables_[i];

      std::vector<tera::TabletInfo> tablets;
      client_->GetTabletLocation(table_name, &tablets, &err);
      if (tera::ErrorCode::kOK != err.GetType()) {
        LOG(ERROR) << "Update table info failed, tablename:" << table_name
                   << " err:" << err.ToString();
        continue;
      }

      table_update_copy->insert(
          std::pair<std::string, std::vector<tera::TabletInfo>>(table_name, tablets));
    }

    // update pointer
    MutexLock locker(&table_mutex_);
    tables_.swap(table_update_copy);
  }
}

}  // namespace observer
}  // namespace tera
