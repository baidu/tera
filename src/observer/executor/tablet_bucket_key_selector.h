// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/mutex.h"
#include "observer/executor/key_selector.h"
#include "tera.h"

namespace tera {
namespace observer {

class TabletBucketKeySelector : public KeySelector {
 public:
  TabletBucketKeySelector(int32_t bucket_id, int32_t bucket_cnt);
  virtual ~TabletBucketKeySelector();

  virtual bool SelectRange(std::string* table_name, std::string* start_key, std::string* end_key);

  virtual ErrorCode Observe(const std::string& table_name);

 private:
  tera::Client* client_;
  std::vector<std::string> observe_tables_;
  std::shared_ptr<std::map<std::string, std::vector<tera::TabletInfo>>> tables_;
  int32_t bucket_id_;
  int32_t bucket_cnt_;
};

}  // namespace observer
}  // namespace tera
