// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <set>
#include <string>
#include <vector>
#include "common/mutex.h"

namespace tera {
namespace auth {

class Authorization {
 public:
  explicit Authorization() {}
  virtual ~Authorization() {}
  void Update(const std::string& role_name);
  void Delete(const std::string& role_name);
  void GetAll(std::set<std::string>* role_list);
  bool Authorize(const std::set<std::string>& role_list);

 private:
  std::set<std::string> roles_;
  mutable Mutex mutex_;
};
}
}
