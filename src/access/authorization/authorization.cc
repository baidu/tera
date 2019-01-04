// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/authorization/authorization.h"

namespace tera {
namespace auth {

void Authorization::Update(const std::string& role_name) {
  MutexLock l(&mutex_);
  roles_.emplace(role_name);
}

void Authorization::Delete(const std::string& role_name) {
  MutexLock l(&mutex_);
  auto it = roles_.find(role_name);
  if (it != roles_.end()) {
    roles_.erase(it);
  }
}

void Authorization::GetAll(std::set<std::string>* role_list) {
  MutexLock l(&mutex_);
  for (auto& role : roles_) {
    role_list->emplace(role);
  }
}

bool Authorization::Authorize(const std::set<std::string>& role_list) {
  MutexLock l(&mutex_);
  for (const auto& role : role_list) {
    auto it = roles_.find(role);
    if (it != roles_.end()) {
      return true;
    }
  }
  return false;
}
}
}
