// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "ugi_verification.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {
namespace auth {

UgiVerification::UgiVerification() {}

bool UgiVerification::Verify(const IdentityInfo& identity_info, RoleList* roles) {
  MutexLock l(&ugi_mutex_);
  auto it = user_verification_info_list_.find(identity_info.name());
  if (it != user_verification_info_list_.end() && it->second->first == identity_info.token()) {
    for (auto& role : it->second->second) {
      roles->emplace(role);
    }
    return true;
  }
  return false;
}

bool UgiVerification::Update(const std::string& user_name,
                             const VerificationInfoPtr& verification_info_ptr) {
  MutexLock l(&ugi_mutex_);
  VLOG(23) << "UpdateUgi success [user = " << user_name
           << ", passwd = " << verification_info_ptr->first << "]";
  user_verification_info_list_[user_name] = verification_info_ptr;
  return true;
}

bool UgiVerification::Get(const std::string& user_name,
                          VerificationInfoPtr* verification_info_ptr) {
  MutexLock l(&ugi_mutex_);
  auto it = user_verification_info_list_.find(user_name);
  if (it == user_verification_info_list_.end()) {
    return false;
  }
  *verification_info_ptr = it->second;
  return true;
}

void UgiVerification::GetAll(UserVerificationInfoList* user_verification_info) {
  MutexLock l(&ugi_mutex_);
  for (auto it = user_verification_info_list_.begin(); it != user_verification_info_list_.end();
       ++it) {
    (*user_verification_info)[it->first] = it->second;
  }
}

bool UgiVerification::Delete(const std::string& user_name) {
  MutexLock l(&ugi_mutex_);
  auto it = user_verification_info_list_.find(user_name);
  if (it != user_verification_info_list_.end()) {
    VLOG(23) << "DelUgi success [user = " << it->first << ", psswd = " << it->second << "]";
    user_verification_info_list_.erase(it);
  }
  return true;
}
}
}