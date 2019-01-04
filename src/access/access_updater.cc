// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/access_updater.h"
#include "access/giano/giano_verification.h"
#include "access/helpers/access_utils.h"
#include "access/verification/ugi_verification.h"
#include "common/func_scope_guard.h"
#include <glog/logging.h>

namespace tera {
namespace auth {

AccessUpdater::AccessUpdater(AuthPolicyType auth_policy_type)
    : auth_policy_type_(auth_policy_type) {
  if (auth_policy_type_ == AuthPolicyType::kGianoAuthPolicy) {
    verification_.reset(new GianoVerification());
  } else if (auth_policy_type_ == AuthPolicyType::kUgiAuthPolicy) {
    verification_.reset(new UgiVerification());
  } else if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy) {
    LOG(INFO) << "AccessEntry auth_policy kNoneAuthPolicy";

    // support set ugi in kNoneAuthPolicy, otherwise couldn't show all ugi_infos in kNoneAuthPolicy
    verification_.reset(new UgiVerification());
  } else {
    LOG(ERROR) << "Unexpected error,"
               << "AccessEntry construct failed, exit!";
    _Exit(EXIT_FAILURE);
  }
  authorization_.reset(new Authorization);
}

bool AccessUpdater::AddRecord(const std::string& key, const std::string& value) {
  if (key.size() <= 3) {
    return false;
  }

  if (key[2] == '0') {
    if (auth_policy_type_ != AuthPolicyType::kUgiAuthPolicy &&
        auth_policy_type_ != AuthPolicyType::kNoneAuthPolicy) {
      LOG(ERROR) << "Master[auth_policy_type_ = " << auth_policy_type_ << "] mismatch";
      return false;
    }
    std::string user_name = AccessUtils::GetNameFromMetaKey(key);
    if (user_name.empty()) {
      return false;
    }
    VerificationInfoPtr verification_info_ptr =
        AccessUtils::GetVerificationInfoFromMetaValue(value);
    if (!verification_info_ptr) {
      return false;
    }
    verification_->Update(user_name, verification_info_ptr);
  } else if (key[2] == '1') {
    std::string role = AccessUtils::GetNameFromMetaKey(key);
    authorization_->Update(role);
  } else {
    LOG(ERROR) << "Wrong key in AccessUpdater::AddRecord";
    return false;
  }
  ugi_version_recorder_.IncVersion();
  ugi_version_recorder_.SetNeedUpdate(true);
  return true;
}

bool AccessUpdater::DelRecord(const std::string& key) {
  if (key.size() <= 3) {
    return false;
  }

  if (key[2] == '0') {
    if (auth_policy_type_ != AuthPolicyType::kUgiAuthPolicy &&
        auth_policy_type_ != AuthPolicyType::kNoneAuthPolicy) {
      LOG(ERROR) << "Master[auth_policy_type_ = " << auth_policy_type_ << "] mismatch";
      return false;
    }

    std::string user_name = AccessUtils::GetNameFromMetaKey(key);
    if (user_name.empty()) {
      return false;
    }

    verification_->Delete(user_name);
  } else if (key[2] == '1') {
    std::string role = AccessUtils::GetNameFromMetaKey(key);
    authorization_->Delete(role);
  } else {
    LOG(ERROR) << "Wrong key in AccessUpdater::DelRecord";
    return false;
  }
  ugi_version_recorder_.IncVersion();
  ugi_version_recorder_.SetNeedUpdate(true);
  return true;
}

// for ts
bool AccessUpdater::UpdateTs(const QueryRequest* request, QueryResponse* response) {
  FuncScopeGuard on_exit([&] { response->set_version(ugi_version_recorder_.GetVersion()); });
  if (request->has_version() && !IsSameVersion(request->version())) {
    // Only update ugi in kUgiAuthPolicy
    if (auth_policy_type_ == AuthPolicyType::kUgiAuthPolicy) {
      ssize_t ugi_meta_infos_size = request->ugi_meta_infos_size();
      VerificationPtr new_verification(new UgiVerification());
      for (ssize_t i = 0; i < ugi_meta_infos_size; ++i) {
        const UgiMetaInfo& ugi_meta_info = request->ugi_meta_infos(i);
        const std::string& user_name = ugi_meta_info.user_name();
        VerificationInfoPtr verification_info_ptr(new VerificationInfo);
        verification_info_ptr->first = ugi_meta_info.passwd();
        RoleList& roles = verification_info_ptr->second;
        int roles_size = ugi_meta_info.roles_size();
        for (int roles_index = 0; roles_index < roles_size; ++roles_index) {
          roles.emplace(ugi_meta_info.roles(roles_index));
        }
        new_verification->Update(user_name, verification_info_ptr);
      }
      MutexLock l(&mutex_);
      verification_.swap(new_verification);
    }

    AuthorizationPtr new_authorization(new Authorization);
    ssize_t role_infos_size = request->role_infos_size();
    for (ssize_t i = 0; i < role_infos_size; ++i) {
      const RoleInfo& role_info = request->role_infos(i);
      new_authorization->Update(role_info.role());
    }
    {
      MutexLock l(&mutex_);
      authorization_.swap(new_authorization);
    }
    VLOG(23) << "UpdateAuth ts version from " << ugi_version_recorder_.GetVersion() << " to "
             << request->version();
    ugi_version_recorder_.SetVersion(request->version());
    return true;
  }
  return false;
}
}  // namespace auth
}  // namespace tera
