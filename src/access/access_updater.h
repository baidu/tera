// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <memory>
#include "common/mutex.h"
#include "access/helpers/version_recorder.h"
#include "access/verification/verification.h"
#include "access/authorization/authorization.h"
#include "proto/master_rpc.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {

class QueryRequest;
class QueryResponse;
class ShowUgiResponse;

namespace auth {

using VerificationPtr = std::shared_ptr<Verification>;
using AuthorizationPtr = std::shared_ptr<Authorization>;

class AccessUpdater {
 public:
  explicit AccessUpdater(AuthPolicyType auth_policy_type);
  virtual ~AccessUpdater() {}
  AccessUpdater(AccessUpdater&) = delete;
  AccessUpdater& operator=(const AccessUpdater&) = delete;

  VerificationPtr GetVerification() {
    MutexLock l(&mutex_);
    return verification_;
  }

  AuthorizationPtr GetAuthorization() {
    MutexLock l(&mutex_);
    return authorization_;
  }

  void SyncUgiVersion(bool updated) { ugi_version_recorder_.SetNeedUpdate(updated); }

  // master
  bool AddRecord(const std::string& key, const std::string& value);
  bool DelRecord(const std::string& key);
  bool IsSameVersion(uint64_t version) { return ugi_version_recorder_.IsSameVersion(version); }
  void BuildReq(QueryRequest* request) {
    if (auth_policy_type_ == AuthPolicyType::kUgiAuthPolicy) {
      BuildUgiMetaInfos<QueryRequest>(request);
    }
    BuildRoleInfos<QueryRequest>(request);
  }
  void ShowUgiInfo(ShowUgiResponse* response) { BuildUgiMetaInfos<ShowUgiResponse>(response); }
  void ShowAuthInfo(ShowAuthResponse* response) { BuildRoleInfos<ShowAuthResponse>(response); }

  // ts
  bool UpdateTs(const QueryRequest* request, QueryResponse* response);

 private:
  template <typename Message>
  void BuildUgiMetaInfos(Message* message) {
    UserVerificationInfoList user_verification_info_list;
    verification_->GetAll(&user_verification_info_list);
    for (auto it = user_verification_info_list.begin(); it != user_verification_info_list.end();
         ++it) {
      UgiMetaInfo* ugi_meta_info = message->add_ugi_meta_infos();
      ugi_meta_info->set_user_name(it->first);
      ugi_meta_info->set_passwd(it->second->first);
      for (auto& role : it->second->second) {
        *ugi_meta_info->add_roles() = role;
      }
    }
  }
  template <typename Message>
  void BuildRoleInfos(Message* message) {
    std::set<std::string> role_list;
    authorization_->GetAll(&role_list);
    for (auto& role : role_list) {
      RoleInfo* role_info = message->add_role_infos();
      role_info->set_role(role);
    }
  }

 private:
  AuthPolicyType auth_policy_type_;
  VerificationPtr verification_;
  AuthorizationPtr authorization_;
  mutable Mutex mutex_;

  // master
  VersionRecorder ugi_version_recorder_;
};
}
}
