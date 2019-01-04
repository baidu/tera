// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <glog/logging.h>
#include <memory>
#include "access/access_updater.h"
#include "access/helpers/access_utils.h"
#include "access/verification/verification.h"

namespace tera {
namespace auth {

enum class AccessUpdateType { UpdateUgi = 0, UpdateAuth, UpdateMax };

class AccessEntry {
 public:
  // Provide the object for server related with access control.
  explicit AccessEntry(const std::string& auth_policy);
  virtual ~AccessEntry() {}

  AccessUpdater& GetAccessUpdater() { return *access_updater_; }

  // Support for verify identity & check permissions
  template <class Request, class Response>
  bool VerifyAndAuthorize(const Request* const req, Response* res) {
    if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy) {
      return true;
    }

    if (!req->has_identity_info()) {
      // TODO: true just for compatibility with old sdk
      // Will return false in future
      return true;
    }
    IdentityInfo identity_info = req->identity_info();

    // InternalGroup means master=>ts rpc
    if (identity_info.name() == kInternalGroup) {
      return true;
    }

    if (identity_info.auth_policy_type() != auth_policy_type_) {
      std::string policy;
      AccessUtils::GetAuthPolicy(identity_info.auth_policy_type(), &policy);

      std::string auth_policy;
      AccessUtils::GetAuthPolicy(auth_policy_type_, &auth_policy);

      LOG(ERROR) << "Not the same auth policy between sdk[" << policy << "] with master/ts["
                 << auth_policy << "]";
      res->set_status(kMismatchAuthType);
      return false;
    }

    RoleList roles;
    if (!Verify(identity_info, &roles) || !Authorize(roles)) {
      res->set_status(kNotPermission);
      return false;
    }
    return true;
  }

 private:
  bool Verify(const IdentityInfo& identity_info, RoleList* roles);
  bool Authorize(const RoleList& roles);

 private:
  std::string auth_policy_;
  AuthPolicyType auth_policy_type_;
  std::shared_ptr<Verification> verification_;
  std::unique_ptr<AccessUpdater> access_updater_;
};
}
}
