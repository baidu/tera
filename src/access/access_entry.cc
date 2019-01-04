// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access_entry.h"

namespace tera {
namespace auth {

AccessEntry::AccessEntry(const std::string& auth_policy) : auth_policy_(auth_policy) {
  if (!AccessUtils::GetAuthPolicyType(auth_policy, &auth_policy_type_)) {
    LOG(ERROR) << "Unimplemented auth policy " << auth_policy
               << ", AccessEntry construct failed, exit!";
    _Exit(EXIT_FAILURE);
  }
  access_updater_.reset(new AccessUpdater(auth_policy_type_));
}

bool AccessEntry::Verify(const IdentityInfo& identity_info, RoleList* roles) {
  if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy) {
    return true;
  }
  VerificationPtr verification = access_updater_->GetVerification();
  return verification->Verify(identity_info, roles);
}

bool AccessEntry::Authorize(const RoleList& roles) {
  if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy) {
    return true;
  }
  AuthorizationPtr authorization = access_updater_->GetAuthorization();
  return authorization->Authorize(roles);
}
}
}
