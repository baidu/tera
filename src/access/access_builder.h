// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <memory>
#include "tera/error_code.h"
#include "access/identification/identification.h"
#include "access/helpers/access_utils.h"

namespace tera {
namespace auth {
class AccessBuilder {
 public:
  // Provide the object for client related with access control
  explicit AccessBuilder(const std::string& auth_policy);
  virtual ~AccessBuilder() {}

  // Get the cred/password and save them to user
  // Return false if any error happend
  // Only support for one group login in one shot
  // TODO: extend to multi-group login.
  bool Login(const std::string& name, const std::string& token, ErrorCode* const error_code);

  // auth User for Request
  template <class Request>
  bool BuildRequest(Request* const req) {
    if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy) {
      return true;
    }
    IdentityInfo* identity_info = new IdentityInfo();
    identity_info->CopyFrom(identity_info_);
    req->set_allocated_identity_info(identity_info);
    return true;
  }

  // Set InternalGroup for internal Request
  // Work for master => ts such as scan meta_table etc.
  template <class Request>
  void BuildInternalGroupRequest(Request* const req) {
    IdentityInfo* identity_info = new IdentityInfo();
    identity_info->set_name(kInternalGroup);
    identity_info->set_auth_policy_type(AuthPolicyType::kNoneAuthPolicy);
    identity_info->set_token("");
    identity_info->set_ip_addr("");
    req->set_allocated_identity_info(identity_info);
  }

 private:
  std::string auth_policy_;
  AuthPolicyType auth_policy_type_;
  IdentityInfo identity_info_;
  std::unique_ptr<Identification> identification_;
};
}
}
