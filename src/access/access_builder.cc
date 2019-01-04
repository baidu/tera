// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/access_builder.h"
#include "access/identification/ugi_identification.h"
#include "access/giano/giano_identification.h"
#include "utils/utils_cmd.h"
#include <glog/logging.h>
#include <mutex>

namespace tera {
namespace auth {

AccessBuilder::AccessBuilder(const std::string& auth_policy) : auth_policy_(auth_policy) {
  if (!AccessUtils::GetAuthPolicyType(auth_policy, &auth_policy_type_)) {
    LOG(ERROR) << "Unimplemented auth policy " << auth_policy_
               << ", AccessBuilder construct failed, exit!";
    _Exit(EXIT_FAILURE);
  }
  identity_info_.set_auth_policy_type(auth_policy_type_);
  identity_info_.set_ip_addr(utils::GetLocalHostAddr());
  if (auth_policy_type_ == AuthPolicyType::kGianoAuthPolicy) {
    identification_.reset(new GianoIdentification());
  } else if (auth_policy_type_ == AuthPolicyType::kUgiAuthPolicy) {
    identification_.reset(new UgiIdentification());
  } else if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy) {
    LOG(INFO) << "AccessBuilder auth_policy kNoneAuthPolicy";
  } else {
    LOG(ERROR) << "Unexpected error"
               << ", AccessBuilder construct failed, exit!";
    _Exit(EXIT_FAILURE);
  }
}

bool AccessBuilder::Login(const std::string& name, const std::string& token,
                          ErrorCode* const error_code) {
  if (auth_policy_type_ == AuthPolicyType::kNoneAuthPolicy || name == kInternalGroup) {
    return true;
  }
  bool ret = true;
  identity_info_.set_name(name);
  identity_info_.set_token(token);
  ret = identification_->Login(&identity_info_, error_code);
  return ret;
}
}
}
