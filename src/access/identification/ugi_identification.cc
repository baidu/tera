// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/identification/ugi_identification.h"
#include <glog/logging.h>

namespace tera {
namespace auth {

UgiIdentification::UgiIdentification() {}

bool UgiIdentification::Login(IdentityInfo* const identity_info, ErrorCode* const error_code) {
  if (identity_info->name() == "" || identity_info->token() == "") {
    LOG(ERROR) << "Make sure set --tera_auth_name=xxx & --tera_auth_token=yyy "
               << "in tera.flag when use kUgiAuthPolicy, [name = " << identity_info->name()
               << ", token = " << identity_info->token();
    if (error_code != nullptr) {
      std::string reason =
          "Login argument absent(--tera_auth_name=xxx && --tera_auth_token=yyy)\
                                  when use kUgiAuthPolicy";
      error_code->SetFailed(ErrorCode::kAuthBadParam, reason);
    }
    return false;
  }
  return true;
}
}
}