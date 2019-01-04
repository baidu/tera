// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>
#include <vector>
#include "access/verification/verification.h"

namespace tera {

namespace master {
struct MetaWriteRecord;
}

namespace auth {

class AccessEntry;

static const std::string kInternalGroup("internal_group");

class AccessUtils {
 public:
  static bool GetAuthPolicy(const AuthPolicyType& auth_policy_type, std::string* const auth_policy);
  static bool GetAuthPolicyType(const std::string& auth_policy,
                                AuthPolicyType* const auth_policy_type);
  static master::MetaWriteRecord* NewMetaRecord(const std::shared_ptr<AccessEntry>& access_entry,
                                                const UpdateAuthInfo& update_auth_info);
  static std::string GetNameFromMetaKey(const std::string& key);
  static VerificationInfoPtr GetVerificationInfoFromMetaValue(const std::string& value);
};
}
}
