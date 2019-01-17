// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <vector>
#include <set>
#include <utility>
#include <string>
#include <memory>
#include "proto/access_control.pb.h"

namespace tera {
namespace auth {

using RoleList = std::set<std::string>;
using VerificationInfo = std::pair<std::string, RoleList>;
using VerificationInfoPtr = std::shared_ptr<VerificationInfo>;
using UserVerificationInfoList = std::map<std::string, VerificationInfoPtr>;

class Verification {
 public:
  // Verify ideantity
  // Return false means to fake ideantity
  virtual bool Verify(const IdentityInfo& identity_info, RoleList* roles) = 0;

  // Update verification infos
  virtual bool Update(const std::string& user_name,
                      const VerificationInfoPtr& verification_info_ptr) = 0;
  virtual bool Get(const std::string& user_name, VerificationInfoPtr* verification_info_ptr) = 0;
  virtual void GetAll(UserVerificationInfoList* user_verification_info) = 0;

  // Delete verification's user
  virtual bool Delete(const std::string& user_name) = 0;
};
}
}
