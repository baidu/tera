// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "access/verification/verification.h"

namespace tera {
namespace auth {

class GianoVerification : public Verification {
 public:
  GianoVerification();
  virtual ~GianoVerification() {}
  virtual bool Verify(const IdentityInfo& identity_info, RoleList* roles) override;
  virtual bool Update(const std::string& user_name,
                      const VerificationInfoPtr& verification_info_ptr) override {
    return false;
  }
  virtual bool Get(const std::string& user_name,
                   VerificationInfoPtr* verification_info_ptr) override {
    return false;
  }
  virtual void GetAll(UserVerificationInfoList* user_verification_info) override {
  }
  virtual bool Delete(const std::string& user_name) override {
    return false;
  }
};

} // auth
} // tera
