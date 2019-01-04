// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <memory>
#include <string>
#include "access/access_builder.h"
#include "access/access_entry.h"
#include "access/verification/ugi_verification.h"
#include "proto/tabletnode_rpc.pb.h"
#include "master/master_env.h"

DECLARE_string(tera_auth_name);
DECLARE_string(tera_auth_token);

namespace tera {
namespace auth {
namespace test {

static const std::string user_name("mock_group");
static const std::string passwd("2862933555777941757");

class AuthVerificationTest : public ::testing::Test {
 public:
  AuthVerificationTest() : meta_write_record_(nullptr) {
    FLAGS_tera_auth_name = user_name;
    FLAGS_tera_auth_token = passwd;
  }

  virtual ~AuthVerificationTest() {}

  void RegisterNoneAuth() {
    std::string none_auth_policy;
    AccessUtils::GetAuthPolicy(AuthPolicyType::kNoneAuthPolicy, &none_auth_policy);
    access_builder_.reset(new AccessBuilder(none_auth_policy));
    access_entry_.reset(new AccessEntry(none_auth_policy));
  }

  void RegisterUgiAuth() {
    std::string ugi_auth_policy;
    AccessUtils::GetAuthPolicy(AuthPolicyType::kUgiAuthPolicy, &ugi_auth_policy);
    access_builder_.reset(new AccessBuilder(ugi_auth_policy));
    access_entry_.reset(new AccessEntry(ugi_auth_policy));

    UpdateAuthInfo update_auth_info;
    update_auth_info.set_update_type(kUpdateUgi);
    UgiInfo* ugi_info = update_auth_info.mutable_ugi_info();
    ugi_info->set_user_name(user_name);
    ugi_info->set_passwd(passwd);

    meta_write_record_.reset(AccessUtils::NewMetaRecord(access_entry_, update_auth_info));
  }

  bool Login(ErrorCode* err = NULL) {
    return access_builder_->Login(FLAGS_tera_auth_name, FLAGS_tera_auth_token, err);
  }

  bool Verify(const IdentityInfo& identity_info, RoleList* roles) {
    return access_entry_->Verify(identity_info, roles);
  }

  IdentityInfo GetIdentityInfo() const { return access_builder_->identity_info_; }

 private:
  std::unique_ptr<AccessBuilder> access_builder_;
  std::shared_ptr<AccessEntry> access_entry_;
  std::unique_ptr<master::MetaWriteRecord> meta_write_record_;
};

TEST_F(AuthVerificationTest, NullAuthVerificationTest) {
  RegisterNoneAuth();
  EXPECT_TRUE(Login());

  IdentityInfo identity_info = GetIdentityInfo();
  RoleList roles;
  EXPECT_TRUE(Verify(identity_info, &roles));
}

TEST_F(AuthVerificationTest, UgiAuthVerificationTest) {
  RegisterUgiAuth();
  EXPECT_TRUE(access_entry_->GetAccessUpdater().auth_policy_type_ ==
              AuthPolicyType::kUgiAuthPolicy);
  EXPECT_TRUE(Login());

  EXPECT_TRUE(access_entry_->GetAccessUpdater().AddRecord(meta_write_record_->key,
                                                          meta_write_record_->value));
  IdentityInfo identity_info = GetIdentityInfo();
  EXPECT_EQ(user_name, identity_info.name());
  EXPECT_EQ(passwd, identity_info.token());

  RoleList roles;
  EXPECT_TRUE(Verify(identity_info, &roles));
}

TEST_F(AuthVerificationTest, UpdateMasterTest) {
  RegisterUgiAuth();
  EXPECT_TRUE(access_entry_->GetAccessUpdater().auth_policy_type_ ==
              AuthPolicyType::kUgiAuthPolicy);
  EXPECT_TRUE(access_entry_->GetAccessUpdater().AddRecord(meta_write_record_->key,
                                                          meta_write_record_->value));

  QueryRequest request;
  access_entry_->GetAccessUpdater().BuildReq(&request);
  // access_entry_->GetAccessUpdater().FinishAccessUpdated();
  // EXPECT_TRUE(access_entry_->GetAccessUpdater().BuildReq(&request));
  // EXPECT_TRUE(request.ugi_infos_size() == 1);
  // const std::string& user_name = request.ugi_infos(0).user_name();
  // const std::string& passwd = request.ugi_infos(0).passwd();
  // EXPECT_EQ("mock_group", user_name);
  // EXPECT_EQ("2862933555777941757", passwd);
}
}
}
}
