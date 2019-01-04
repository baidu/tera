// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/access_control.pb.h"
#include "master/update_auth_procedure.h"
#include "master/master_env.h"
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "access/helpers/access_utils.h"

namespace tera {
namespace master {
namespace test {

class MockClosure : public google::protobuf::Closure {
 public:
  virtual void Run() { return; }
};

static const std::string user_name("tera_dev");
static const std::string passwd("qwer1234");

class UpdateUgiProcedureTest : public ::testing::Test {
 public:
  UpdateUgiProcedureTest()
      : request_(new UpdateUgiRequest),
        response_(new UpdateUgiResponse),
        proc_executor_(new ProcedureExecutor) {
    std::string ugi_auth_policy;
    auth::AccessUtils::GetAuthPolicy(AuthPolicyType::kUgiAuthPolicy, &ugi_auth_policy);
    access_entry_.reset(new auth::AccessEntry(ugi_auth_policy));
  }
  virtual ~UpdateUgiProcedureTest() {}

  virtual void SetUp() {
    InitMasterEnv();

    UpdateAuthInfo* update_auth_info = request_->mutable_update_info();
    UgiInfo* ugi_info = update_auth_info->mutable_ugi_info();
    ugi_info->set_user_name(user_name);
    ugi_info->set_passwd(passwd);
    update_auth_info->set_update_type(UpdateAuthType::kUpdateUgi);

    std::unique_ptr<MetaWriteRecord> meta_write_record(
        auth::AccessUtils::NewMetaRecord(access_entry_, *update_auth_info));
    update_proc_.reset(new UpdateAuthProcedure<UpdateUgiPair>(
        request_.get(), response_.get(), closure_.get(), MasterEnv().GetThreadPool().get(),
        access_entry_, meta_write_record, auth::AccessUpdateType::UpdateUgi));
  }

  virtual void TearDown() { proc_executor_->running_ = false; }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  void InitMasterEnv() {
    MasterEnv().Init(nullptr, nullptr, nullptr, access_builder_, nullptr, nullptr, nullptr,
                     std::shared_ptr<ThreadPool>(new ThreadPool), proc_executor_, nullptr, nullptr);
  }

 private:
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::shared_ptr<auth::AccessEntry> access_entry_;
  std::unique_ptr<UpdateUgiRequest> request_;
  std::unique_ptr<UpdateUgiResponse> response_;
  std::unique_ptr<MockClosure> closure_;
  std::shared_ptr<UpdateAuthProcedure<UpdateUgiPair>> update_proc_;
  std::shared_ptr<ProcedureExecutor> proc_executor_;
};

TEST_F(UpdateUgiProcedureTest, CheckUpdateUgiProcedure) {
  EXPECT_EQ(update_proc_->ProcId(), "UpdateUgi:" + user_name);
  std::string m_user_name =
      auth::AccessUtils::GetNameFromMetaKey(update_proc_->meta_write_record_->key);
  EXPECT_TRUE(!m_user_name.empty());
  EXPECT_EQ(m_user_name, user_name);
}

TEST_F(UpdateUgiProcedureTest, UpdateUgiProcedureUpdateMetaFlase) {
  update_proc_->UpdateMetaDone(false);
  EXPECT_EQ(response_->status(), kMetaTabletError);
}

TEST_F(UpdateUgiProcedureTest, UpdateUgiProcedureUpdateMetaTrue) {
  update_proc_->UpdateMetaDone(true);
  EXPECT_EQ(response_->status(), kMasterOk);
}
}
}
}
