// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/quota.pb.h"
#include "quota/master_quota_entry.h"
#include "master/set_quota_procedure.h"
#include "master/master_env.h"
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "quota/helpers/quota_utils.h"

namespace tera {
namespace master {
namespace test {

static const std::string table_name{"test"};

class MockClosure : public google::protobuf::Closure {
 public:
  virtual void Run() { return; }
};

class SetQuotaProcedureTest : public ::testing::Test {
 public:
  SetQuotaProcedureTest()
      : quota_entry_(new quota::MasterQuotaEntry()),
        request_(new SetQuotaRequest),
        response_(new SetQuotaResponse),
        proc_executor_(new ProcedureExecutor) {}
  virtual ~SetQuotaProcedureTest() {}

  virtual void SetUp() {
    InitMasterEnv();

    TableQuota* table_quota = request_->mutable_table_quota();
    table_quota->set_table_name(table_name);
    QuotaInfo* quota_info = table_quota->add_quota_infos();
    quota_info->set_type(kQuotaWriteReqs);
    quota_info->set_limit(100);
    quota_info->set_period(2);

    std::unique_ptr<MetaWriteRecord> meta_write_record(
        quota::MasterQuotaHelper::NewMetaRecordFromQuota(*table_quota));
    set_proc_.reset(new SetQuotaProcedure(request_.get(), response_.get(), closure_.get(),
                                          MasterEnv().GetThreadPool().get(), quota_entry_,
                                          meta_write_record));
  }

  virtual void TearDown() { proc_executor_->running_ = false; }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  void InitMasterEnv() {
    MasterEnv().Init(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
                     std::shared_ptr<ThreadPool>(new ThreadPool), proc_executor_, nullptr, nullptr);
  }

 private:
  std::shared_ptr<quota::MasterQuotaEntry> quota_entry_;
  std::unique_ptr<SetQuotaRequest> request_;
  std::unique_ptr<SetQuotaResponse> response_;
  std::unique_ptr<MockClosure> closure_;
  std::shared_ptr<SetQuotaProcedure> set_proc_;
  std::shared_ptr<ProcedureExecutor> proc_executor_;
};

TEST_F(SetQuotaProcedureTest, CheckSetQuotaProcedure) {
  EXPECT_EQ(set_proc_->ProcId(), "SetQuota:" + table_name);
  std::string m_table_name =
      quota::MasterQuotaHelper::GetTableNameFromMetaKey(set_proc_->meta_write_record_->key);
  EXPECT_TRUE(!m_table_name.empty());
  EXPECT_EQ(m_table_name, table_name);
}

TEST_F(SetQuotaProcedureTest, SetQuotaProcedureUpdateMetaFlase) {
  set_proc_->SetMetaDone(false);
  EXPECT_EQ(response_->status(), kMetaTabletError);
}

TEST_F(SetQuotaProcedureTest, SetQuotaProcedureUpdateMetaTrue) {
  set_proc_->SetMetaDone(true);
  EXPECT_EQ(response_->status(), kMasterOk);
}
}
}
}
