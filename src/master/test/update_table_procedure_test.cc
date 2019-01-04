// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/filename.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "io/utils_leveldb.h"
#include "master/update_table_procedure.h"
#include "master/master_env.h"

namespace tera {
namespace master {
namespace test {

class MockClosure : public google::protobuf::Closure {
 public:
  virtual void Run() { return; }
};

class UpdateTableProcedureTest : public ::testing::Test {
 public:
  UpdateTableProcedureTest()
      : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)),
        proc_executor_(new ProcedureExecutor) {}
  virtual ~UpdateTableProcedureTest() {}

  virtual void SetUp() {
    TableSchema schema;
    table_ = TabletManager::CreateTable("test", schema, kTableEnable);
    EXPECT_TRUE(table_);

    StatusCode code;
    EXPECT_TRUE(tablet_manager_->AddTable(table_, &code));
    InitMasterEnv();
    request_.reset(new UpdateTableRequest);
    response_.reset(new UpdateTableResponse);
    closure_.reset(new MockClosure);
    table_->LockTransition();
    update_proc_.reset(
        new UpdateTableProcedure(table_, request_.get(), response_.get(), closure_.get(), nullptr));
  }

 private:
  void InitMasterEnv() {
    MasterEnv().Init(nullptr, nullptr, tablet_manager_, access_builder_, nullptr, nullptr, nullptr,
                     nullptr, proc_executor_, nullptr, nullptr);
  }

 private:
  TablePtr table_;
  std::shared_ptr<TabletManager> tablet_manager_;
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::unique_ptr<UpdateTableRequest> request_;
  std::unique_ptr<UpdateTableResponse> response_;
  std::unique_ptr<MockClosure> closure_;
  std::shared_ptr<UpdateTableProcedure> update_proc_;
  std::shared_ptr<ProcedureExecutor> proc_executor_;
};

TEST_F(UpdateTableProcedureTest, NoConcurrentUpdate) {
  EXPECT_TRUE(table_->InTransition());
  EXPECT_FALSE(table_->LockTransition());
  update_proc_->EofPhaseHandler(UpdateTablePhase::kEofPhase);
  EXPECT_FALSE(table_->InTransition());
  EXPECT_TRUE(table_->LockTransition());
}
}
}
}
