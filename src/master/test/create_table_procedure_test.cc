// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/filename.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "io/utils_leveldb.h"
#include "master/create_table_procedure.h"
#include "master/master_env.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace master {
namespace test {

class CreateTableProcedureTest : public ::testing::Test {
 public:
  CreateTableProcedureTest()
      : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)),
        proc_executor_(new ProcedureExecutor) {}

  virtual ~CreateTableProcedureTest() {}

  virtual void SetUp() {
    FLAGS_tera_leveldb_env_type.assign("local");
    FLAGS_tera_tabletnode_path_prefix.assign("./create_table_procedure");
    InitMasterEnv();
    TableSchema schema;
    StatusCode ret_code;
    // table_ = TabletManager::CreateTable("test", schema, kTableEnable);
    // EXPECT_TRUE(table_);
    request_.reset(new CreateTableRequest);
    response_.reset(new CreateTableResponse);
    request_->set_table_name("test");
    create_proc_.reset(new CreateTableProcedure(request_.get(), response_.get(), nullptr, nullptr));

    const ::testing::TestInfo* test_case = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string test_name(test_case->name());
    // if (test_name.find("ObsoleteOldTableDir") != std::string::npos) {
    InitFileSystem();
    //}
  }

 private:
  void InitFileSystem() { fs_env_ = io::LeveldbBaseEnv(); }

  void InitMasterEnv() {
    MasterEnv().Init(nullptr, nullptr, tablet_manager_, access_builder_, nullptr, nullptr, nullptr,
                     std::shared_ptr<ThreadPool>(new ThreadPool), proc_executor_,
                     std::shared_ptr<TabletAvailability>(new TabletAvailability(tablet_manager_)),
                     nullptr);
  }

 private:
  TablePtr table_;
  std::unique_ptr<CreateTableRequest> request_;
  std::unique_ptr<CreateTableResponse> response_;
  std::shared_ptr<CreateTableProcedure> create_proc_;
  std::shared_ptr<TabletManager> tablet_manager_;
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::shared_ptr<ProcedureExecutor> proc_executor_;
  leveldb::Env* fs_env_;
};

TEST_F(CreateTableProcedureTest, ObsoleteOldTableDir) {
  std::string table_path = FLAGS_tera_tabletnode_path_prefix + "/test";
  std::string trash_path = FLAGS_tera_tabletnode_path_prefix + "/#trash";
  EXPECT_TRUE(fs_env_->CreateDir(table_path).ok());
  EXPECT_TRUE(fs_env_->CreateDir(trash_path).ok());
  int ret = chmod(trash_path.c_str(), 0600);
  create_proc_->PreCheckHandler(CreateTablePhase::kPrepare);
  EXPECT_EQ(response_->status(), kTableExist);
  chmod(trash_path.c_str(), 0700);
  create_proc_->PreCheckHandler(CreateTablePhase::kPrepare);
  EXPECT_NE(access(table_path.c_str(), F_OK), 0);
}
}
}
}
