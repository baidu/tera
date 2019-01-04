// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "db/filename.h"
#include "io/utils_leveldb.h"
#include "master/load_tablet_procedure.h"
#include "master/split_tablet_procedure.h"
#include "master/unload_tablet_procedure.h"
#include "master/master_env.h"
#include "master/master_zk_adapter.h"
#include "leveldb/env.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_int32(tera_master_max_split_concurrency);

namespace tera {
namespace master {
namespace test {

using leveldb::EnvOptions;

class SplitTabletProcedureTest : public ::testing::Test {
 public:
  SplitTabletProcedureTest()
      : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)),
        ts_manager_(new TabletNodeManager(nullptr)),
        proc_executor_(new ProcedureExecutor) {}

  virtual ~SplitTabletProcedureTest() {}

  virtual void SetUp() {
    FLAGS_tera_leveldb_env_type.assign("local");
    FLAGS_tera_tabletnode_path_prefix.assign("./split_tablet_procedure/");
    InitMasterEnv();
    TableSchema schema;
    StatusCode ret_code;
    table_ = TabletManager::CreateTable("test", schema, kTableEnable);
    EXPECT_TRUE(table_);
    EXPECT_TRUE(tablet_manager_->AddTable(table_, &ret_code));
    TabletMeta tablet_meta;
    TabletManager::PackTabletMeta(&tablet_meta, "test", "b", "d", "test/tablet00000001", "",
                                  TabletMeta::kTabletOffline, 10);
    tablet_ = table_->AddTablet(tablet_meta, &ret_code);
    EXPECT_TRUE(tablet_);
    node_ = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
    tablet_->SetStatus(TabletMeta::kTabletReady);
    split_proc_.reset(new SplitTabletProcedure(tablet_, MasterEnv().GetThreadPool().get()));
    proc_executor_->running_ = true;
  }
  virtual void TearDown() { proc_executor_->running_ = false; }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  void InitMasterEnv() {
    MasterEnv().Init(nullptr, ts_manager_, tablet_manager_, access_builder_, nullptr, nullptr,
                     nullptr, std::shared_ptr<ThreadPool>(new ThreadPool), proc_executor_,
                     std::shared_ptr<TabletAvailability>(new TabletAvailability(tablet_manager_)),
                     nullptr);
    // push one element to the queue, avoiding call TryMoveTablet while call
    // SuspendMetaOperation
    MasterEnv().meta_task_queue_.push(nullptr);
  }

  void InitFileSystemForSplit() {
    fs_env_ = io::LeveldbBaseEnv();
    std::string table_path = FLAGS_tera_tabletnode_path_prefix + tablet_->GetTableName();
    EXPECT_TRUE(fs_env_->CreateDir(table_path).ok());
    std::string tablet_path = FLAGS_tera_tabletnode_path_prefix + tablet_->GetPath();
    EXPECT_TRUE(fs_env_->CreateDir(tablet_path).ok());
  }

 private:
  TablePtr table_;
  TabletPtr tablet_;
  TabletNodePtr node_;
  std::shared_ptr<SplitTabletProcedure> split_proc_;
  std::shared_ptr<TabletManager> tablet_manager_;
  std::shared_ptr<TabletNodeManager> ts_manager_;
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::shared_ptr<ProcedureExecutor> proc_executor_;
  leveldb::Env* fs_env_;
};

TEST_F(SplitTabletProcedureTest, SplitTabletProcedureInit) {
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kPreSplitTablet);
  std::cout << split_proc_->ProcId() << ", " << split_proc_->phases_.back() << std::endl;
}

TEST_F(SplitTabletProcedureTest, UnloadTabletPhaseHandler) {
  EXPECT_FALSE(split_proc_->unload_proc_);
  split_proc_->UnloadTabletPhaseHandler(SplitTabletPhase::kUnLoadTablet);
  EXPECT_TRUE(split_proc_->unload_proc_);
  EXPECT_FALSE(split_proc_->unload_proc_->Done());
  std::shared_ptr<UnloadTabletProcedure> unload_proc =
      std::dynamic_pointer_cast<UnloadTabletProcedure>(split_proc_->unload_proc_);
  EXPECT_TRUE(unload_proc->is_sub_proc_);
  EXPECT_EQ(proc_executor_->procedures_.size(), 1);
  EXPECT_EQ(proc_executor_->procedures_[1]->proc_, split_proc_->unload_proc_);

  split_proc_->UnloadTabletPhaseHandler(SplitTabletPhase::kUnLoadTablet);
  EXPECT_FALSE(split_proc_->unload_proc_->Done());

  unload_proc->done_ = true;
  tablet_->AssignTabletNode(node_);
  split_proc_->UnloadTabletPhaseHandler(SplitTabletPhase::kUnLoadTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);
  split_proc_->UnloadTabletPhaseHandler(SplitTabletPhase::kUnLoadTablet);
  tablet_->SetStatus(TabletMeta::kTabletOffline);
  split_proc_->UnloadTabletPhaseHandler(SplitTabletPhase::kUnLoadTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kPostUnLoadTablet);
}

TEST_F(SplitTabletProcedureTest, PostUnLoadTabletPhaseHandler) {
  split_proc_->PostUnloadTabletPhaseHandler(SplitTabletPhase::kPostUnLoadTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kFaultRecover);
  InitFileSystemForSplit();
  split_proc_->PostUnloadTabletPhaseHandler(SplitTabletPhase::kPostUnLoadTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kUpdateMeta);

  std::string log_path =
      FLAGS_tera_tabletnode_path_prefix + leveldb::LogHexFileName(tablet_->GetPath(), 123);
  leveldb::WritableFile* log_file;
  EXPECT_TRUE(fs_env_->NewWritableFile(log_path, &log_file, EnvOptions()).ok());
  delete log_file;
  split_proc_->PostUnloadTabletPhaseHandler(SplitTabletPhase::kPostUnLoadTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kFaultRecover);
}

TEST_F(SplitTabletProcedureTest, UpdateMetaPhaseHandler) {
  EXPECT_FALSE(split_proc_->child_tablets_[0]);
  EXPECT_FALSE(split_proc_->child_tablets_[1]);
  split_proc_->split_key_ = "c";
  split_proc_->UpdateMetaPhaseHandler(SplitTabletPhase::kUpdateMeta);
  EXPECT_TRUE(split_proc_->child_tablets_[0]);
  EXPECT_TRUE(split_proc_->child_tablets_[1]);
  EXPECT_EQ(split_proc_->tablet_->GetKeyStart(), split_proc_->child_tablets_[0]->GetKeyStart());
  EXPECT_EQ(split_proc_->child_tablets_[0]->GetKeyEnd(),
            split_proc_->child_tablets_[1]->GetKeyStart());
  EXPECT_EQ(split_proc_->tablet_->GetKeyEnd(), split_proc_->child_tablets_[1]->GetKeyEnd());
  EXPECT_EQ(split_proc_->child_tablets_[0]->GetPath(), "test/tablet00000002");
  EXPECT_EQ(split_proc_->child_tablets_[1]->GetPath(), "test/tablet00000003");
  EXPECT_EQ(split_proc_->child_tablets_[0]->GetStatus(), TabletMeta::kTabletOffline);
  EXPECT_EQ(split_proc_->child_tablets_[1]->GetStatus(), TabletMeta::kTabletOffline);
}

TEST_F(SplitTabletProcedureTest, LoadTabletsPhaseHandler) {
  split_proc_->split_key_ = "c";
  split_proc_->UpdateMetaPhaseHandler(SplitTabletPhase::kUpdateMeta);
  EXPECT_FALSE(split_proc_->load_procs_[0]);
  EXPECT_FALSE(split_proc_->load_procs_[1]);
  split_proc_->LoadTabletsPhaseHandler(SplitTabletPhase::kLoadTablets);
  EXPECT_TRUE(split_proc_->load_procs_[0]);
  EXPECT_TRUE(split_proc_->load_procs_[1]);
  std::shared_ptr<LoadTabletProcedure> load_proc1 =
      std::dynamic_pointer_cast<LoadTabletProcedure>(split_proc_->load_procs_[0]);
  std::shared_ptr<LoadTabletProcedure> load_proc2 =
      std::dynamic_pointer_cast<LoadTabletProcedure>(split_proc_->load_procs_[1]);
  EXPECT_FALSE(split_proc_->load_procs_[0]->Done());
  EXPECT_FALSE(split_proc_->load_procs_[1]->Done());
  EXPECT_EQ(proc_executor_->procedures_.size(), 2);
  EXPECT_EQ(proc_executor_->procedures_[1]->proc_, split_proc_->load_procs_[0]);
  EXPECT_EQ(proc_executor_->procedures_[2]->proc_, split_proc_->load_procs_[1]);
  load_proc1->done_ = true;
  split_proc_->LoadTabletsPhaseHandler(SplitTabletPhase::kLoadTablets);
  load_proc2->done_ = true;
  split_proc_->child_tablets_[0]->SetStatus(TabletMeta::kTabletReady);
  split_proc_->child_tablets_[1]->SetStatus(TabletMeta::kTabletReady);
  split_proc_->LoadTabletsPhaseHandler(SplitTabletPhase::kLoadTablets);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);
}

TEST_F(SplitTabletProcedureTest, FaultRecoverPhaseHandler) {
  split_proc_->phases_.push_back(SplitTabletPhase::kPostUnLoadTablet);
  split_proc_->phases_.push_back(SplitTabletPhase::kFaultRecover);
  tablet_->SetStatus(TabletMeta::kTabletOffline);
  EXPECT_FALSE(split_proc_->recover_proc_);
  split_proc_->FaultRecoverPhaseHandler(SplitTabletPhase::kFaultRecover);
  EXPECT_TRUE(split_proc_->recover_proc_);
  EXPECT_EQ(proc_executor_->procedures_.size(), 1);
  EXPECT_FALSE(split_proc_->recover_proc_->Done());
  std::shared_ptr<LoadTabletProcedure> recover_proc =
      std::dynamic_pointer_cast<LoadTabletProcedure>(split_proc_->recover_proc_);
  recover_proc->done_ = true;
  split_proc_->FaultRecoverPhaseHandler(SplitTabletPhase::kFaultRecover);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);
}

TEST_F(SplitTabletProcedureTest, PreSplitTabletPhaseHandler) {
  FLAGS_tera_master_max_split_concurrency = 1;
  tablet_->AssignTabletNode(node_);
  split_proc_->split_key_ = "c";
  split_proc_->PreSplitTabletPhaseHandler(SplitTabletPhase::kPreSplitTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kUnLoadTablet);
  split_proc_->split_key_ = "b";
  split_proc_->PreSplitTabletPhaseHandler(SplitTabletPhase::kPreSplitTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);
  split_proc_->phases_.clear();
  split_proc_->split_key_ = "a";
  split_proc_->PreSplitTabletPhaseHandler(SplitTabletPhase::kPreSplitTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);

  split_proc_->phases_.clear();
  split_proc_->split_key_ = "d";
  split_proc_->PreSplitTabletPhaseHandler(SplitTabletPhase::kPreSplitTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);
  split_proc_->phases_.clear();
  split_proc_->split_key_ = "z";
  split_proc_->PreSplitTabletPhaseHandler(SplitTabletPhase::kPreSplitTablet);
  EXPECT_EQ(split_proc_->phases_.back(), SplitTabletPhase::kEofPhase);
}
}
}
}
