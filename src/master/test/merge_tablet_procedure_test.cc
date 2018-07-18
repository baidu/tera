// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "db/filename.h"
#include "io/utils_leveldb.h"
#include "master/load_tablet_procedure.h"
#include "master/merge_tablet_procedure.h"
#include "master/unload_tablet_procedure.h"
#include "master/master_env.h"
#include "master/master_zk_adapter.h"
#include "leveldb/env.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace master {
namespace test {

using leveldb::EnvOptions;

class MergeTabletProcedureTest : public ::testing::Test {
public:
    MergeTabletProcedureTest() : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)), 
                                ts_manager_(new TabletNodeManager(nullptr)), 
                                proc_executor_(new ProcedureExecutor) {}
    
    virtual ~MergeTabletProcedureTest() {}

    virtual void SetUp() {
        FLAGS_tera_leveldb_env_type.assign("local");
        FLAGS_tera_tabletnode_path_prefix.assign("./merge_tablet_procedure/");
        InitMasterEnv();
        TableSchema schema;
        StatusCode ret_code;
        table_ = TabletManager::CreateTable("test", schema, kTableEnable);
        EXPECT_TRUE(table_);
        EXPECT_TRUE(tablet_manager_->AddTable(table_, &ret_code));
        TabletMeta tablet_meta;
        TabletManager::PackTabletMeta(&tablet_meta, "test", "a", "b", "test/tablet00000001", "", TabletMeta::kTabletOffline, 10);
        tablets_[0] = TabletManager::CreateTablet(table_, tablet_meta);
        TabletManager::PackTabletMeta(&tablet_meta, "test", "b", "c", "test/tablet00000002", "", TabletMeta::kTabletOffline, 10);
        tablets_[1] = TabletManager::CreateTablet(table_, tablet_meta);
        EXPECT_TRUE(tablets_[0]);
        EXPECT_TRUE(tablets_[1]);
        EXPECT_TRUE(table_->AddTablet(tablets_[0], &ret_code));
        EXPECT_TRUE(table_->AddTablet(tablets_[1], &ret_code));


//        EXPECT_TRUE(tablet_manager_->AddTablet("test", 
//                    "a", "b", "test/tablet00000001", "", schema, 10, &tablets_[0], &ret_code));
//        EXPECT_TRUE(tablet_manager_->AddTablet("test", 
//                    "b", "c", "test/tablet00000002", "", schema, 10, &tablets_[1], &ret_code));
        proc_executor_->running_ = true;
        
        const ::testing::TestInfo* test_case = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string test_name(test_case->name());
        if (test_name.find("PostUnloadTabletsPhase") != std::string::npos) {
            InitFileSystemForMerge();
        }

    }
    virtual void TearDown() {
        proc_executor_->running_ = false;
    }

    static void SetUpTestCase() {}
    static void TearDownTestCase() {
        
    }
private:
    void InitMasterEnv() {
        MasterEnv().Init(nullptr, ts_manager_, tablet_manager_, 
                nullptr, nullptr, std::shared_ptr<ThreadPool>(new ThreadPool), proc_executor_, 
                std::shared_ptr<TabletAvailability>(new TabletAvailability(tablet_manager_)), nullptr); 
        // push one element to the queue, avoiding call TryMoveTablet while call SuspendMetaOperation
        MasterEnv().meta_task_queue_.push(nullptr);
    }

    void InitFileSystemForMerge() {
        fs_env_ = io::LeveldbBaseEnv();
        std::string table_path = FLAGS_tera_tabletnode_path_prefix + tablets_[0]->GetTableName();
        EXPECT_TRUE(fs_env_->CreateDir(table_path).ok());
        std::string tablet1_path = FLAGS_tera_tabletnode_path_prefix + tablets_[0]->GetPath();
        EXPECT_TRUE(fs_env_->CreateDir(tablet1_path).ok());
        std::string tablet2_path = FLAGS_tera_tabletnode_path_prefix + tablets_[1]->GetPath();
        EXPECT_TRUE(fs_env_->CreateDir(tablet2_path).ok());
        tablets_[0]->SetStatus(TabletMeta::kTabletReady);
        tablets_[1]->SetStatus(TabletMeta::kTabletReady);
        merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablets_[1], MasterEnv().GetThreadPool().get()));
    }

private:
    TablePtr table_;
    TabletPtr tablets_[2];
    std::shared_ptr<MergeTabletProcedure> merge_proc_;
    std::shared_ptr<TabletManager> tablet_manager_;
    std::shared_ptr<TabletNodeManager> ts_manager_;
    std::shared_ptr<ProcedureExecutor> proc_executor_;
    leveldb::Env* fs_env_; 
};

TEST_F(MergeTabletProcedureTest, MergeTabletProcedureInit) {
    tablets_[0]->SetStatus(TabletMeta::kTabletReady);
    tablets_[1]->SetStatus(TabletMeta::kTabletReady);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablets_[1], MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kUnLoadTablets);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[1], tablets_[0], MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kUnLoadTablets);
    TabletMeta tablet_meta;
    TabletManager::PackTabletMeta(&tablet_meta, "test", "ab", "c", "test/tablet00000001", "", TabletMeta::kTabletOffline, 10);
    StatusCode ret_code;
    TabletPtr tablet = TabletManager::CreateTablet(table_, tablet_meta);
    EXPECT_TRUE(table_->AddTablet(tablet, &ret_code));
    tablet->SetStatus(TabletMeta::kTabletReady);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablet, MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kEofPhase);
    std::cout << merge_proc_->ProcId() << std::endl;
}

TEST_F(MergeTabletProcedureTest, UpdateMeta) {
    tablets_[0]->SetStatus(TabletMeta::kTabletReady);
    tablets_[1]->SetStatus(TabletMeta::kTabletReady);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablets_[1], MasterEnv().GetThreadPool().get()));
    TabletNodePtr node = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
    tablets_[0]->AssignTabletNode(node);
    tablets_[1]->AssignTabletNode(node);
    EXPECT_FALSE(merge_proc_->merged_);
    merge_proc_->UpdateMeta();
    EXPECT_TRUE(merge_proc_->merged_);
    EXPECT_FALSE(merge_proc_->merged_->InTransition());
    EXPECT_EQ(merge_proc_->merged_->GetKeyStart(), tablets_[0]->GetKeyStart());
    EXPECT_EQ(merge_proc_->merged_->GetKeyEnd(), tablets_[1]->GetKeyEnd());

    merge_proc_.reset(new MergeTabletProcedure(tablets_[1], tablets_[0], MasterEnv().GetThreadPool().get()));
    merge_proc_->UpdateMeta();
    EXPECT_EQ(merge_proc_->merged_->GetKeyStart(), tablets_[0]->GetKeyStart());
    EXPECT_EQ(merge_proc_->merged_->GetKeyEnd(), tablets_[1]->GetKeyEnd());

    merge_proc_->MergeUpdateMetaDone(true);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kLoadMergedTablet);
}

TEST_F(MergeTabletProcedureTest, UnloadTabletsPhase) {
    TabletNodePtr node = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
    node->SetState(kReady, NULL);
    tablets_[0]->SetStatus(TabletMeta::kTabletReady);
    tablets_[1]->SetStatus(TabletMeta::kTabletReady);
    tablets_[0]->AssignTabletNode(node);
    tablets_[1]->AssignTabletNode(node);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablets_[1], MasterEnv().GetThreadPool().get()));
    EXPECT_FALSE(merge_proc_->unload_procs_[0]);
    EXPECT_FALSE(merge_proc_->unload_procs_[1]);

    merge_proc_->UnloadTabletsPhaseHandler(MergeTabletPhase::kUnLoadTablets);
    EXPECT_TRUE(merge_proc_->unload_procs_[0]);
    EXPECT_TRUE(merge_proc_->unload_procs_[1]);
    EXPECT_FALSE(merge_proc_->unload_procs_[0]->Done());
    EXPECT_FALSE(merge_proc_->unload_procs_[1]->Done());
    EXPECT_EQ(proc_executor_->procedures_.size(), 2);
    EXPECT_EQ(proc_executor_->procedures_[1]->proc_, merge_proc_->unload_procs_[0]);
    EXPECT_EQ(proc_executor_->procedures_[2]->proc_, merge_proc_->unload_procs_[1]);
    std::shared_ptr<UnloadTabletProcedure> unload_proc_0 = 
        std::dynamic_pointer_cast<UnloadTabletProcedure>(merge_proc_->unload_procs_[0]);
    std::shared_ptr<UnloadTabletProcedure> unload_proc_1 = 
        std::dynamic_pointer_cast<UnloadTabletProcedure>(merge_proc_->unload_procs_[1]);
    unload_proc_0->done_ = true;
    merge_proc_->UnloadTabletsPhaseHandler(MergeTabletPhase::kUnLoadTablets);
    unload_proc_1->done_ = true;
    merge_proc_->UnloadTabletsPhaseHandler(MergeTabletPhase::kUnLoadTablets);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kEofPhase);

    tablets_[0]->SetStatus(TabletMeta::kTabletOffline);
    tablets_[1]->SetStatus(TabletMeta::kTabletOffline);
    merge_proc_->UnloadTabletsPhaseHandler(MergeTabletPhase::kUnLoadTablets);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kPostUnLoadTablets);

}

TEST_F(MergeTabletProcedureTest, LoadMergedTabletPhase) {
    TabletNodePtr node = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
    node->SetState(kReady, NULL);
    tablets_[0]->SetStatus(TabletMeta::kTabletReady);
    tablets_[1]->SetStatus(TabletMeta::kTabletReady);
    tablets_[0]->AssignTabletNode(node);
    tablets_[1]->AssignTabletNode(node);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablets_[1], MasterEnv().GetThreadPool().get()));
    merge_proc_->UpdateMeta();
    EXPECT_TRUE(merge_proc_->merged_);
    EXPECT_FALSE(merge_proc_->load_proc_);

    merge_proc_->LoadMergedTabletPhaseHandler(MergeTabletPhase::kLoadMergedTablet);
    EXPECT_TRUE(merge_proc_->load_proc_);
    EXPECT_EQ(merge_proc_->load_proc_, proc_executor_->procedures_[1]->proc_);
    std::shared_ptr<LoadTabletProcedure> load_proc = 
        std::dynamic_pointer_cast<LoadTabletProcedure>(merge_proc_->load_proc_);
    EXPECT_FALSE(merge_proc_->load_proc_->Done());
    EXPECT_EQ(merge_proc_->load_proc_, proc_executor_->procedures_[1]->proc_);
    EXPECT_EQ(proc_executor_->procedures_.size(), 1);
    load_proc->done_ = true;
    merge_proc_->LoadMergedTabletPhaseHandler(MergeTabletPhase::kLoadMergedTablet);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kEofPhase);
}

TEST_F(MergeTabletProcedureTest, EOFPhaseHandler) {
    tablets_[0]->SetStatus(TabletMeta::kTabletReady);
    tablets_[1]->SetStatus(TabletMeta::kTabletReady);
    merge_proc_.reset(new MergeTabletProcedure(tablets_[0], tablets_[1], MasterEnv().GetThreadPool().get()));
    //TableSchema schema;
    StatusCode ret_code;
    TabletMeta meta;
    meta.set_table_name("test");
    meta.set_path("test/tablet00000003");
    meta.mutable_key_range()->set_key_start("a");
    meta.mutable_key_range()->set_key_end("c");
    merge_proc_->merged_.reset(new Tablet(meta, tablets_[0]->GetTable()));
    //EXPECT_TRUE(tablet_manager_->AddTablet("test", 
    //            "a", "c", "test/tablet00000003", "", schema, 1, &merge_proc_->merged_, &ret_code));
    EXPECT_FALSE(merge_proc_->merged_->InTransition());
    merge_proc_->merged_->LockTransition();
    EXPECT_TRUE(merge_proc_->merged_->InTransition());
    EXPECT_FALSE(merge_proc_->done_);
    merge_proc_->EOFPhaseHandler(MergeTabletPhase::kEofPhase);
    EXPECT_TRUE(merge_proc_->merged_->InTransition());
    EXPECT_TRUE(merge_proc_->done_);
}

TEST_F(MergeTabletProcedureTest, PostUnloadTabletsPhaseCheck_Ok) {

    merge_proc_->PostUnloadTabletsPhaseHandler(MergeTabletPhase::kPostUnLoadTablets);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kUpdateMeta);
}

TEST_F(MergeTabletProcedureTest, PostUnloadTabletsPhaseCheck_FirstTabletNotOk) {
    std::string log_path =FLAGS_tera_tabletnode_path_prefix + 
                "/" + leveldb::LogHexFileName(tablets_[0]->GetPath(), 123);
    leveldb::WritableFile* log_file;
    EXPECT_TRUE(fs_env_->NewWritableFile(log_path, &log_file, EnvOptions()).ok());
    delete log_file;
    merge_proc_->PostUnloadTabletsPhaseHandler(MergeTabletPhase::kPostUnLoadTablets);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kFaultRecover);
}

TEST_F(MergeTabletProcedureTest, PostUnloadTabletsPhaseCheck_SecondTabletNotOk) {
    std::string log_path =FLAGS_tera_tabletnode_path_prefix + 
                "/" + leveldb::LogHexFileName(tablets_[1]->GetPath(), 123);
    leveldb::WritableFile* log_file;
    EXPECT_TRUE(fs_env_->NewWritableFile(log_path, &log_file, EnvOptions()).ok());
    delete log_file;
    merge_proc_->PostUnloadTabletsPhaseHandler(MergeTabletPhase::kPostUnLoadTablets);
    EXPECT_EQ(merge_proc_->phases_.back(), MergeTabletPhase::kFaultRecover);
}


}
}
}
