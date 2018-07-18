// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/load_tablet_procedure.h"
#include "master/move_tablet_procedure.h"
#include "master/unload_tablet_procedure.h"
#include "master/master_env.h"
#include "master/master_zk_adapter.h"

namespace tera {
namespace master {
namespace test {
class MoveTabletProcedureTest : public ::testing::Test {
public:
    MoveTabletProcedureTest() : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)), 
                                ts_manager_(new TabletNodeManager(new MasterImpl)), 
                                proc_executor_(new ProcedureExecutor), 
                                tablet_availability_(new TabletAvailability(tablet_manager_)) {}
    virtual ~MoveTabletProcedureTest() {}

    virtual void SetUp() {
        InitMasterEnv();
        TableSchema schema;
        StatusCode ret_code;
        table_ = TabletManager::CreateTable("test", schema, kTableEnable);
        EXPECT_TRUE(table_);
        EXPECT_TRUE(tablet_manager_->AddTable(table_, &ret_code));
        TabletMeta tablet_meta;
        TabletManager::PackTabletMeta(&tablet_meta, "test", "", "", "test/tablet00000001", "", TabletMeta::kTabletOffline, 0);
        tablet_ = TabletManager::CreateTablet(table_, tablet_meta);
        EXPECT_TRUE(tablet_);
        EXPECT_TRUE(table_->AddTablet(tablet_, &ret_code));
        //EXPECT_TRUE(tablet_manager_->AddTablet("test", "", "", "test/tablet00000001", "", schema, 0, &tablet_, &ret_code));
        src_node_ = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
        EXPECT_EQ(src_node_->GetState(), kReady);
        dest_node_= ts_manager_->AddTabletNode("127.0.0.2:2000", "1234568");
        EXPECT_EQ(dest_node_->GetState(), kReady);
        // pretend proc_executor is running
        proc_executor_->running_ = true;
    }
    virtual void TearDown() {
        proc_executor_->running_ = false;
    }

    static void SetUpTestCase() {}
    static void TearDownTestCase() {}
private:
    void InitMasterEnv();

private:
    TablePtr table_;
    TabletPtr tablet_;
    TabletNodePtr src_node_;
    TabletNodePtr dest_node_;
    std::shared_ptr<MoveTabletProcedure> move_proc_;
    std::shared_ptr<TabletManager> tablet_manager_;
    std::shared_ptr<TabletNodeManager> ts_manager_;
    std::shared_ptr<TabletAvailability> tablet_availability_;
    std::shared_ptr<ProcedureExecutor> proc_executor_;
};

void MoveTabletProcedureTest::InitMasterEnv() {
    MasterEnv().Init(new MasterImpl, ts_manager_, tablet_manager_, 
            std::shared_ptr<SizeScheduler>(new SizeScheduler), nullptr, 
            std::shared_ptr<ThreadPool>(new ThreadPool), proc_executor_, tablet_availability_, nullptr);
}

TEST_F(MoveTabletProcedureTest, MoveProcedureInit) {
    tablet_->SetStatus(TabletMeta::kTabletReady);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, nullptr, MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kUnLoadTablet);
    tablet_->SetStatus(TabletMeta::kTabletOffline);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, nullptr, MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kLoadTablet);
    tablet_->SetStatus(TabletMeta::kTabletLoadFail);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, nullptr, MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kLoadTablet);

    tablet_->SetStatus(TabletMeta::kTabletUnloading);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, nullptr, MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kEofPhase);
    std::cout << move_proc_->ProcId();
    std::cout << MoveTabletPhase::kUnLoadTablet;
    
}

TEST_F(MoveTabletProcedureTest, UnLoadTabletPhaseHandler) {
    tablet_->SetStatus(TabletMeta::kTabletReady);
    tablet_->AssignTabletNode(src_node_);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, nullptr, MasterEnv().GetThreadPool().get()));
    EXPECT_FALSE(move_proc_->unload_proc_);
    move_proc_->UnloadTabletPhaseHandler(MoveTabletPhase::kUnLoadTablet);
    EXPECT_TRUE(move_proc_->unload_proc_);
    std::shared_ptr<UnloadTabletProcedure> unload_proc = 
            std::dynamic_pointer_cast<UnloadTabletProcedure>(move_proc_->unload_proc_);
    EXPECT_FALSE(move_proc_->unload_proc_->Done());
    EXPECT_EQ(proc_executor_->procedures_.size(), 1);
    EXPECT_EQ(proc_executor_->procedures_[1]->proc_, move_proc_->unload_proc_);

    move_proc_->UnloadTabletPhaseHandler(MoveTabletPhase::kUnLoadTablet);
    EXPECT_FALSE(move_proc_->unload_proc_->Done());
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kUnLoadTablet);
    EXPECT_EQ(proc_executor_->procedures_.size(), 1);
    EXPECT_EQ(proc_executor_->procedures_[1]->proc_, move_proc_->unload_proc_);
    
    unload_proc->done_ = true;
    move_proc_->UnloadTabletPhaseHandler(MoveTabletPhase::kUnLoadTablet);
    // as the procedure_executor is not really running, tablet_ status will not change
    EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletReady);
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kEofPhase);

    tablet_->SetStatus(TabletMeta::kTabletOffline);
    move_proc_->UnloadTabletPhaseHandler(MoveTabletPhase::kUnLoadTablet);
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kLoadTablet);
}

TEST_F(MoveTabletProcedureTest, LoadTabletPhaseHandler) {
    tablet_->SetStatus(TabletMeta::kTabletOffline);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, nullptr, MasterEnv().GetThreadPool().get()));
    EXPECT_FALSE(move_proc_->load_proc_);
    move_proc_->LoadTabletPhaseHandler(MoveTabletPhase::kLoadTablet);
    EXPECT_TRUE(move_proc_->load_proc_);
    std::shared_ptr<LoadTabletProcedure> load_proc = 
            std::dynamic_pointer_cast<LoadTabletProcedure>(move_proc_->load_proc_);
    EXPECT_FALSE(move_proc_->load_proc_->Done());
    EXPECT_EQ(proc_executor_->procedures_.size(), 1);
    EXPECT_EQ(proc_executor_->procedures_[1]->proc_, move_proc_->load_proc_);

    move_proc_->LoadTabletPhaseHandler(MoveTabletPhase::kLoadTablet);
    EXPECT_FALSE(move_proc_->load_proc_->Done());
    EXPECT_EQ(proc_executor_->procedures_.size(), 1);
    EXPECT_EQ(proc_executor_->procedures_[1]->proc_, move_proc_->load_proc_);
    
    load_proc->done_ = true;
    move_proc_->LoadTabletPhaseHandler(MoveTabletPhase::kLoadTablet);
    EXPECT_TRUE(move_proc_->load_proc_->Done());
    EXPECT_EQ(move_proc_->phases_.back(), MoveTabletPhase::kEofPhase);
}

TEST_F(MoveTabletProcedureTest, EofPhaseHandler) {
    EXPECT_EQ(dest_node_->plan_move_in_count_, 0);
    move_proc_ = std::shared_ptr<MoveTabletProcedure>(new MoveTabletProcedure(tablet_, dest_node_, MasterEnv().GetThreadPool().get()));
    EXPECT_EQ(dest_node_->plan_move_in_count_, 1);
    move_proc_->EOFPhaseHandler(MoveTabletPhase::kEofPhase);
    EXPECT_EQ(dest_node_->plan_move_in_count_, 0);
}

}
}
}
