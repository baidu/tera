#include <limits>
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/master_impl.h"
#include "master/test/mock_master_zk_adapter.h"

DECLARE_double(tera_safemode_tablet_locality_ratio);

namespace tera {
namespace master {
namespace test {

class MasterImplTest : public ::testing::Test {
 public:
  MasterImplTest() {
    master_impl_ = new MasterImpl(nullptr, nullptr);
    master_impl_->state_machine_.curr_status_ = kIsRunning;
  }
  ~MasterImplTest() {
    if (master_impl_) {
      delete master_impl_;
      master_impl_ = nullptr;
    }
  }

  void InitTabletNodesAndTablets();

 private:
  MasterImpl* master_impl_;
  TabletNodePtr tabletnodes_[2];
  TablePtr table_;
  TabletPtr tablets_[4];
};

void MasterImplTest::InitTabletNodesAndTablets() {
  master_impl_->zk_adapter_.reset(new TestZkAdapter);

  tabletnodes_[0] = master_impl_->tabletnode_manager_->AddTabletNode("127.0.0.1:1", "10000");
  tabletnodes_[1] = master_impl_->tabletnode_manager_->AddTabletNode("127.0.0.1:2", "20000");

  TableSchema schema;
  StatusCode status;
  table_ = TabletManager::CreateTable("test", schema, kTableEnable);
  EXPECT_TRUE(table_);
  EXPECT_TRUE(master_impl_->tablet_manager_->AddTable(table_, &status));

  TabletMeta tablet_meta;
  TabletManager::PackTabletMeta(&tablet_meta, "test", "", "b", "test/tablet00000001", "",
                                TabletMeta::kTabletOffline, 0);
  tablets_[0] = table_->AddTablet(tablet_meta, &status);
  EXPECT_TRUE(tablets_[0]);
  BindTabletToTabletNode(tablets_[0], tabletnodes_[0]);
  tablets_[0]->SetStatus(TabletMeta::kTabletReady);

  TabletManager::PackTabletMeta(&tablet_meta, "test", "b", "d", "test/tablet00000002", "",
                                TabletMeta::kTabletOffline, 0);
  tablets_[1] = table_->AddTablet(tablet_meta, &status);
  EXPECT_TRUE(tablets_[1]);
  BindTabletToTabletNode(tablets_[1], tabletnodes_[0]);
  tablets_[1]->SetStatus(TabletMeta::kTabletReady);

  TabletManager::PackTabletMeta(&tablet_meta, "test", "d", "f", "test/tablet00000003", "",
                                TabletMeta::kTabletOffline, 0);
  tablets_[2] = table_->AddTablet(tablet_meta, &status);
  EXPECT_TRUE(tablets_[2]);
  BindTabletToTabletNode(tablets_[2], tabletnodes_[1]);
  tablets_[2]->SetStatus(TabletMeta::kTabletReady);

  TabletManager::PackTabletMeta(&tablet_meta, "test", "f", "", "test/tablet00000004", "",
                                TabletMeta::kTabletOffline, 0);
  tablets_[3] = table_->AddTablet(tablet_meta, &status);
  EXPECT_TRUE(tablets_[3]);
  BindTabletToTabletNode(tablets_[3], tabletnodes_[1]);
  tablets_[3]->SetStatus(TabletMeta::kTabletReady);

  master_impl_->meta_tablet_ =
      master_impl_->tablet_manager_->AddMetaTablet(tabletnodes_[0], master_impl_->zk_adapter_);
}

TEST_F(MasterImplTest, DeleteTsNotEnterSafemode) {
  InitTabletNodesAndTablets();
  FLAGS_tera_safemode_tablet_locality_ratio = 0.3;
  master_impl_->DeleteTabletNode("127.0.0.1:1", "10000");
  EXPECT_EQ(master_impl_->GetMasterStatus(), kIsRunning);
  EXPECT_EQ(master_impl_->tabletnode_manager_->reconnecting_ts_list_.size(), 1);
  EXPECT_EQ(tablets_[0]->GetStatus(), TabletMeta::kTabletDelayOffline);
  EXPECT_EQ(tablets_[1]->GetStatus(), TabletMeta::kTabletDelayOffline);
}

TEST_F(MasterImplTest, DeleteTsEnterSafemode) {
  InitTabletNodesAndTablets();
  FLAGS_tera_safemode_tablet_locality_ratio = 0.8;
  master_impl_->DeleteTabletNode("127.0.0.1:1", "10000");
  EXPECT_EQ(master_impl_->GetMasterStatus(), kIsReadonly);
  EXPECT_EQ(master_impl_->tabletnode_manager_->reconnecting_ts_list_.size(), 0);
  EXPECT_EQ(tablets_[0]->GetStatus(), TabletMeta::kTabletOffline);
  EXPECT_EQ(tablets_[1]->GetStatus(), TabletMeta::kTabletOffline);
}

TEST_F(MasterImplTest, TabletNodeReconnect) {
  InitTabletNodesAndTablets();
  FLAGS_tera_safemode_tablet_locality_ratio = 0.3;
  master_impl_->DeleteTabletNode("127.0.0.1:2", "20000");
  EXPECT_EQ(master_impl_->tabletnode_manager_->reconnecting_ts_list_.size(), 1);
  EXPECT_EQ(master_impl_->GetMasterStatus(), kIsRunning);
  EXPECT_EQ(tablets_[2]->GetStatus(), TabletMeta::kTabletDelayOffline);
  EXPECT_EQ(tablets_[3]->GetStatus(), TabletMeta::kTabletDelayOffline);
  master_impl_->AddTabletNode("127.0.0.1:2", "20001");  // reconnect with new uuid
  EXPECT_EQ(master_impl_->tabletnode_manager_->reconnecting_ts_list_.size(), 0);
  EXPECT_EQ(tablets_[2]->GetStatus(), TabletMeta::kTabletOffline);
  EXPECT_EQ(tablets_[3]->GetStatus(), TabletMeta::kTabletOffline);
}
}
}
}
