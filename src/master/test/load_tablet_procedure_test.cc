#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/load_tablet_procedure.h"
#include "master/master_env.h"
#include "master/test/mock_master_zk_adapter.h"
#include "common/thread_pool.h"

DECLARE_int32(tera_master_tabletnode_timeout);
DECLARE_int32(tera_master_max_load_concurrency);
DECLARE_int32(tera_master_impl_retry_times);
DECLARE_int32(tera_master_load_rpc_timeout);
DECLARE_int32(tera_master_control_tabletnode_retry_period);
DECLARE_int32(tablet_load_max_tried_ts);

namespace tera {
namespace master {
namespace test {
class LoadTabletProcedureTest : public ::testing::Test {
 public:
  LoadTabletProcedureTest()
      : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)),
        ts_manager_(new TabletNodeManager(new MasterImpl(nullptr, nullptr))),
        tablet_availability_(new TabletAvailability(tablet_manager_)) {}

  virtual ~LoadTabletProcedureTest() {}

  virtual void SetUp() {
    InitMasterEnv();

    TableSchema schema;
    StatusCode ret_code;
    table_ = TabletManager::CreateTable("test", schema, kTableEnable);
    EXPECT_TRUE(table_);
    EXPECT_TRUE(tablet_manager_->AddTable(table_, &ret_code));

    TabletMeta tablet_meta;
    TabletManager::PackTabletMeta(&tablet_meta, "test", "", "", "test/tablet00000001", "",
                                  TabletMeta::kTabletOffline, 0);
    tablet_ = table_->AddTablet(tablet_meta, &ret_code);
    EXPECT_TRUE(table_);

    load_proc_ = std::shared_ptr<LoadTabletProcedure>(
        new LoadTabletProcedure(tablet_, MasterEnv().GetThreadPool().get()));
    EXPECT_TRUE(load_proc_);
  }

  virtual void TearDown() {}

  static void SetUpTestCase(){};
  static void TearDownTestCase(){};

 private:
  void InitLoadTabletCallbackParameters(StatusCode status) {
    request_ = new LoadTabletRequest;
    response_ = new LoadTabletResponse;
    response_->set_status(status);
    load_proc_->load_request_dispatching_ = true;
    load_proc_->load_retrys_ = 0;
    load_proc_->slow_load_retrys_ = 0;
  }

  void InitMasterEnv();
  TablePtr table_;
  TabletPtr tablet_;
  std::shared_ptr<LoadTabletProcedure> load_proc_;
  std::shared_ptr<TabletManager> tablet_manager_;
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::shared_ptr<TabletNodeManager> ts_manager_;
  std::shared_ptr<TabletAvailability> tablet_availability_;

  LoadTabletRequest* request_;
  LoadTabletResponse* response_;
};

void LoadTabletProcedureTest::InitMasterEnv() {
  MasterEnv().Init(new MasterImpl(nullptr, nullptr), ts_manager_, tablet_manager_, access_builder_,
                   nullptr, std::shared_ptr<SizeScheduler>(new SizeScheduler), nullptr,
                   std::shared_ptr<ThreadPool>(new ThreadPool),
                   std::shared_ptr<ProcedureExecutor>(new ProcedureExecutor), tablet_availability_,
                   std::shared_ptr<tera::sdk::StatTable>(new tera::sdk::StatTable(
                       nullptr, access_builder_, sdk::StatTableCustomer::kMaster)));
}

TEST_F(LoadTabletProcedureTest, GenerateEventForUserTabletInStatusOffline) {
  // events to be processed with tablet in status kTableOffline
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletOffline);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsOffline);
  TabletNodePtr tabletnode(new TabletNode("127.0.0.1:2000", "1234567"));
  load_proc_->dest_node_ = tabletnode;
  // dest_node down without tera_master_tabletnode_timeout
  tabletnode->state_ = kOffline;
  FLAGS_tera_master_tabletnode_timeout = 0;
  EXPECT_EQ(tabletnode->GetState(), kOffline);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsOffline);
  // dest_node down with tera_master_tabletnode_timeout
  FLAGS_tera_master_tabletnode_timeout = 100;
  EXPECT_EQ(tabletnode->GetState(), kPendingOffline);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsDelayOffline);
  usleep(100 * 1000);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsOffline);

  // dest_node restarted
  ts_manager_->AddTabletNode("127.0.0.1:2000", "1234570");
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsRestart);

  tabletnode->state_ = kReady;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kUpdateMeta);

  load_proc_->update_meta_done_ = true;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kLoadTablet);
  EXPECT_EQ(tabletnode->onload_count_, 1);
  FLAGS_tera_master_max_load_concurrency = 1;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsLoadBusy);
}

TEST_F(LoadTabletProcedureTest, GenerateEventForUserTabletInStatusOnLoad) {
  // events to be processed with tablet in status kTableOnLoad
  tablet_->SetStatus(TabletMeta::kTabletLoading);
  FLAGS_tera_master_impl_retry_times = 2;
  TabletNodePtr tabletnode(new TabletNode("127.0.0.1:2000", "1234567"));
  tabletnode->state_ = kOffline;
  load_proc_->dest_node_ = tabletnode;
  FLAGS_tera_master_tabletnode_timeout = 0;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsOffline);
  FLAGS_tera_master_tabletnode_timeout = 100;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsDelayOffline);
  ts_manager_->AddTabletNode("127.0.0.1:2000", "1234570");
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsRestart);

  tabletnode->state_ = kReady;
  load_proc_->load_request_dispatching_ = true;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kWaitRpcResponse);

  load_proc_->load_request_dispatching_ = false;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsLoadSucc);

  load_proc_->load_retrys_ = FLAGS_tera_master_impl_retry_times + 1;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsLoadFail);

  tablet_->SetStatus(TabletMeta::kTabletReady);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kEofEvent);
  tablet_->SetStatus(TabletMeta::kTabletOffline);
  tablet_->SetStatus(TabletMeta::kTabletLoadFail);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kEofEvent);
}

TEST_F(LoadTabletProcedureTest, GenerateEventForMetaTablet) {
  TabletNodePtr tabletnode(new TabletNode("127.0.0.1:2000", "1234567"));
  MetaTabletPtr meta_tablet = tablet_manager_->AddMetaTablet(
      tabletnode, std::shared_ptr<MasterZkAdapterBase>(new TestZkAdapter));
  load_proc_->tablet_ = meta_tablet;
  EXPECT_EQ(meta_tablet->GetStatus(), TabletMeta::kTabletReady);
  meta_tablet->SetStatus(TabletMeta::kTabletOffline);
  load_proc_->dest_node_ = tabletnode;
  FLAGS_tera_master_tabletnode_timeout = 100;
  EXPECT_EQ(tabletnode->GetState(), kPendingOffline);
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kTsOffline);
  // meta tablet always load immediate discard tabletnode concurrency loading
  // limits
  tabletnode->state_ = kReady;
  tabletnode->onload_count_ = 3;
  FLAGS_tera_master_max_load_concurrency = 2;
  EXPECT_EQ(load_proc_->GenerateEvent(), TabletEvent::kLoadTablet);
}

TEST_F(LoadTabletProcedureTest, IsNeweEvent) {
  EXPECT_TRUE(load_proc_->events_.empty());
  EXPECT_TRUE(load_proc_->IsNewEvent(TabletEvent::kLoadTablet));
  // we should never got two successive TabletEvent::kLoadTablet
  EXPECT_FALSE(load_proc_->IsNewEvent(TabletEvent::kLoadTablet));
  EXPECT_EQ(load_proc_->events_.size(), 1);
  EXPECT_EQ(load_proc_->events_.back(), TabletEvent::kLoadTablet);
  EXPECT_TRUE(load_proc_->IsNewEvent(TabletEvent::kTsRestart));
  EXPECT_EQ(load_proc_->events_.size(), 2);
  EXPECT_EQ(load_proc_->events_.back(), TabletEvent::kTsRestart);
  load_proc_->dest_node_ = TabletNodePtr(new TabletNode("127.0.0.1:2000", "1234567"));
  EXPECT_TRUE(load_proc_->IsNewEvent(TabletEvent::kTsRestart));
  EXPECT_EQ(load_proc_->events_.size(), 3);
  EXPECT_EQ(load_proc_->events_.back(), TabletEvent::kTsRestart);
}

TEST_F(LoadTabletProcedureTest, LoadTabletCallback) {
  tablet_->SetStatus(TabletMeta::kTabletLoading);
  TabletNodePtr tabletnode = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
  EXPECT_EQ(tabletnode->GetState(), kReady);
  InitLoadTabletCallbackParameters(kTabletNodeOk);
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_FALSE(load_proc_->load_request_dispatching_);
  EXPECT_EQ(load_proc_->load_retrys_, 0);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 0);

  InitLoadTabletCallbackParameters(kTabletReady);
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_FALSE(load_proc_->load_request_dispatching_);
  EXPECT_EQ(load_proc_->load_retrys_, 0);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 0);

  load_proc_->slow_load_retrys_ = 0;
  load_proc_->load_retrys_ = 0;
  InitLoadTabletCallbackParameters(kTabletNodeIsBusy);
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  InitLoadTabletCallbackParameters(kTabletNodeIsBusy);
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_EQ(load_proc_->load_retrys_, 0);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 0);

  InitLoadTabletCallbackParameters(kTabletOnLoad);
  load_proc_->slow_load_retrys_ = 0;
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_TRUE(load_proc_->load_request_dispatching_);
  EXPECT_EQ(load_proc_->load_retrys_, 0);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 1);

  InitLoadTabletCallbackParameters(kTabletWaitLoad);
  load_proc_->slow_load_retrys_ = 0;
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 1);

  InitLoadTabletCallbackParameters(kTabletOnLoad);
  load_proc_->slow_load_retrys_ = 9;
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_TRUE(load_proc_->load_request_dispatching_);
  EXPECT_EQ(load_proc_->load_retrys_, 1);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 10);
  // rpc succ with unexpect response->status
  InitLoadTabletCallbackParameters(kTabletNotInit);
  load_proc_->load_retrys_ = FLAGS_tera_master_impl_retry_times;
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_TRUE(load_proc_->load_request_dispatching_);
  EXPECT_EQ(load_proc_->load_retrys_, FLAGS_tera_master_impl_retry_times + 1);

  // rpc fail
  InitLoadTabletCallbackParameters(kTabletNodeOk);
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, true, 1);
  EXPECT_TRUE(load_proc_->load_request_dispatching_);
  EXPECT_EQ(load_proc_->load_retrys_, 1);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 0);
  // tabletnode down
  InitLoadTabletCallbackParameters(kTabletNodeOk);
  tabletnode->state_ = kOffline;
  load_proc_->LoadTabletCallback(tabletnode, request_, response_, false, 0);
  EXPECT_EQ(load_proc_->load_retrys_, 1);
  EXPECT_EQ(load_proc_->slow_load_retrys_, 0);
}

TEST_F(LoadTabletProcedureTest, TestEventHandlers) {
  TabletNodePtr node1 = ts_manager_->AddTabletNode("127.0.0.1:2000", "1");
  TabletNodePtr node2 = ts_manager_->AddTabletNode("127.0.0.1:2001", "2");
  TabletNodePtr node3 = ts_manager_->AddTabletNode("127.0.0.1:2002", "3");

  node1->table_size_[tablet_->GetTableName()] = 20;
  node2->state_ = kOffline;
  node3->data_size_ = 10;
  load_proc_->TabletNodeOfflineHandler(TabletEvent::kTsOffline);
  EXPECT_EQ(load_proc_->dest_node_->uuid_, node3->uuid_);
  TabletNodePtr del_node = ts_manager_->DelTabletNode("127.0.0.1:2002");
  load_proc_->restarted_dest_node_ = ts_manager_->AddTabletNode("127.0.0.1:2002", "4");
  load_proc_->TabletNodeRestartHandler(TabletEvent::kTsRestart);
  EXPECT_NE(load_proc_->dest_node_->uuid_, "3");
  EXPECT_EQ(del_node, node3);
  EXPECT_TRUE(del_node->NodeDown());
  EXPECT_EQ(load_proc_->dest_node_->uuid_, "4");
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletOffline);

  load_proc_->TabletNodeBusyHandler(TabletEvent::kTsLoadBusy);
  load_proc_->TabletPendOfflineHandler(TabletEvent::kTsDelayOffline);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletDelayOffline);

  tablet_->SetStatus(TabletMeta::kTabletLoading);
  load_proc_->WaitRpcResponseHandler(TabletEvent::kWaitRpcResponse);
  load_proc_->TabletNodeLoadSuccHandler(TabletEvent::kTsLoadSucc);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletReady);

  tablet_->SetStatus(TabletMeta::kTabletLoading);
  load_proc_->TabletNodeLoadFailHandler(TabletEvent::kTsLoadFail);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletLoadFail);
  tablet_->SetStatus(TabletMeta::kTabletOffline);
  load_proc_->TabletLoadFailHandler(TabletEvent::kTabletLoadFail);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletLoadFail);

  // tablet can only transite to kTableOnLoad from kTableOffline, so set status
  // to kTableOffline first
  tablet_->SetStatus(TabletMeta::kTabletOffline);
  load_proc_->dest_node_ = node1;
  // load_proc_->LoadTabletHandler(TabletEvent::kLoadTablet);
  // EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletLoading);

  tablet_->SetStatus(TabletMeta::kTabletReady);
  load_proc_->EOFHandler(TabletEvent::kEofEvent);
}

TEST_F(LoadTabletProcedureTest, EOFPhaseHandler) {
  std::shared_ptr<ProcedureExecutor> proc_executor = MasterEnv().GetExecutor();

  tablet_->load_fail_cnt_ = FLAGS_tablet_load_max_tried_ts;
  tablet_->LockTransition();
  load_proc_->EOFHandler(TabletEvent::kEofEvent);
  EXPECT_FALSE(tablet_->InTransition());
  EXPECT_EQ(proc_executor->procedures_.size(), 0);

  tablet_->load_fail_cnt_ = FLAGS_tablet_load_max_tried_ts + 1;
  tablet_->LockTransition();
  load_proc_->EOFHandler(TabletEvent::kEofEvent);
  EXPECT_FALSE(tablet_->InTransition());
  EXPECT_EQ(proc_executor->procedures_.size(), 0);

  tablet_->load_fail_cnt_ = FLAGS_tablet_load_max_tried_ts - 1;
  tablet_->LockTransition();
  tablet_->SetStatus(TabletMeta::kTabletReady);
  load_proc_->EOFHandler(TabletEvent::kEofEvent);
  EXPECT_EQ(proc_executor->procedures_.size(), 0);
  EXPECT_FALSE(tablet_->InTransition());

  proc_executor->Start();
  tablet_->SetStatus(TabletMeta::kTabletLoadFail);
  tablet_->LockTransition();
  load_proc_->EOFHandler(TabletEvent::kEofEvent);
  EXPECT_EQ(proc_executor->procedures_.size(), 1);
  EXPECT_TRUE(tablet_->InTransition());
  proc_executor->Stop();
}

// those AsycLifeCycle* test cases should cause core dump. Since load_proc_ is
// dectructed, we cannot
// check its member fields
TEST_F(LoadTabletProcedureTest, AsynLifeCycle_CallbackExceedProcedureCycle) {
  tablet_->SetStatus(TabletMeta::kTabletReady);
  TabletNodePtr tabletnode(new TabletNode("127.0.0.1:2000", "1234567"));
  tabletnode->state_ = kReady;
  load_proc_->dest_node_ = tabletnode;
  load_proc_->LoadTabletAsync(tabletnode);
  load_proc_.reset();
}

TEST_F(LoadTabletProcedureTest, AsyncLifeCycleTest_DelayedLoadTaskExeedProcedureCycle) {
  TabletNodePtr tabletnode(new TabletNode("127.0.0.1:2000", "1234567"));
  tabletnode->state_ = kReady;
  load_proc_->dest_node_ = tabletnode;
  ThreadPool::Task task = std::bind(&LoadTabletProcedure::LoadTabletAsyncWrapper,
                                    std::weak_ptr<LoadTabletProcedure>(load_proc_), tabletnode);
  MasterEnv().GetThreadPool()->DelayTask(100, task);
  MasterEnv().GetThreadPool()->Start();
  EXPECT_EQ(MasterEnv().GetThreadPool()->latest_.size(), 1);
  load_proc_.reset();
  usleep(200 * 1000);
  EXPECT_EQ(MasterEnv().GetThreadPool()->latest_.size(), 0);
  MasterEnv().GetThreadPool()->Stop(false);
}
}
}
}
