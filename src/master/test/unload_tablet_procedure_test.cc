#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/unload_tablet_procedure.h"
#include "master/master_env.h"
#include "master/master_zk_adapter.h"

DECLARE_int32(tera_master_impl_retry_times);
DECLARE_bool(tera_master_kick_tabletnode_enabled);
namespace tera {
namespace master {

class UnloadTabletProcedureTest : public ::testing::Test {
 public:
  UnloadTabletProcedureTest()
      : tablet_manager_(new TabletManager(nullptr, nullptr, nullptr)),
        ts_manager_(new TabletNodeManager(new MasterImpl(nullptr, nullptr))),
        tablet_availability_(new TabletAvailability(tablet_manager_)) {}

  virtual ~UnloadTabletProcedureTest() {}

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
    EXPECT_TRUE(tablet_);
    tablet_->SetStatus(TabletMeta::kTabletReady);
    node_ = ts_manager_->AddTabletNode("127.0.0.1:2000", "1234567");
    tablet_->AssignTabletNode(node_);
    unload_proc_ = std::shared_ptr<UnloadTabletProcedure>(
        new UnloadTabletProcedure(tablet_, MasterEnv().GetThreadPool().get()));
    EXPECT_TRUE(unload_proc_);
  }

  virtual void TearDown() {}

  static void SetUpTestCase(){};
  static void TearDownTestCase(){};

 private:
  void PrepareUnloadTabletCallbackParameter(StatusCode status) {
    request_ = new UnloadTabletRequest;
    response_ = new UnloadTabletResponse;
    response_->set_status(status);
    unload_proc_->unload_request_dispatching_ = true;
    unload_proc_->unload_retrys_ = 0;
  }

  void InitMasterEnv();
  TablePtr table_;
  TabletPtr tablet_;
  TabletNodePtr node_;
  std::shared_ptr<UnloadTabletProcedure> unload_proc_;
  std::shared_ptr<TabletManager> tablet_manager_;
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::shared_ptr<TabletNodeManager> ts_manager_;
  std::shared_ptr<TabletAvailability> tablet_availability_;
  UnloadTabletRequest* request_;
  UnloadTabletResponse* response_;
};

void UnloadTabletProcedureTest::InitMasterEnv() {
  MasterEnv().Init(new MasterImpl(nullptr, nullptr), ts_manager_, tablet_manager_, access_builder_,
                   nullptr, std::shared_ptr<SizeScheduler>(new SizeScheduler), nullptr,
                   std::shared_ptr<ThreadPool>(new ThreadPool), nullptr, tablet_availability_,
                   nullptr);
}

TEST_F(UnloadTabletProcedureTest, GenerateEvent) {
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletReady);
  EXPECT_EQ(node_->GetState(), kReady);
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kUnLoadTablet);

  node_->state_ = kOffline;
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kTsOffline);

  node_->state_ = kReady;
  tablet_->SetStatus(TabletMeta::kTabletUnloading);
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kTsUnLoadSucc);

  unload_proc_->unload_request_dispatching_ = true;
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kWaitRpcResponse);
  unload_proc_->unload_request_dispatching_ = false;
  unload_proc_->unload_retrys_ = FLAGS_tera_master_impl_retry_times;
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kTsUnLoadSucc);

  unload_proc_->kick_ts_succ_ = true;
  unload_proc_->unload_retrys_ = FLAGS_tera_master_impl_retry_times + 1;
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kWaitRpcResponse);
  unload_proc_->kick_ts_succ_ = false;
  EXPECT_EQ(unload_proc_->GenerateEvent(), TabletEvent::kTsUnLoadFail);
}

TEST_F(UnloadTabletProcedureTest, UnloadTabletCallback) {
  PrepareUnloadTabletCallbackParameter(kTabletNodeOk);
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_FALSE(unload_proc_->unload_request_dispatching_);
  EXPECT_EQ(unload_proc_->unload_retrys_, 0);

  PrepareUnloadTabletCallbackParameter(kKeyNotInRange);
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_FALSE(unload_proc_->unload_request_dispatching_);
  EXPECT_EQ(unload_proc_->unload_retrys_, 0);

  PrepareUnloadTabletCallbackParameter(kTabletNodeIsBusy);
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  PrepareUnloadTabletCallbackParameter(kTabletNodeIsBusy);
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_EQ(unload_proc_->unload_retrys_, 0);
  EXPECT_TRUE(unload_proc_->unload_request_dispatching_);

  PrepareUnloadTabletCallbackParameter(kTabletUnloading);
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_TRUE(unload_proc_->unload_request_dispatching_);
  EXPECT_EQ(unload_proc_->unload_retrys_, 1);

  PrepareUnloadTabletCallbackParameter(kTabletUnloading);
  unload_proc_->unload_retrys_ = FLAGS_tera_master_impl_retry_times;
  node_->state_ = kKicked;
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_EQ(unload_proc_->unload_retrys_, FLAGS_tera_master_impl_retry_times + 1);
  EXPECT_TRUE(unload_proc_->kick_ts_succ_);

  PrepareUnloadTabletCallbackParameter(kTabletUnloading);
  FLAGS_tera_master_kick_tabletnode_enabled = false;
  unload_proc_->unload_retrys_ = FLAGS_tera_master_impl_retry_times;
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_FALSE(unload_proc_->unload_request_dispatching_);
  EXPECT_EQ(unload_proc_->unload_retrys_, FLAGS_tera_master_impl_retry_times + 1);
  EXPECT_FALSE(!unload_proc_->kick_ts_succ_);

  PrepareUnloadTabletCallbackParameter(kTabletNodeOk);
  unload_proc_->UnloadTabletCallback(request_, response_, true, 1);
  EXPECT_TRUE(unload_proc_->unload_request_dispatching_);
  EXPECT_EQ(unload_proc_->unload_retrys_, 1);

  PrepareUnloadTabletCallbackParameter(kTabletNodeOk);
  node_->state_ = kOffline;
  unload_proc_->UnloadTabletCallback(request_, response_, false, 0);
  EXPECT_FALSE(unload_proc_->unload_request_dispatching_);
  EXPECT_EQ(unload_proc_->unload_retrys_, 0);
}

TEST_F(UnloadTabletProcedureTest, TestEventHandlers) {
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletReady);
  unload_proc_->UnloadTabletHandler(TabletEvent::kUnLoadTablet);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletUnloading);
  unload_proc_->WaitRpcResponseHandler(TabletEvent::kWaitRpcResponse);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletUnloading);

  unload_proc_->UnloadTabletSuccHandler(TabletEvent::kTsUnLoadSucc);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletOffline);
  tablet_->SetStatus(TabletMeta::kTabletUnloading);
  unload_proc_->UnloadTabletFailHandler(TabletEvent::kTsUnLoadFail);
  EXPECT_EQ(tablet_->GetStatus(), TabletMeta::kTabletUnloadFail);
  unload_proc_->EOFHandler(TabletEvent::kEofEvent);
}

// those AsycLifeCycle* test cases should cause core dump. Since unload_proc_ is
// dectructed, we cannot
// check its member fields
TEST_F(UnloadTabletProcedureTest, AsynLifeCycle_CallbackExceedProcedureCycle) {
  tablet_->SetStatus(TabletMeta::kTabletUnloading);
  tablet_->node_->state_ = kReady;
  unload_proc_->UnloadTabletAsync();
  unload_proc_.reset();
}

TEST_F(UnloadTabletProcedureTest, AsyncLifeCycleTest_DelayedUnloadTaskExeedProcedureCycle) {
  tablet_->SetStatus(TabletMeta::kTabletUnloading);
  tablet_->node_->state_ = kOffline;
  ThreadPool::Task task = std::bind(&UnloadTabletProcedure::UnloadTabletAsyncWrapper,
                                    std::weak_ptr<UnloadTabletProcedure>(unload_proc_));
  MasterEnv().GetThreadPool()->DelayTask(100, task);
  MasterEnv().GetThreadPool()->Start();
  EXPECT_EQ(MasterEnv().GetThreadPool()->latest_.size(), 1);
  unload_proc_.reset();
  usleep(200 * 1000);
  EXPECT_EQ(MasterEnv().GetThreadPool()->latest_.size(), 0);
  MasterEnv().GetThreadPool()->Stop(false);
}
}
}
