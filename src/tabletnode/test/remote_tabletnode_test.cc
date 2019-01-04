// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <memory>
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "tabletnode/remote_tabletnode.h"
#include "tabletnode/tabletnode_impl.h"

DECLARE_int32(tera_tabletnode_ctrl_thread_num);
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace tabletnode {
namespace test {

class MockClosure : public google::protobuf::Closure {
 public:
  MockClosure() {}
  virtual ~MockClosure() {}
  void Run() {}
};

class RemoteTabletNodeTest : public ::testing::Test {
 public:
  RemoteTabletNodeTest() {
    FLAGS_tera_leveldb_env_type = "local";
    FLAGS_tera_tabletnode_path_prefix = "./remote_tabletnode_test";
    tabletnode_impl_.reset(new TabletNodeImpl);
    FLAGS_tera_tabletnode_ctrl_thread_num = 1;
    remote_ts_.reset(new RemoteTabletNode(tabletnode_impl_.get()));
  }

  virtual ~RemoteTabletNodeTest() {}
  virtual void SetUp() {
    remote_ts_->ctrl_thread_pool_->Stop(true);
    remote_ts_->lightweight_ctrl_thread_pool_->Stop(true);
  }
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  std::unique_ptr<TabletNodeImpl> tabletnode_impl_;
  std::unique_ptr<RemoteTabletNode> remote_ts_;
};

int main(int argc, char* argv[]) {
  ::google::InitGoogleLogging(argv[0]);
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(RemoteTabletNodeTest, LoadTablet) {
  std::unique_ptr<LoadTabletRequest> request(new LoadTabletRequest);
  request->set_path("test/tablet00000001");
  std::unique_ptr<LoadTabletResponse> response(new LoadTabletResponse);
  std::unique_ptr<MockClosure> done(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller(new sofa::pbrpc::RpcController);
  remote_ts_->LoadTablet(controller.get(), request.get(), response.get(), done.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 1);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[request->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitLoad);

  std::unique_ptr<LoadTabletRequest> request4(new LoadTabletRequest);
  request4->set_path("test/tablet00000001");
  std::unique_ptr<LoadTabletResponse> response4(new LoadTabletResponse);
  std::unique_ptr<MockClosure> done4(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller4(new sofa::pbrpc::RpcController);
  remote_ts_->LoadTablet(controller4.get(), request4.get(), response4.get(), done4.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 1);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[request4->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitLoad);
  EXPECT_EQ(remote_ts_->lightweight_ctrl_thread_pool_->queue_.size(), 1);

  std::unique_ptr<LoadTabletRequest> request2(new LoadTabletRequest);
  request2->set_path("test/tablet00000002");
  std::unique_ptr<LoadTabletResponse> response2(new LoadTabletResponse);
  std::unique_ptr<MockClosure> done2(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller2(new sofa::pbrpc::RpcController);
  remote_ts_->LoadTablet(controller2.get(), request2.get(), response2.get(), done2.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[request2->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitLoad);

  std::unique_ptr<LoadTabletRequest> request3(new LoadTabletRequest);
  request3->set_path("test/tablet00000003");
  std::unique_ptr<LoadTabletResponse> response3(new LoadTabletResponse);
  std::unique_ptr<MockClosure> done3(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller3(new sofa::pbrpc::RpcController);
  remote_ts_->LoadTablet(controller3.get(), request3.get(), response3.get(), done3.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);
  EXPECT_TRUE(remote_ts_->tablets_ctrl_status_.find(request3->path()) ==
              remote_ts_->tablets_ctrl_status_.end());
  EXPECT_EQ(response3->status(), kTabletNodeIsBusy);

  remote_ts_->ctrl_thread_pool_->Start();
  remote_ts_->ctrl_thread_pool_->Stop(true);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 0);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 0);
}

TEST_F(RemoteTabletNodeTest, UnloadTablet) {
  std::unique_ptr<UnloadTabletRequest> request(new UnloadTabletRequest);
  std::unique_ptr<UnloadTabletResponse> response(new UnloadTabletResponse);
  std::unique_ptr<MockClosure> done(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller(new sofa::pbrpc::RpcController);
  request->set_path("test/tablet00000001");
  remote_ts_->UnloadTablet(controller.get(), request.get(), response.get(), done.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 1);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[request->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitUnload);

  std::unique_ptr<UnloadTabletRequest> request_2(new UnloadTabletRequest);
  std::unique_ptr<UnloadTabletResponse> response_2(new UnloadTabletResponse);
  std::unique_ptr<MockClosure> done_2(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller_2(new sofa::pbrpc::RpcController);
  request_2->set_path("test/tablet00000001");
  remote_ts_->UnloadTablet(controller_2.get(), request_2.get(), response_2.get(), done_2.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 1);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[request_2->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitUnload);
  EXPECT_EQ(remote_ts_->lightweight_ctrl_thread_pool_->queue_.size(), 1);

  std::unique_ptr<UnloadTabletRequest> request_3(new UnloadTabletRequest);
  std::unique_ptr<UnloadTabletResponse> response_3(new UnloadTabletResponse);
  std::unique_ptr<MockClosure> done_3(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller_3(new sofa::pbrpc::RpcController);
  request_3->set_path("test/tablet00000002");
  remote_ts_->UnloadTablet(controller_3.get(), request_3.get(), response_3.get(), done_3.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[request_3->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitUnload);

  std::unique_ptr<UnloadTabletRequest> request_4(new UnloadTabletRequest);
  std::unique_ptr<UnloadTabletResponse> response_4(new UnloadTabletResponse);
  std::unique_ptr<MockClosure> done_4(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> controller_4(new sofa::pbrpc::RpcController);
  request_4->set_path("test/tablet00000003");
  remote_ts_->UnloadTablet(controller_4.get(), request_4.get(), response_4.get(), done_4.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);
  EXPECT_TRUE(remote_ts_->tablets_ctrl_status_.find(request_4->path()) ==
              remote_ts_->tablets_ctrl_status_.end());
  EXPECT_EQ(response_4->status(), kTabletNodeIsBusy);

  remote_ts_->ctrl_thread_pool_->Start();
  remote_ts_->ctrl_thread_pool_->Stop(true);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 0);
}

TEST_F(RemoteTabletNodeTest, LoadAndUnloadTablet) {
  std::unique_ptr<LoadTabletRequest> load_req(new LoadTabletRequest);
  std::unique_ptr<LoadTabletResponse> load_resp(new LoadTabletResponse);
  std::unique_ptr<MockClosure> load_done(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> load_ctrl(new sofa::pbrpc::RpcController);
  load_req->set_path("test/tablet00000001");
  remote_ts_->LoadTablet(load_ctrl.get(), load_req.get(), load_resp.get(), load_done.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 1);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 1);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[load_req->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitLoad);

  std::unique_ptr<UnloadTabletRequest> unload_req(new UnloadTabletRequest);
  std::unique_ptr<UnloadTabletResponse> unload_resp(new UnloadTabletResponse);
  std::unique_ptr<MockClosure> unload_done(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> unload_ctrl(new sofa::pbrpc::RpcController);
  unload_req->set_path("test/tablet00000002");
  remote_ts_->UnloadTablet(unload_ctrl.get(), unload_req.get(), unload_resp.get(),
                           unload_done.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_[unload_req->path()],
            RemoteTabletNode::TabletCtrlStatus::kCtrlWaitUnload);

  std::unique_ptr<LoadTabletRequest> load_req_2(new LoadTabletRequest);
  std::unique_ptr<LoadTabletResponse> load_resp_2(new LoadTabletResponse);
  std::unique_ptr<MockClosure> load_done_2(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> load_ctrl_2(new sofa::pbrpc::RpcController);
  load_req_2->set_path("test/tablet00000002");
  remote_ts_->LoadTablet(load_ctrl_2.get(), load_req_2.get(), load_resp_2.get(), load_done_2.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);
  EXPECT_EQ(remote_ts_->lightweight_ctrl_thread_pool_->queue_.size(), 1);

  std::unique_ptr<UnloadTabletRequest> unload_req_2(new UnloadTabletRequest);
  std::unique_ptr<UnloadTabletResponse> unload_resp_2(new UnloadTabletResponse);
  std::unique_ptr<MockClosure> unload_done_2(new MockClosure);
  std::unique_ptr<google::protobuf::RpcController> unload_ctrl_2(new sofa::pbrpc::RpcController);
  unload_req_2->set_path("test/tablet00000001");
  remote_ts_->UnloadTablet(unload_ctrl_2.get(), unload_req_2.get(), unload_resp_2.get(),
                           unload_done_2.get());
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->queue_.size(), 2);
  EXPECT_EQ(remote_ts_->ctrl_thread_pool_->PendingNum(), 2);
  EXPECT_EQ(remote_ts_->tablets_ctrl_status_.size(), 2);

  EXPECT_EQ(remote_ts_->lightweight_ctrl_thread_pool_->queue_.size(), 2);
  remote_ts_->lightweight_ctrl_thread_pool_->Start();
  remote_ts_->lightweight_ctrl_thread_pool_->Stop(true);
  EXPECT_EQ(remote_ts_->lightweight_ctrl_thread_pool_->queue_.size(), 0);
  EXPECT_EQ(load_resp_2->status(), kTabletWaitUnload);
  EXPECT_EQ(unload_resp_2->status(), kTabletWaitLoad);
}
}
}
}
