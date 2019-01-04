// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sofa/pbrpc/pbrpc.h>

#include "observer/rowlockproxy/remote_rowlock_proxy.h"
#include "observer/rowlockproxy/rowlock_proxy_impl.h"
#include "proto/rpc_client.h"
#include "sdk/rowlock_client.h"
#include "utils/utils_cmd.h"

class TestClosure : public google::protobuf::Closure {
 public:
  TestClosure() {}
  virtual void Run() {}
};

namespace tera {
namespace observer {

class TestClient : public RowlockStub {
 public:
  TestClient() : RowlockStub("127.0.0.1:22222"){};
  ~TestClient() {}

  virtual bool TryLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
    response->set_lock_status(kLockSucc);
    return true;
  }

  virtual bool UnLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
    response->set_lock_status(kLockSucc);
    return true;
  }
};

TEST(RowlockProxyTest, ValueTest) {
  RowlockProxyImpl rowlock_proxy_impl;

  rowlock_proxy_impl.SetServerNumber(100);
  EXPECT_EQ(100, rowlock_proxy_impl.server_number_);
  EXPECT_EQ(100, rowlock_proxy_impl.GetServerNumber());

  rowlock_proxy_impl.SetServerNumber(1000);
  EXPECT_EQ(1000, rowlock_proxy_impl.server_number_);
  EXPECT_EQ(1000, rowlock_proxy_impl.GetServerNumber());

  EXPECT_EQ(std::hash<std::string>()("tablerow"), rowlock_proxy_impl.GetRowKey("table", "row"));

  EXPECT_EQ((*rowlock_proxy_impl.server_addrs_)[0], rowlock_proxy_impl.ScheduleRowKey(0));
  EXPECT_EQ((*rowlock_proxy_impl.server_addrs_)[1], rowlock_proxy_impl.ScheduleRowKey(1));
}

}  // namespace observer
}  // namespace tera
