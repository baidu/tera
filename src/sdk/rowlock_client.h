// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>

#include <sofa/pbrpc/pbrpc.h>

#include "common/mutex.h"
#include "proto/rpc_client.h"
#include "proto/rowlocknode_rpc.pb.h"
#include "sdk/rowlock_client_zk_adapter.h"

namespace tera {
namespace observer {

class ZkRowlockClientZkAdapter;

class RowlockStub : public tera::RpcClient<RowlockService::Stub> {
 public:
  static void SetThreadPool(ThreadPool* thread_pool);

  static void SetRpcOption(int32_t max_inflow = -1, int32_t max_outflow = -1,
                           int32_t pending_buffer_size = -1, int32_t thread_num = -1);

  RowlockStub(const std::string& addr = "", int32_t rpc_timeout = 60000);
  ~RowlockStub();

  virtual bool TryLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

  virtual bool UnLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

 private:
  int32_t rpc_timeout_;
  static ThreadPool* thread_pool_;
};

class RowlockClient {
 public:
  static void SetThreadPool(ThreadPool* thread_pool);

  RowlockClient(const std::string& addr = "", int32_t rpc_timeout = 60000);
  ~RowlockClient() {}

  virtual bool TryLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(const RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

  virtual bool UnLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

  void Update(const std::vector<std::string>& addrs);

 private:
  void SetZkAdapter();

 private:
  mutable Mutex client_mutex_;
  std::shared_ptr<RowlockStub> client_;
  std::unique_ptr<ZkRowlockClientZkAdapter> zk_adapter_;
  std::string local_addr_;
  static bool init_;
  static std::string server_addr_;
};

class FakeRowlockClient : public RowlockClient {
 public:
  FakeRowlockClient() : RowlockClient("127.0.0.1:22222"){};
  ~FakeRowlockClient() {}

  virtual bool TryLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(const RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
    response->set_lock_status(kLockSucc);
    if (done != NULL) {
      bool failed = true;
      int error_code = 0;
      done(request, response, failed, error_code);
    }
    return true;
  }

  virtual bool UnLock(
      const RowlockRequest* request, RowlockResponse* response,
      std::function<void(RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
    response->set_lock_status(kLockSucc);

    return true;
  }
};

}  // namespace observer
}  // namespace tera
