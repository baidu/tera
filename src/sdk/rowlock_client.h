// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_ROWLOCK_CLIENT_H_
#define TERA_SDK_ROWLOCK_CLIENT_H_

#include <atomic>

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "common/mutex.h"
#include "observer/rowlocknode/zk_rowlock_client_zk_adapter.h"
#include "proto/rpc_client.h"
#include "proto/rowlocknode_rpc.pb.h"

namespace tera {
namespace observer {

class RowlockClientZkAdapter;

class RowlockStub : public tera::RpcClient<RowlockService::Stub> {
public:
    static void SetThreadPool(ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow = -1, int32_t max_outflow = -1,
            int32_t pending_buffer_size = -1,
            int32_t thread_num = -1);

    RowlockStub(const std::string& addr = "", int32_t rpc_timeout = 60000);
    ~RowlockStub();

    virtual bool TryLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

    virtual bool UnLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);


private:
    int32_t rpc_timeout_;
    static ThreadPool* thread_pool_;
};

class RowlockClient {
public:
    static void SetThreadPool(ThreadPool* thread_pool);
    
    RowlockClient(const std::string& addr = "", int32_t rpc_timeout = 60000);
    ~RowlockClient() {}

    virtual bool TryLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

    virtual bool UnLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL);

    void Update(const std::vector<std::string>& addrs);

private:
    void SetZkAdapter();

private:
    mutable Mutex client_mutex_;
    std::shared_ptr<RowlockStub> client_;
    std::unique_ptr<ZkRowlockClientZkAdapter> zk_adapter_;
    std::string local_addr_;
};

} // namespace observer
} // namespace tera
#endif  // TERA_SDK_ROWLOCK_CLIENT_H
