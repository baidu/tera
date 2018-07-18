// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/rowlock_client.h"

#include <stdlib.h>
#include <time.h>

#include "gflags/gflags.h"

#include "observer/rowlocknode/ins_rowlock_client_zk_adapter.h"
#include "proto/rowlocknode_rpc.pb.h"
#include "types.h"
#include "utils/utils_cmd.h"

DECLARE_string(rowlock_server_port);
DECLARE_string(tera_coord_type);
DECLARE_bool(rowlock_test);
DECLARE_int32(rowlock_client_max_fail_times);
DECLARE_bool(mock_rowlock_enable);

namespace tera{
namespace observer {

ThreadPool* RowlockStub::thread_pool_ = NULL;

void RowlockStub::SetThreadPool(ThreadPool* thread_pool) {
    thread_pool_ = thread_pool;
}

void RowlockStub::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
        int32_t pending_buffer_size, int32_t thread_num) {
    tera::RpcClientBase::SetOption(max_inflow, max_outflow,
            pending_buffer_size, thread_num);
}

RowlockStub::RowlockStub(const std::string& server_addr,
        int32_t rpc_timeout)
    : tera::RpcClient<RowlockService::Stub>(server_addr),
      rpc_timeout_(rpc_timeout) {
}

RowlockStub::~RowlockStub() {}

bool RowlockStub::TryLock(const RowlockRequest* request,
        RowlockResponse* response,
        std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done) {
    return SendMessageWithRetry(&RowlockService::Stub::Lock,
            request, response, done, "TryLock",
            rpc_timeout_, thread_pool_);
}

bool RowlockStub::UnLock(const RowlockRequest* request,
        RowlockResponse* response,
        std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done) {
    return SendMessageWithRetry(&RowlockService::Stub::UnLock,
            request, response, done, "UnLock",
            rpc_timeout_, thread_pool_);
}

bool RowlockClient::init_ = false;
std::string RowlockClient::server_addr_ = "";

void RowlockClient::SetThreadPool(ThreadPool* thread_pool) {
    RowlockStub::SetThreadPool(thread_pool);
}

RowlockClient::RowlockClient(const std::string& addr, int32_t rpc_timeout)
    : local_addr_(tera::utils::GetLocalHostName() + ":" + FLAGS_rowlock_server_port) {
    srand((unsigned int)(time(NULL)));

    if (FLAGS_mock_rowlock_enable == true) {
        return;
    }
    if (init_ == false) {
        SetZkAdapter();
        init_ = true;
    } else {
        std::vector<std::string> init_addrs;
        init_addrs.push_back(server_addr_);
        Update(init_addrs);
    }
    
}

void RowlockClient::Update(const std::vector<std::string>& addrs) {
    std::string addr = addrs[rand() % addrs.size()];
    std::shared_ptr<RowlockStub> client(new RowlockStub(addr));

    LOG(INFO) << "Update rowlock client ip: " << addr;

    MutexLock locker(&client_mutex_);
    server_addr_ = addr;
    client_.swap(client);
}

bool RowlockClient::TryLock(const RowlockRequest* request,
            RowlockResponse* response,
            std::function<void (const RowlockRequest*, RowlockResponse*, bool, int)> done) {
    std::shared_ptr<RowlockStub> client;
    {
        MutexLock locker(&client_mutex_);
        // COW ref +1
        client = client_;
    }
    for (int32_t i = 0; i < FLAGS_rowlock_client_max_fail_times; ++i) {
        bool ret = client->TryLock(request, response, done);
        if (ret) {
            return true;
        }
        LOG(WARNING) << "try lock fail: " << request->row();  
    }
    // rpc fail
    SetZkAdapter();
    return false;
}

bool RowlockClient::UnLock(const RowlockRequest* request,
        RowlockResponse* response,
        std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done) {
    std::shared_ptr<RowlockStub> client;
    {
        MutexLock locker(&client_mutex_);
        // copy-on-write ref+1
        client = client_;
    }
    for (int32_t i = 0; i < FLAGS_rowlock_client_max_fail_times; ++i) {
        bool ret = client->UnLock(request, response, done);
        if (ret) {
            return true;
        }
        // rpc fail
        SetZkAdapter();
    }
    return false;
}

void RowlockClient::SetZkAdapter() {
    // mock rowlock, do not need a real zk adapter
    if (FLAGS_mock_rowlock_enable == true) {
        return;
    }

    if (FLAGS_tera_coord_type == "zk") {
        zk_adapter_.reset(new ZkRowlockClientZkAdapter(this, local_addr_));
    } else if (FLAGS_tera_coord_type == "ins") {
        zk_adapter_.reset(new InsRowlockClientZkAdapter(this, local_addr_));
    } else {
        LOG(ERROR) << "Unknow coord type for rowlock client";
        return;
    }

    zk_adapter_->Init();
}

} // namespace observer
} // namespace tera
