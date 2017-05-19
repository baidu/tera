// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/timeoracle_client.h"

namespace tera {
namespace timeoracle {

ThreadPool* TimeoracleClient::thread_pool_ = NULL;

void TimeoracleClient::SetThreadPool(ThreadPool* thread_pool) {
    thread_pool_ = thread_pool;
}

void TimeoracleClient::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                         int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientBase::SetOption(max_inflow, max_outflow,
                             pending_buffer_size, thread_num);
}

TimeoracleClient::TimeoracleClient(const std::string& server_addr,
                                             int32_t rpc_timeout)
    : RpcClient<TimeoracleServer::Stub>(server_addr),
      rpc_timeout_(rpc_timeout) {}

TimeoracleClient::~TimeoracleClient() {}

bool TimeoracleClient::GetTimestamp(const GetTimestampRequest* request,
                               GetTimestampResponse* response,
                               std::function<void (GetTimestampRequest*, GetTimestampResponse*, bool, int)> done) {
    return SendMessageWithRetry(&TimeoracleServer::Stub::GetTimestamp,
                                request, response, done, "GetTimestamp",
                                rpc_timeout_, thread_pool_);
}

} // namespace timeoracle
} // namespace tera
