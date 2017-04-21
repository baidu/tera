// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tso/tso_rpc_client.h"

#include "common/base/closure.h"

namespace tera {
namespace tso {

TsoRpcClient::TsoRpcClient(const std::string& server_addr,
                           common::ThreadPool* thread_pool,
                           int32_t rpc_timeout)
    : RpcClient<TimestampOracleServer::Stub>(server_addr),
      thread_pool_(thread_pool),
      rpc_timeout_(rpc_timeout) {}

TsoRpcClient::~TsoRpcClient() {}

bool TsoRpcClient::GetTimestamp(const GetTimestampRequest* request,
                                GetTimestampResponse* response,
                                Closure<void, GetTimestampRequest*, GetTimestampResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TimestampOracleServer::Stub::GetTimestamp, request, response,
                                done, "GetTimestamp", rpc_timeout_, thread_pool_);
}

} // namespace tso
} // namespace tera
