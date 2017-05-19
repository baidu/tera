// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TIMEORACLE_TIMEORACLE_CLIENT_H
#define TERA_TIMEORACLE_TIMEORACLE_CLIENT_H

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "proto/timeoracle_rpc.pb.h"
#include "proto/rpc_client.h"

DECLARE_int32(tera_rpc_timeout_period);

class ThreadPool;

namespace tera {
namespace timeoracle {

class TimeoracleClient : public RpcClient<TimeoracleServer::Stub> {
public:
    static void SetThreadPool(ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow = -1, int32_t max_outflow = -1,
                             int32_t pending_buffer_size = -1,
                             int32_t thread_num = -1);

    TimeoracleClient(const std::string& addr = "",
                     int32_t rpc_timeout = FLAGS_tera_rpc_timeout_period);

    ~TimeoracleClient();

    bool GetTimestamp(const GetTimestampRequest* request, GetTimestampResponse* response,
            std::function<void (GetTimestampRequest*, GetTimestampResponse*, bool, int)> done);
private:
    int32_t rpc_timeout_;
    static ThreadPool* thread_pool_;
};

} // namespace timeoracle
} // namespace tera

#endif // TERA_TIMEORACLE_TIMEORACLE_CLIENT_H
