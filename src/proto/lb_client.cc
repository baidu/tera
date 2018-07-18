// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>

#include "gflags/gflags.h"

#include "proto/lb_client.h"

DECLARE_int32(tera_master_connect_retry_times);
DECLARE_int32(tera_master_connect_retry_period);
DECLARE_int32(tera_master_connect_timeout_period);

namespace tera {
namespace load_balancer {

LBClient::LBClient(const std::string& server_addr,
                   int32_t rpc_timeout)
    : RpcClient<LoadBalancerService::Stub>(server_addr),
    rpc_timeout_(rpc_timeout) {
}

LBClient::~LBClient() {
}

bool LBClient::CmdCtrl(const CmdCtrlRequest* request,
                       CmdCtrlResponse* response) {
    return SendMessageWithRetry(&LoadBalancerService::Stub::CmdCtrl,
                                request, response,
                                (std::function<void (CmdCtrlRequest*, CmdCtrlResponse*, bool, int)>)NULL,
                                "CmdCtrl", rpc_timeout_);
}

} // namespace load_balancer
} // namespace tera

