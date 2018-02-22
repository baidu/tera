// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_LB_CLIENT_H_
#define TERA_LOAD_BALANCER_LB_CLIENT_H_

#include <string>

#include "proto/load_balancer_rpc.pb.h"
#include "proto/rpc_client.h"

DECLARE_int32(tera_rpc_timeout_period);

namespace tera {
namespace load_balancer {

class LBClient : public RpcClient<LoadBalancerService::Stub> {
public:
    LBClient(const std::string& server_addr = "",
             int32_t rpc_timeout = FLAGS_tera_rpc_timeout_period);
    virtual ~LBClient();

    virtual bool CmdCtrl(const CmdCtrlRequest* request,
                         CmdCtrlResponse* response);

private:
    int32_t rpc_timeout_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_LB_CLIENT_H_

