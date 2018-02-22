// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_LB_SERVICE_IMPL_H_
#define TERA_LOAD_BALANCER_LB_SERVICE_IMPL_H_

#include <memory>

#include "common/thread_pool.h"
#include "proto/load_balancer_rpc.pb.h"

namespace tera {
namespace load_balancer {

class LBImpl;

class LBServiceImpl: public LoadBalancerService {
public:
    explicit LBServiceImpl(const std::shared_ptr<LBImpl>& lb_impl);
    virtual ~LBServiceImpl();

    void CmdCtrl(google::protobuf::RpcController* controller,
                 const CmdCtrlRequest* request,
                 CmdCtrlResponse* response,
                 google::protobuf::Closure* done);

private:
    void DoCmdCtrl(google::protobuf::RpcController* controller,
                   const CmdCtrlRequest* request,
                   CmdCtrlResponse* response,
                   google::protobuf::Closure* done);

private:
    std::shared_ptr<LBImpl> lb_impl_;
    std::unique_ptr<ThreadPool> thread_pool_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_LB_SERVICE_IMPL_H_
