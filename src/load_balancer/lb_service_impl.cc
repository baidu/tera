// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/lb_service_impl.h"

#include <functional>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "load_balancer/lb_impl.h"
#include "utils/network_utils.h"

DECLARE_int32(tera_lb_server_thread_num);

namespace tera {
namespace load_balancer {

LBServiceImpl::LBServiceImpl(const std::shared_ptr<LBImpl>& lb_impl) :
    lb_impl_(lb_impl),
    thread_pool_(new ThreadPool(FLAGS_tera_lb_server_thread_num)) {
}

LBServiceImpl::~LBServiceImpl() {
}

void LBServiceImpl::CmdCtrl(google::protobuf::RpcController* controller,
                            const CmdCtrlRequest* request,
                            CmdCtrlResponse* response,
                            google::protobuf::Closure* done) {
    VLOG(20) << "accept RPC (CmdCtrl) from: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task task =
        std::bind(&LBServiceImpl::DoCmdCtrl, this, controller, request, response, done);
    thread_pool_->AddTask(task);
}

void LBServiceImpl::DoCmdCtrl(google::protobuf::RpcController* controller,
                              const CmdCtrlRequest* request,
                              CmdCtrlResponse* response,
                              google::protobuf::Closure* done) {
    VLOG(20) << "run RPC (CmdCtrl)";
    lb_impl_->CmdCtrl(request, response, done);
    VLOG(20) << "finish RPC (CmdCtrl)";
}

} // namespace load_balancer
} // namespace tera

