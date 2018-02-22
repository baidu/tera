// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/lb_entry.h"

#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include "load_balancer/lb_impl.h"
#include "load_balancer/lb_service_impl.h"

DECLARE_string(tera_lb_server_addr);
DECLARE_string(tera_lb_server_port);

std::string GetTeraEntryName() {
    return "load_balancer";
}

tera::TeraEntry* GetTeraEntry() {
    return new tera::load_balancer::LBEntry();
}

namespace tera {
namespace load_balancer {

LBEntry::LBEntry() :
    rpc_server_(nullptr),
    lb_service_impl_(nullptr),
    lb_impl_(nullptr) {
    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

LBEntry::~LBEntry() {
}

bool LBEntry::StartServer() {
    IpAddress lb_addr(FLAGS_tera_lb_server_addr, FLAGS_tera_lb_server_port);
    LOG(INFO) << "Start load balancer RPC server at: " << lb_addr.ToString();

    lb_impl_.reset(new LBImpl());
    lb_service_impl_ = new LBServiceImpl(lb_impl_);

    if (!lb_impl_->Init()) {
        return false;
    }

    rpc_server_->RegisterService(lb_service_impl_);
    if (!rpc_server_->Start(lb_addr.ToString())) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }

    LOG(INFO) << "finish starting load balancer server";
    return true;
}

bool LBEntry::Run() {
    ThisThread::Sleep(1000);
    return true;
}

void LBEntry::ShutdownServer() {
    rpc_server_->Stop();
}

} // namespace load_balancer
} // namespace tera

