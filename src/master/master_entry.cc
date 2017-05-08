// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/master_entry.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/net/ip_address.h"
#include "master/master_impl.h"
#include "master/remote_master.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_master_port);
DECLARE_int32(tera_master_rpc_server_max_inflow);
DECLARE_int32(tera_master_rpc_server_max_outflow);

std::string GetTeraEntryName() {
    return "master";
}

tera::TeraEntry* GetTeraEntry() {
    return new tera::master::MasterEntry();
}

namespace tera {
namespace master {

MasterEntry::MasterEntry()
    : master_impl_(NULL),
      remote_master_(NULL),
      rpc_server_(NULL) {
    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_options.max_throughput_in = FLAGS_tera_master_rpc_server_max_inflow;
    rpc_options.max_throughput_out = FLAGS_tera_master_rpc_server_max_outflow;
    rpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

MasterEntry::~MasterEntry() {}

bool MasterEntry::StartServer() {
    IpAddress master_addr("0.0.0.0", FLAGS_tera_master_port);
    LOG(INFO) << "Start master RPC server at: " << master_addr.ToString();

    master_impl_.reset(new MasterImpl());
    remote_master_ = new RemoteMaster(master_impl_.get());

    if (!master_impl_->Init()) {
        return false;
    }

    rpc_server_->RegisterService(remote_master_);
    if (!rpc_server_->Start(master_addr.ToString())) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }

    LOG(INFO) << "finish starting master server";
    return true;
}

bool MasterEntry::Run() {
    static int64_t timer_ticks = 0;
    ++timer_ticks;

    if (timer_ticks % 10 == 0) {
        LOG(INFO) << "[ThreadPool schd/task/cnt] " << master_impl_->ProfilingLog();
    }

    ThisThread::Sleep(1000);
    return true;
}

void MasterEntry::ShutdownServer() {
    rpc_server_->Stop();
    master_impl_.reset();
}

} // namespace master
} // namespace tera
