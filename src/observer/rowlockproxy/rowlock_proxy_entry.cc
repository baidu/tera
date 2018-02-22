// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlockproxy/rowlock_proxy_entry.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include "common/thread_attributes.h"
#include "common/timer.h"
#include "common/counter.h"
#include "utils/rpc_timer_list.h"
#include "observer/rowlockproxy/remote_rowlock_proxy.h"

DECLARE_string(rowlock_proxy_port);
DECLARE_int32(rowlock_io_service_pool_size);
DECLARE_int32(rowlock_rpc_work_thread_num);

std::string GetTeraEntryName() {
    return "rowlock_proxy";
}

tera::TeraEntry* GetTeraEntry() {
    return new tera::observer::RowlockProxyEntry();
}

namespace tera {
namespace observer {

RowlockProxyEntry::RowlockProxyEntry() {
        sofa::pbrpc::RpcServerOptions rpc_options;
        rpc_options.max_throughput_in = -1;
        rpc_options.max_throughput_out = -1;
        rpc_options.work_thread_num = FLAGS_rowlock_rpc_work_thread_num;
        rpc_options.io_service_pool_size = FLAGS_rowlock_io_service_pool_size;
        rpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

RowlockProxyEntry::~RowlockProxyEntry() {}

bool RowlockProxyEntry::StartServer() {
    IpAddress rowlock_proxy_addr("0.0.0.0", FLAGS_rowlock_proxy_port);
    LOG(INFO) << "Start RPC server at: " << rowlock_proxy_addr.ToString();
    rowlock_proxy_impl_.reset(new RowlockProxyImpl());
    remote_rowlock_proxy_ = new RemoteRowlockProxy(rowlock_proxy_impl_.get());
    rpc_server_->RegisterService(remote_rowlock_proxy_);
    if (!rpc_server_->Start(rowlock_proxy_addr.ToString())) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }
    if (!rowlock_proxy_impl_->Init()) {
        LOG(ERROR) << "fail to init rowlocknode_impl";
        return false;
    }
    LOG(INFO) << "finish starting RPC server";

    return true;
}

void RowlockProxyEntry::ShutdownServer() {
    LOG(INFO) << "shut down server";
    rpc_server_->Stop();

    LOG(INFO) << "RowlockProxyEntry stop done!";
    _exit(0);
}

bool RowlockProxyEntry::Run() {
    ThisThread::Sleep(1000);
    return true;
}

} // namespace observer
} // namespace tera