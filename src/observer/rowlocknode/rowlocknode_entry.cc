// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlocknode/rowlocknode_entry.h"

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
#include "common/timer.h"
#include "observer/rowlocknode/remote_rowlocknode.h"

DECLARE_string(rowlock_server_port);
DECLARE_int32(rowlock_io_service_pool_size);
DECLARE_int32(rowlock_rpc_work_thread_num);

std::string GetTeraEntryName() {
    return "rowlock";
}

tera::TeraEntry* GetTeraEntry() {
    return new tera::observer::RowlockNodeEntry();
}

namespace tera {
namespace observer {

RowlockNodeEntry::RowlockNodeEntry() : rowlocknode_impl_(NULL), remote_rowlocknode_(NULL) {
        sofa::pbrpc::RpcServerOptions rpc_options;
        rpc_options.max_throughput_in = -1;
        rpc_options.max_throughput_out = -1;
        rpc_options.work_thread_num = FLAGS_rowlock_rpc_work_thread_num;
        rpc_options.io_service_pool_size = FLAGS_rowlock_io_service_pool_size;
        rpc_options.no_delay = false;                   //use Nagle's Algorithm
        rpc_options.write_buffer_base_block_factor = 0; //64Bytes per malloc
        rpc_options.read_buffer_base_block_factor = 7;  //8kBytes per malloc
        rpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

RowlockNodeEntry::~RowlockNodeEntry() {}

bool RowlockNodeEntry::StartServer() {
    SetProcessorAffinity();
    IpAddress rowlocknode_addr("0.0.0.0", FLAGS_rowlock_server_port);
    LOG(INFO) << "Start RPC server at: " << rowlocknode_addr.ToString();
    rowlocknode_impl_.reset(new RowlockNodeImpl());
    remote_rowlocknode_ = new RemoteRowlockNode(rowlocknode_impl_.get());
    rpc_server_->RegisterService(remote_rowlocknode_);
    if (!rpc_server_->Start(rowlocknode_addr.ToString())) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }
    if (!rowlocknode_impl_->Init()) {
        LOG(ERROR) << "fail to init rowlocknode_impl";
        return false;
    }
    LOG(INFO) << "finish starting RPC server";

    return true;
}

void RowlockNodeEntry::ShutdownServer() {
    LOG(INFO) << "shut down server";
    rpc_server_->Stop();
    rowlocknode_impl_->Exit();
    rowlocknode_impl_.reset();
    LOG(INFO) << "RowlockNodeEntry stop done!";
}

bool RowlockNodeEntry::Run() {
    ThisThread::Sleep(3000);
    rowlocknode_impl_->PrintQPS();
    return true;
}

void RowlockNodeEntry::SetProcessorAffinity() {}

} // namespace observer
} // namespace tera
