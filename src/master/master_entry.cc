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

namespace tera {
namespace master {

MasterEntry::MasterEntry()
    : m_master_impl(NULL),
      m_remote_master(NULL),
      m_rpc_server(NULL) {
    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_options.max_throughput_in = FLAGS_tera_master_rpc_server_max_inflow;
    rpc_options.max_throughput_out = FLAGS_tera_master_rpc_server_max_outflow;
    m_rpc_server.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

MasterEntry::~MasterEntry() {}

bool MasterEntry::StartServer() {
    IpAddress master_addr("0.0.0.0", FLAGS_tera_master_port);
    LOG(INFO) << "Start master RPC server at: " << master_addr.ToString();

    m_master_impl.reset(new MasterImpl());
    m_remote_master = new RemoteMaster(m_master_impl.get());

    if (!m_master_impl->Init()) {
        return false;
    }

    m_rpc_server->RegisterService(m_remote_master);
    if (!m_rpc_server->Start(master_addr.ToString())) {
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
        LOG(INFO) << "[ThreadPool schd/task/cnt] " << m_master_impl->ProfilingLog();
    }

    ThisThread::Sleep(1000);
    return true;
}

void MasterEntry::ShutdownServer() {
    m_rpc_server->Stop();
    m_master_impl.reset();
}

} // namespace master
} // namespace tera
