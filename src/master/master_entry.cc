// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/master_entry.h"

#include "common/net/ip_address.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master/master_impl.h"
#include "master/remote_master.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_master_addr);
DECLARE_string(tera_master_port);

namespace tera {
namespace master {

MasterEntry::MasterEntry()
    : m_master_impl(NULL),
      m_remote_master(NULL),
      m_rpc_server(m_rpc_options) {}

MasterEntry::~MasterEntry() {}

bool MasterEntry::StartServer() {
    IpAddress master_addr(utils::GetLocalHostAddr(), FLAGS_tera_master_port);
    FLAGS_tera_master_addr = master_addr.ToString();
    LOG(INFO) << "Start master RPC server at: " << FLAGS_tera_master_addr;

    m_master_impl.reset(new MasterImpl());
    m_remote_master = new RemoteMaster(m_master_impl.get());

    if (!m_master_impl->Init()) {
        return false;
    }

    m_rpc_server.RegisterService(m_remote_master);
    if (!m_rpc_server.Start(FLAGS_tera_master_addr)) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }

    LOG(INFO) << "finish starting master server";
    return true;
}

void MasterEntry::ShutdownServer() {
    m_rpc_server.Stop();
    m_master_impl.reset();
}

} // namespace master
} // namespace tera
