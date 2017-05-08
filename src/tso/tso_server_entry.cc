// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tso/tso_server_entry.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/net/ip_address.h"
#include "tso/tso_server_impl.h"

DEFINE_string(tera_tso_port, "50000", "port of timestamp oracle");
DECLARE_string(tera_tso_port);

std::string GetTeraEntryName() {
    return "tso";
}

tera::TeraEntry* GetTeraEntry() {
    return new tera::tso::TsoServerEntry();
}

namespace tera {
namespace tso {

TsoServerEntry::TsoServerEntry()
    : tso_server_impl_(NULL),
      rpc_server_(NULL) {
}

TsoServerEntry::~TsoServerEntry() {
    ShutdownServer();
}

bool TsoServerEntry::StartServer() {
    tso_server_impl_ = new TsoServerImpl;

    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_server_ = new sofa::pbrpc::RpcServer(rpc_options);
    rpc_server_->RegisterService(tso_server_impl_);

    IpAddress tso_addr("0.0.0.0", FLAGS_tera_tso_port);
    LOG(INFO) << "Start TSO RPC server at: " << tso_addr.ToString();
    if (!rpc_server_->Start(tso_addr.ToString())) {
        LOG(ERROR) << "start TSO RPC server error";
        return false;
    }

    LOG(INFO) << "finish starting TSO server";
    return true;
}

bool TsoServerEntry::Run() {
    static int64_t timer_ticks = 0;
    ++timer_ticks;
    usleep(1000);
    return true;
}

void TsoServerEntry::ShutdownServer() {
    if (rpc_server_ != NULL) {
        rpc_server_->Stop();
        delete rpc_server_;
        rpc_server_ = NULL;
    }
    if (tso_server_impl_ != NULL) {
        delete tso_server_impl_;
        tso_server_impl_ = NULL;
    }
}

} // namespace tso
} // namespace tera
