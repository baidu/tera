// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "timeoracle/timeoracle_entry.h"

#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "utils/utils_cmd.h"

#include "timeoracle/remote_timeoracle.h"
#include "timeoracle/timeoracle_zk_adapter.h"
#include <iostream>

DECLARE_string(tera_timeoracle_port);
DECLARE_string(tera_local_addr);
DECLARE_int32(tera_timeoracle_refresh_lease_second);
DECLARE_int32(tera_timeoracle_max_lease_second);

namespace tera {
namespace timeoracle {

TimeoracleEntry::TimeoracleEntry() : remote_timeoracle_(nullptr), need_quit_(false) {
    sofa::pbrpc::RpcServerOptions rpc_options;
    //rpc_options.max_throughput_in = FLAGS_tera_master_sofa_pbrpc_server_max_inflow;
    //rpc_options.max_throughput_out = FLAGS_tera_master_sofa_pbrpc_server_max_outflow;
    sofa_pbrpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));

    if (FLAGS_tera_local_addr.empty()) {
        local_addr_ = utils::GetLocalHostName()+ ":" + FLAGS_tera_timeoracle_port;
    } else {
        local_addr_ = FLAGS_tera_local_addr + ":" + FLAGS_tera_timeoracle_port;
    }
}

bool TimeoracleEntry::Start() {
    if (!InitZKAdaptor()) {
        return false;
    }

    if (!StartServer()) {
        return false;
    }

    return true;
}

TimeoracleEntry::~TimeoracleEntry() {
    need_quit_ = true;
    if (lease_thread_.joinable()) {
        lease_thread_.join();
    }
}

bool TimeoracleEntry::InitZKAdaptor() {
    startup_timestamp_ = 0;
    //return true;
    zk_adapter_.reset(new TimeoracleZkAdapter(local_addr_));
    return zk_adapter_->Init(&startup_timestamp_);
}

bool TimeoracleEntry::StartServer() {
    IpAddress timeoracle_addr("0.0.0.0", FLAGS_tera_timeoracle_port);
    LOG(INFO) << "Start timeoracle RPC server at: " << timeoracle_addr.ToString();

    remote_timeoracle_ = new RemoteTimeoracle(startup_timestamp_);
    std::thread lease_thread(&TimeoracleEntry::LeaseThread, this);
    lease_thread_ = std::move(lease_thread);


    auto timeoracle = remote_timeoracle_->GetTimeoracle();

    while (!timeoracle->GetLimitTimestamp()) {
        if (need_quit_) {
            return false;
        }
        ThisThread::Sleep(100);
    }

    sofa_pbrpc_server_->RegisterService(remote_timeoracle_);
    if (!sofa_pbrpc_server_->Start(timeoracle_addr.ToString())) {
        LOG(ERROR) << "start timeoracle RPC server error";
        return false;
    }

    LOG(INFO) << "finish start timeoracle RPC server";
    return true;
}

bool TimeoracleEntry::Run() {
    if (need_quit_) {
        return false;
    }

    remote_timeoracle_->GetTimeoracle()->UpdateStartTimestamp();

    ThisThread::Sleep(1000);
    return true;
}

void TimeoracleEntry::ShutdownServer() {
    need_quit_ = true;
    sofa_pbrpc_server_->Stop();
}

void TimeoracleEntry::LeaseThread() {
    auto timeoracle = remote_timeoracle_->GetTimeoracle();

    while (!need_quit_) {
        uint64_t start_timestamp = timeoracle->GetStartTimestamp();
        uint64_t limit_timestamp = timeoracle->GetLimitTimestamp();
        uint64_t refresh_lease_timestamp =
            FLAGS_tera_timeoracle_refresh_lease_second * kNanoPerSecond;

        if (start_timestamp + refresh_lease_timestamp >= limit_timestamp) {
            // need to require lease
            if (limit_timestamp < start_timestamp) {
                limit_timestamp = start_timestamp;
            }

            uint64_t next_limit_timestamp =
                limit_timestamp + FLAGS_tera_timeoracle_max_lease_second * kNanoPerSecond;

            if (!zk_adapter_->UpdateTimestamp(next_limit_timestamp)) {
                need_quit_ = true;
                return;
            }

            timeoracle->UpdateLimitTimestamp(next_limit_timestamp);
        }

        ThisThread::Sleep(1000);
    }
}

} // namespace timeoracle
} // namespace tera
