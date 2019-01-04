// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "timeoracle/timeoracle_entry.h"

#include <iostream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include "utils/utils_cmd.h"

#include "timeoracle/remote_timeoracle.h"
#include "timeoracle/timeoracle_zk_adapter.h"

DECLARE_string(tera_local_addr);
DECLARE_string(tera_timeoracle_port);
DECLARE_int32(tera_timeoracle_refresh_lease_second);
DECLARE_int32(tera_timeoracle_max_lease_second);
DECLARE_bool(tera_timeoracle_mock_enabled);
DECLARE_int32(tera_timeoracle_work_thread_num);
DECLARE_int32(tera_timeoracle_io_service_pool_size);
DECLARE_string(tera_coord_type);

namespace tera {
namespace timeoracle {

TimeoracleEntry::TimeoracleEntry()
    : remote_timeoracle_(nullptr), startup_timestamp_(0), need_quit_(false) {
  sofa::pbrpc::RpcServerOptions rpc_options;
  rpc_options.work_thread_num = FLAGS_tera_timeoracle_work_thread_num;
  rpc_options.io_service_pool_size = FLAGS_tera_timeoracle_io_service_pool_size;
  rpc_options.no_delay = false;                    // use Nagle's Algorithm
  rpc_options.write_buffer_base_block_factor = 0;  // 64Bytes per malloc
  rpc_options.read_buffer_base_block_factor = 7;   // 8kBytes per malloc
  sofa_pbrpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));

  if (FLAGS_tera_local_addr.empty()) {
    local_addr_ = utils::GetLocalHostName() + ":" + FLAGS_tera_timeoracle_port;
  } else {
    local_addr_ = FLAGS_tera_local_addr + ":" + FLAGS_tera_timeoracle_port;
  }
}

bool TimeoracleEntry::Start() {
  if (!InitZKAdaptor()) {
    return false;
  }

  int64_t current_timestamp = Timeoracle::CurrentTimestamp();
  if (startup_timestamp_ < current_timestamp) {
    startup_timestamp_ = current_timestamp;
  } else {
    LOG(WARNING) << "startup timestamp big than current timestamp,"
                 << "startup timestamp is " << startup_timestamp_ << "current timestamp is "
                 << current_timestamp;
  }

  LOG(INFO) << "set startup timestamp to " << startup_timestamp_;

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
  if (FLAGS_tera_timeoracle_mock_enabled) {
    LOG(INFO) << "mock mode";
    zk_adapter_.reset(new TimeoracleMockAdapter(local_addr_));
  } else if (FLAGS_tera_coord_type == "zk") {
    LOG(INFO) << "zk mode";
    zk_adapter_.reset(new TimeoracleZkAdapter(local_addr_));
  } else if (FLAGS_tera_coord_type == "ins") {
    LOG(INFO) << "ins mode";
    zk_adapter_.reset(new TimeoracleInsAdapter(local_addr_));
  } else {
    LOG(FATAL) << "invalid configure for coord service, please check "
               << "--tera_timeoracle_mock_enabled=true or "
               << "--tera_coord_type=zk|ins";
    assert(0);
  }

  return zk_adapter_->Init(&startup_timestamp_);
}

bool TimeoracleEntry::StartServer() {
  IpAddress timeoracle_addr("0.0.0.0", FLAGS_tera_timeoracle_port);
  LOG(INFO) << "Start timeoracle RPC server at: " << timeoracle_addr.ToString();

  remote_timeoracle_ = new RemoteTimeoracle(startup_timestamp_);
  std::thread lease_thread(&TimeoracleEntry::LeaseThread, this);
  lease_thread_ = std::move(lease_thread);

  auto timeoracle = remote_timeoracle_->GetTimeoracle();

  while (startup_timestamp_ < timeoracle->GetLimitTimestamp()) {
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

  int64_t start_timestamp = remote_timeoracle_->GetTimeoracle()->UpdateStartTimestamp();

  VLOG(100) << "adjust start timestamp finished, start timestmap is " << start_timestamp;

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
    int64_t start_timestamp = timeoracle->GetStartTimestamp();
    int64_t limit_timestamp = timeoracle->GetLimitTimestamp();
    int64_t refresh_lease_timestamp =
        FLAGS_tera_timeoracle_refresh_lease_second * kTimestampPerSecond;

    if (start_timestamp + refresh_lease_timestamp >= limit_timestamp) {
      // need to require lease
      if (limit_timestamp < start_timestamp) {
        limit_timestamp = start_timestamp;
      }

      int64_t next_limit_timestamp =
          limit_timestamp + FLAGS_tera_timeoracle_max_lease_second * kTimestampPerSecond;

      if (!zk_adapter_->UpdateTimestamp(next_limit_timestamp)) {
        need_quit_ = true;
        return;
      }

      timeoracle->UpdateLimitTimestamp(next_limit_timestamp);
    }

    ThisThread::Sleep(1000);
  }
}

}  // namespace timeoracle
}  // namespace tera
