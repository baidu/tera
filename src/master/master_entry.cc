// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/master_entry.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "common/metric/collector_report.h"
#include "common/net/ip_address.h"
#include "master/master_impl.h"
#include "master/multi_tenancy_service_impl.h"
#include "master/remote_master.h"
#include "master/remote_multi_tenancy_service.h"
#include "utils/utils_cmd.h"

DEFINE_bool(tera_metric_http_server_enable, true, "enable metric http server, enable as default");
DEFINE_int32(tera_metric_http_server_listen_port, 20221, "listen port for metric http server");

DECLARE_string(tera_master_port);
DECLARE_int32(tera_master_rpc_server_max_inflow);
DECLARE_int32(tera_master_rpc_server_max_outflow);
DECLARE_bool(tera_metric_http_server_enable);
DECLARE_int32(tera_metric_http_server_listen_port);
DECLARE_int32(tera_master_thread_max_num);
DECLARE_string(tera_auth_policy);

DECLARE_string(tera_cluster_name);

std::string GetTeraEntryName() { return "master"; }

tera::TeraEntry* GetTeraEntry() { return new tera::master::MasterEntry(); }

namespace tera {
namespace master {

MasterEntry::MasterEntry()
    : master_impl_(nullptr),
      multi_tenancy_service_impl_(nullptr),
      thread_pool_(nullptr),
      remote_master_(nullptr),
      remote_multi_tenancy_service_(nullptr),
      rpc_server_(nullptr),
      metric_http_server_(new tera::MetricHttpServer()) {
  sofa::pbrpc::RpcServerOptions rpc_options;
  rpc_options.max_throughput_in = FLAGS_tera_master_rpc_server_max_inflow;
  rpc_options.max_throughput_out = FLAGS_tera_master_rpc_server_max_outflow;
  rpc_options.keep_alive_time = 7200;
  rpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

MasterEntry::~MasterEntry() {}

bool MasterEntry::StartServer() {
  // start metric http server
  if (FLAGS_tera_metric_http_server_enable) {
    if (!metric_http_server_->Start(FLAGS_tera_metric_http_server_listen_port)) {
      LOG(ERROR) << "Start metric http server failed.";
      return false;
    }
  } else {
    LOG(INFO) << "Metric http server is disabled.";
  }

  IpAddress master_addr("0.0.0.0", FLAGS_tera_master_port);
  LOG(INFO) << "Start master RPC server at: " << master_addr.ToString();

  std::shared_ptr<auth::AccessEntry> access_entry =
      std::make_shared<auth::AccessEntry>(FLAGS_tera_auth_policy);
  std::shared_ptr<quota::MasterQuotaEntry> quota_entry =
      std::make_shared<quota::MasterQuotaEntry>();
  master_impl_.reset(new MasterImpl(access_entry, quota_entry));
  multi_tenancy_service_impl_.reset(new MultiTenacyServiceImpl(access_entry, quota_entry));
  thread_pool_.reset(new ThreadPool(FLAGS_tera_master_thread_max_num));
  remote_master_ = new RemoteMaster(master_impl_.get(), thread_pool_);
  remote_multi_tenancy_service_ =
      new RemoteMultiTenancyService(multi_tenancy_service_impl_.get(), thread_pool_);

  if (!master_impl_->Init()) {
    return false;
  }

  rpc_server_->RegisterService(remote_master_);
  rpc_server_->RegisterService(remote_multi_tenancy_service_);
  if (!rpc_server_->Start(master_addr.ToString())) {
    LOG(ERROR) << "start RPC server error";
    return false;
  }

  LOG(INFO) << "finish starting master server for cluster: " << FLAGS_tera_cluster_name;

  return true;
}

bool MasterEntry::Run() {
  CollectorReportPublisher::GetInstance().Refresh();
  static int64_t timer_ticks = 0;
  ++timer_ticks;

  if (timer_ticks % 10 == 0) {
    LOG(INFO) << "[ThreadPool schd/task/cnt] " << master_impl_->ProfilingLog();
  }

  ThisThread::Sleep(1000);
  return true;
}

void MasterEntry::ShutdownServer() {
  metric_http_server_->Stop();
  rpc_server_->Stop();
  master_impl_.reset();
  thread_pool_.reset();
}

}  // namespace master
}  // namespace tera
