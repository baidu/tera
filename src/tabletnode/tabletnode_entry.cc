// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#include <iomanip>
#include "tabletnode/tabletnode_entry.h"

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/metric/collector_report.h"
#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include "common/thread_attributes.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "proto/master_client.h"
#include "proto/tabletnode.pb.h"
#include "tabletnode/remote_tabletnode.h"
#include "tabletnode/tabletnode_impl.h"
#include "common/counter.h"
#include "utils/rpc_timer_list.h"
#include "common/timer.h"
#include "utils/utils_cmd.h"

DEFINE_bool(tera_metric_http_server_enable, true, "enable metric http server, enable as default");
DEFINE_int32(tera_metric_http_server_listen_port, 20221, "listen port for metric http server");

DECLARE_string(tera_tabletnode_port);
DECLARE_int32(tera_garbage_collect_period);
DECLARE_bool(tera_tabletnode_cpu_affinity_enabled);
DECLARE_string(tera_tabletnode_cpu_affinity_set);
DECLARE_bool(tera_tabletnode_hang_detect_enabled);
DECLARE_int32(tera_tabletnode_hang_detect_threshold);
DECLARE_int32(tera_tabletnode_rpc_server_max_inflow);
DECLARE_int32(tera_tabletnode_rpc_server_max_outflow);
DECLARE_bool(tera_metric_http_server_enable);
DECLARE_int32(tera_metric_http_server_listen_port);

DECLARE_bool(tera_tabletnode_dump_level_size_info_enabled);

std::string GetTeraEntryName() { return "tabletnode"; }

tera::TeraEntry* GetTeraEntry() { return new tera::tabletnode::TabletNodeEntry(); }

namespace tera {
namespace tabletnode {

TabletNodeEntry::TabletNodeEntry()
    : tabletnode_impl_(NULL),
      remote_tabletnode_(NULL),
      metric_http_server_(new tera::MetricHttpServer()) {
  sofa::pbrpc::RpcServerOptions rpc_options;
  rpc_options.max_throughput_in = FLAGS_tera_tabletnode_rpc_server_max_inflow;
  rpc_options.max_throughput_out = FLAGS_tera_tabletnode_rpc_server_max_outflow;
  rpc_options.keep_alive_time = 7200;
  rpc_server_.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

TabletNodeEntry::~TabletNodeEntry() {}

bool TabletNodeEntry::StartServer() {
  // set which core could work on this TS
  SetProcessorAffinity();
  //
  // start metric http server
  if (FLAGS_tera_metric_http_server_enable) {
    if (!metric_http_server_->Start(FLAGS_tera_metric_http_server_listen_port)) {
      LOG(ERROR) << "Start metric http server failed.";
      return false;
    }
  } else {
    LOG(INFO) << "Metric http server is disabled.";
  }

  IpAddress tabletnode_addr("0.0.0.0", FLAGS_tera_tabletnode_port);
  LOG(INFO) << "Start RPC server at: " << tabletnode_addr.ToString();

  tabletnode_impl_.reset(new TabletNodeImpl());
  remote_tabletnode_ = new RemoteTabletNode(tabletnode_impl_.get());

  // 注册给rpcserver, rpcserver会负责delete
  rpc_server_->RegisterService(remote_tabletnode_);
  if (!rpc_server_->Start(tabletnode_addr.ToString())) {
    LOG(ERROR) << "start RPC server error";
    return false;
  }

  if (!tabletnode_impl_->Init()) {  // register on ZK
    LOG(ERROR) << "fail to init tabletnode_impl";
    return false;
  }
  LOG(INFO) << "finish starting RPC server";
  return true;
}

void TabletNodeEntry::ShutdownServer() {
  tabletnode_impl_->Exit();
  LOG(INFO) << "TabletNodeEntry stop done!";
  _exit(0);
}

bool TabletNodeEntry::Run() {
  static int64_t timer_ticks = 0;
  ++timer_ticks;

  // Run garbage collect, in secondes.
  const int garbage_collect_period =
      (FLAGS_tera_garbage_collect_period) ? FLAGS_tera_garbage_collect_period : 60;
  if (timer_ticks % garbage_collect_period == 0) {
    tabletnode_impl_->GarbageCollect();
  }

  if (FLAGS_tera_tabletnode_dump_level_size_info_enabled) {
    tabletnode_impl_->RefreshLevelSize();
  }

  CollectorReportPublisher::GetInstance().Refresh();
  tabletnode_impl_->RefreshAndDumpSysInfo();

  LOG(INFO) << "[ThreadPool schd/task/cnt] " << remote_tabletnode_->ProfilingLog();

  int64_t now_time = get_micros();
  int64_t earliest_rpc_time = now_time;
  RpcTimerList::Instance()->TopTime(&earliest_rpc_time);
  double max_delay = (now_time - earliest_rpc_time) / 1000.0;
  LOG(INFO) << "pending rpc max delay: " << std::fixed << std::setprecision(2) << max_delay;
  if (FLAGS_tera_tabletnode_hang_detect_enabled &&
      max_delay > FLAGS_tera_tabletnode_hang_detect_threshold) {
    LOG(FATAL) << "hang detected: " << std::fixed << std::setprecision(2) << max_delay;
  }

  ThisThread::Sleep(1000);
  return true;
}

void TabletNodeEntry::SetProcessorAffinity() {
  if (!FLAGS_tera_tabletnode_cpu_affinity_enabled) {
    return;
  }

  ThreadAttributes thread_attr;
  thread_attr.MarkCurMask();
  thread_attr.ResetCpuMask();
  std::vector<std::string> cpu_set;

  SplitString(FLAGS_tera_tabletnode_cpu_affinity_set, ",", &cpu_set);
  for (uint32_t i = 0; i < cpu_set.size(); ++i) {
    int32_t cpu_id;
    if (StringToNumber(cpu_set[i], &cpu_id)) {
      thread_attr.SetCpuMask(cpu_id);
    } else {
      LOG(ERROR) << "invalid cpu affinity id: " << cpu_set[i];
    }
  }

  if (!thread_attr.SetCpuAffinity()) {
    LOG(ERROR) << "fail to set affinity, revert back";
    if (!thread_attr.RevertCpuAffinity()) {
      LOG(ERROR) << "fail to revert previous affinity";
    }
    return;
  }

  LOG(INFO) << "Set processor affinity to CPU: " << FLAGS_tera_tabletnode_cpu_affinity_set;
}
}  // namespace tabletnode
}  // namespace tera
