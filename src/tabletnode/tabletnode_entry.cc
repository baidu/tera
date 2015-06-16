// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#include <iomanip>
#include "tabletnode/tabletnode_entry.h"

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include "common/thread_attributes.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "proto/master_client.h"
#include "proto/tabletnode.pb.h"
#include "tabletnode/remote_tabletnode.h"
#include "tabletnode/tabletnode_impl.h"
#include "utils/counter.h"
#include "utils/rpc_timer_list.h"
#include "utils/timer.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_tabletnode_port);
DECLARE_int32(tera_garbage_collect_period);
DECLARE_bool(tera_zk_enabled);
DECLARE_bool(tera_tabletnode_cpu_affinity_enabled);
DECLARE_string(tera_tabletnode_cpu_affinity_set);
DECLARE_bool(tera_tabletnode_hang_detect_enabled);
DECLARE_int32(tera_tabletnode_hang_detect_threshold);
DECLARE_int32(tera_tabletnode_rpc_server_max_inflow);
DECLARE_int32(tera_tabletnode_rpc_server_max_outflow);

namespace tera {
namespace tabletnode {

TabletNodeEntry::TabletNodeEntry()
    : m_tabletnode_impl(NULL),
      m_remote_tabletnode(NULL) {
    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_options.max_throughput_in = FLAGS_tera_tabletnode_rpc_server_max_inflow;
    rpc_options.max_throughput_out = FLAGS_tera_tabletnode_rpc_server_max_outflow;
    m_rpc_server.reset(new sofa::pbrpc::RpcServer(rpc_options));
}

TabletNodeEntry::~TabletNodeEntry() {}

bool TabletNodeEntry::StartServer() {
    // set which core could work on this TS
    SetProcessorAffinity();

    IpAddress tabletnode_addr(utils::GetLocalHostAddr(), FLAGS_tera_tabletnode_port);
    LOG(INFO) << "Start RPC server at: " << tabletnode_addr.ToString();

    TabletNodeInfo tabletnode_info;
    tabletnode_info.set_addr(tabletnode_addr.ToString());

    m_tabletnode_impl.reset(new TabletNodeImpl(tabletnode_info));
    m_remote_tabletnode = new RemoteTabletNode(m_tabletnode_impl.get());

    // 注册给rpcserver, rpcserver会负责delete
    m_rpc_server->RegisterService(m_remote_tabletnode);
    if (!m_rpc_server->Start(tabletnode_addr.ToString())) {
        LOG(ERROR) << "start RPC server error";
        return false;
    }

    if (!m_tabletnode_impl->Init()) { // register on ZK
        LOG(ERROR) << "fail to init tabletnode_impl";
        return false;
    }

    LOG(INFO) << "finish starting RPC server";
    return true;
}

void TabletNodeEntry::ShutdownServer() {
    LOG(INFO) << "shut down server";
    // StopServer要保证调用后, 不会再调用serveice的任何方法.
    m_rpc_server->Stop();
    m_tabletnode_impl->Exit();
    m_tabletnode_impl.reset();
    LOG(INFO) << "TabletNodeEntry stop done!";
}

bool TabletNodeEntry::Run() {
    static int64_t timer_ticks = 0;
    ++timer_ticks;

    // Run garbage collect, in secondes.
    const int garbage_collect_period = (FLAGS_tera_garbage_collect_period)?
                                        FLAGS_tera_garbage_collect_period : 60;
    if (timer_ticks % garbage_collect_period == 0) {
        m_tabletnode_impl->GarbageCollect();
    }

    m_tabletnode_impl->RefreshSysInfo();
    m_tabletnode_impl->GetSysInfo().DumpLog();

    int64_t now_time = get_micros();
    int64_t earliest_rpc_time = now_time;
    RpcTimerList::Instance()->TopTime(&earliest_rpc_time);
    double max_delay = (now_time - earliest_rpc_time) / 1000.0;
    VLOG(5) << "pending rpc max delay: "
            << std::fixed<< std::setprecision(2) << max_delay;
    if (FLAGS_tera_tabletnode_hang_detect_enabled &&
        max_delay > FLAGS_tera_tabletnode_hang_detect_threshold) {
        LOG(FATAL) << "hang detected: "
                   << std::fixed<< std::setprecision(2) << max_delay;
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
        StringToNumber(cpu_set[i], &cpu_id);
        thread_attr.SetCpuMask(cpu_id);
    }

    if (!thread_attr.SetCpuAffinity()) {
        LOG(ERROR) << "fail to set affinity, revert back";
        if (!thread_attr.RevertCpuAffinity()) {
            LOG(ERROR) << "fail to revert previous affinity";
        }
        return;
    }

    LOG(INFO) << "Set processor affinity to CPU: "
        << FLAGS_tera_tabletnode_cpu_affinity_set;
}
}// namespace tabletnode
} // namespace tera
