// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin(xupeilin@baidu.com)

#ifndef TERA_TABLETNODE_TABLETNODE_SYSINFO_H_
#define TERA_TABLETNODE_TABLETNODE_SYSINFO_H_

#include <map>
#include <string>

#include "common/mutex.h"
#include "proto/tabletnode.pb.h"
#include "tabletnode/tablet_manager.h"

namespace tera {
namespace tabletnode {

class TabletNodeSysInfo {
public:
    TabletNodeSysInfo();
    TabletNodeSysInfo(const TabletNodeInfo& info);

    ~TabletNodeSysInfo();

    void CollectTabletNodeInfo(TabletManager* tablet_manager,
                               const std::string& server_addr);

    void CollectHardwareInfo();

    void AddExtraInfo(const std::string& name, int64_t value);

    void Reset();

    void SetCurrentTime();

    int64_t GetTimeStamp();

    void SetTimeStamp(int64_t ts);

    void SetStatus(TabletNodeStatus status);

    void GetTabletNodeInfo(TabletNodeInfo* info);

    void GetTabletMetaList(TabletMetaList* meta_list);

    void DumpLog();

private:
    TabletNodeInfo m_info;
    TabletMetaList m_tablet_list;
    int64_t m_mem_check_ts;
    int64_t m_net_check_ts;
    int64_t m_io_check_ts;
    int64_t m_net_tx_total;
    int64_t m_net_rx_total;
    int64_t m_cpu_check_ts;

    int64_t m_tablet_check_ts;
    mutable Mutex m_mutex;
};
} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_SYSINFO_H_
