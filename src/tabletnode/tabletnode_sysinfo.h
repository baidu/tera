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
    TabletNodeInfo info_;
    TabletMetaList tablet_list_;
    int64_t mem_check_ts_;
    int64_t net_check_ts_;
    int64_t io_check_ts_;
    int64_t net_tx_total_;
    int64_t net_rx_total_;
    int64_t cpu_check_ts_;

    int64_t tablet_check_ts_;
    mutable Mutex mutex_;
};
} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_SYSINFO_H_
