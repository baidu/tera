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

    void SetTimeStamp(int64_t ts);

    void SetServerAddr(const std::string& addr);

    void SetStatus(StatusCode status);

    void GetTabletNodeInfo(TabletNodeInfo* info);

    void GetTabletMetaList(TabletMetaList* meta_list);

    void DumpLog();

    void SetProcessStartTime(int64_t ts);

private:
    TabletNodeInfo info_;
    TabletMetaList tablet_list_;

    mutable Mutex mutex_;
    int64_t last_check_ts_;
};
} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_SYSINFO_H_
