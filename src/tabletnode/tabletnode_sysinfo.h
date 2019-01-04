// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin(xupeilin@baidu.com)

#ifndef TERA_TABLETNODE_TABLETNODE_SYSINFO_H_
#define TERA_TABLETNODE_TABLETNODE_SYSINFO_H_

#include <map>
#include <memory>
#include <string>

#include "common/mutex.h"
#include "proto/tabletnode.pb.h"
#include "tabletnode/tablet_manager.h"

namespace tera {
namespace tabletnode {

class TabletNodeSysInfoDumper;

class TabletNodeSysInfo {
 public:
  TabletNodeSysInfo();

  ~TabletNodeSysInfo();

  void CollectTabletNodeInfo(TabletManager* tablet_manager, const std::string& server_addr);

  void CollectHardwareInfo();

  void AddExtraInfo(const std::string& name, int64_t value);

  void SetTimeStamp(int64_t ts);

  void SetServerAddr(const std::string& addr);

  void SetPersistentCacheSize(uint64_t size);

  void SetStatus(StatusCode status);

  void GetTabletNodeInfo(TabletNodeInfo* info);

  void GetTabletMetaList(TabletMetaList* meta_list);

  void DumpLog();

  void SetProcessStartTime(int64_t ts);

  void RefreshTabletsStatus(TabletManager* tablet_manager);

  void UpdateWriteFlowController();

 private:
  void SwitchInfo() {
    auto new_info = new TabletNodeInfo{*info_};
    info_.reset(new_info);
  }

  using DumpInfoFunction = void(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                const std::shared_ptr<CollectorReport>& latest_report,
                                const TabletNodeSysInfoDumper& dumper);
  DumpInfoFunction DumpSysInfo;
  DumpInfoFunction DumpHardWareInfo;
  DumpInfoFunction DumpIoInfo;
  DumpInfoFunction DumpCacheInfo;
  DumpInfoFunction DumpRequestInfo;
  DumpInfoFunction DumpDfsInfo;
  DumpInfoFunction DumpPosixInfo;
  DumpInfoFunction DumpLevelSizeInfo;
  DumpInfoFunction DumpPersistentCacheInfo;
  DumpInfoFunction DumpOtherInfo;

  void RegisterDumpInfoFunction(DumpInfoFunction TabletNodeSysInfo::*f) {
    dump_info_functions_.emplace_back(
        std::bind(f, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  }

  std::vector<std::function<DumpInfoFunction>> dump_info_functions_;

 private:
  std::shared_ptr<TabletNodeInfo> info_;
  std::unique_ptr<TabletMetaList> tablet_list_;

  mutable Mutex mutex_;
};
}  // namespace tabletnode
}  // namespace tera

#endif  // TERA_TABLETNODE_TABLETNODE_SYSINFO_H_
