// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_MASTER_MASTER_ZK_ADAPTER_H
#define  TERA_MASTER_MASTER_ZK_ADAPTER_H

#include <string>
#include <vector>

#include "master/master_impl.h"
#include "zk/zk_adapter.h"

namespace tera {
namespace master {

class MasterZkAdapter: public zk::ZooKeeperAdapter {
public:
    MasterZkAdapter(MasterImpl * master_impl,
                    const std::string & server_addr);
    virtual ~MasterZkAdapter();
    bool Init(std::string* root_tablet_addr,
              std::map<std::string, std::string>* tabletnode_list,
              bool* safe_mode);

    bool KickTabletServer(const std::string& ts_host,
                          const std::string& ts_zk_id);
    bool MarkSafeMode();
    bool UnmarkSafeMode();
    bool UpdateRootTabletNode(const std::string& root_tablet_addr);

protected:
    bool Setup();
    void Reset();

    bool LockMasterLock();
    bool UnlockMasterLock();
    bool CreateMasterNode();
    bool DeleteMasterNode();

    bool WatchRootTabletNode(bool* is_exist, std::string* root_tablet_addr);
    bool WatchSafeModeMark(bool* is_safemode);
    bool WatchTabletNodeList(std::map<std::string, std::string>* tabletnode_list);

    void OnSafeModeMarkCreated();
    void OnSafeModeMarkDeleted();
    void OnMasterLockLost();
    void OnTabletNodeListDeleted();
    void OnRootTabletNodeDeleted();
    void OnMasterNodeDeleted();
    void OnTabletServerKickMarkCreated();
    void OnTabletServerKickMarkDeleted();
    void OnTabletServerStart(const std::string& ts_host);
    void OnTabletServerExist(const std::string& ts_host);

    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list);
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value);
    virtual void OnNodeCreated(const std::string& path);
    virtual void OnNodeDeleted(const std::string& path);
    virtual void OnWatchFailed(const std::string& path, int watch_type,
                               int err);
    virtual void OnSessionTimeout();

private:
    mutable Mutex m_mutex;
    TimerManager* m_timer_manager;
    MasterImpl * m_master_impl;
    std::string m_server_addr;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_MASTER_ZK_ADAPTER_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
