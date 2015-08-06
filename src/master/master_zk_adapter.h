// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_MASTER_MASTER_ZK_ADAPTER_H_
#define  TERA_MASTER_MASTER_ZK_ADAPTER_H_

#include <string>
#include <vector>

#include "master/master_impl.h"
#include "zk/zk_adapter.h"

namespace galaxy{
namespace ins{
namespace sdk {
    class InsSDK;
}
}
}

namespace tera {
namespace master {

class MasterZkAdapterBase : public zk::ZooKeeperAdapter {
public:
    virtual ~MasterZkAdapterBase() {};
    virtual bool Init(std::string* root_tablet_addr,
                      std::map<std::string, std::string>* tabletnode_list,
                      bool* safe_mode) = 0;

    virtual bool KickTabletServer(const std::string& ts_host,
                                  const std::string& ts_zk_id) = 0;
    virtual bool MarkSafeMode() = 0;
    virtual bool UnmarkSafeMode() = 0;
    virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr) = 0;
};

class MasterZkAdapter : public MasterZkAdapterBase {
public:
    MasterZkAdapter(MasterImpl* master_impl,
                    const std::string & server_addr);
    virtual ~MasterZkAdapter();
    virtual bool Init(std::string* root_tablet_addr,
              std::map<std::string, std::string>* tabletnode_list,
              bool* safe_mode);

    virtual bool KickTabletServer(const std::string& ts_host,
                          const std::string& ts_zk_id);
    virtual bool MarkSafeMode();
    virtual bool UnmarkSafeMode();
    virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr);

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
    MasterImpl * m_master_impl;
    std::string m_server_addr;
};

/*
 * This is not zookeeper!
 * Just used on onebox for tasting tera briefly.
 * This is implemented through local file system.
 * Not support watching.
 */
class FakeMasterZkAdapter: public MasterZkAdapterBase {
public:
    FakeMasterZkAdapter(MasterImpl * master_impl,
                        const std::string & server_addr);
    virtual ~FakeMasterZkAdapter();
    virtual bool Init(std::string* root_tablet_addr,
                      std::map<std::string, std::string>* tabletnode_list,
                      bool* safe_mode);

    virtual bool KickTabletServer(const std::string& ts_host,
                                  const std::string& ts_zk_id);
    virtual bool MarkSafeMode();
    virtual bool UnmarkSafeMode();
    virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr);

private:
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
    MasterImpl * m_master_impl;
    std::string m_server_addr;
    std::string m_fake_path;
};


class InsMasterZkAdapter: public MasterZkAdapterBase {
public:
    InsMasterZkAdapter(MasterImpl * master_impl,
                        const std::string & server_addr);
    virtual ~InsMasterZkAdapter();
    virtual bool Init(std::string* root_tablet_addr,
                      std::map<std::string, std::string>* tabletnode_list,
                      bool* safe_mode);

    virtual bool KickTabletServer(const std::string& ts_host,
                                  const std::string& ts_zk_id);
    virtual bool MarkSafeMode() {return true;}
    virtual bool UnmarkSafeMode() {return true;}
    virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr);
    void RefreshTabletNodeList();
    void OnLockChange(std::string session_id, bool deleted);
    void OnSessionTimeout();
private:
    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list) {}
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) {}
    virtual void OnNodeCreated(const std::string& path) {}
    virtual void OnNodeDeleted(const std::string& path) {}
    virtual void OnWatchFailed(const std::string& path, int watch_type,
                               int err) {}
private:
    mutable Mutex m_mutex;
    MasterImpl * m_master_impl;
    std::string m_server_addr;
    galaxy::ins::sdk::InsSDK* m_ins_sdk;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_MASTER_ZK_ADAPTER_H_
