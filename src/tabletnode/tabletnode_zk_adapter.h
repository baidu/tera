// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_
#define  TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_

#include <string>
#include <vector>

#include "tabletnode/tabletnode_impl.h"
#include "zk/zk_adapter.h"

namespace galaxy{
namespace ins{
namespace sdk{
    class InsSDK;
}
}
}

namespace tera {
namespace tabletnode {

class TabletNodeZkAdapterBase : public zk::ZooKeeperAdapter {
public:
    virtual ~TabletNodeZkAdapterBase() {};
    virtual void Init() = 0;
    virtual bool GetRootTableAddr(std::string* root_table_addr) = 0;
};

class TabletNodeZkAdapter : public TabletNodeZkAdapterBase {
public:
    TabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                        const std::string & server_addr);
    virtual ~TabletNodeZkAdapter();
    virtual void Init();
    virtual bool GetRootTableAddr(std::string* root_table_addr);

private:
    bool Register(const std::string& session_id, int* zk_code);
    bool Unregister(int* zk_code);

    bool WatchMaster(std::string* master, int* zk_code);
    bool WatchSafeModeMark(bool* is_exist, int* zk_code);
    bool WatchKickMark(bool* is_exist, int* zk_code);
    bool WatchSelfNode(bool* is_exist, int* zk_code);
    bool WatchRootNode(bool* is_exist, std::string* root_tablet_addr, int* zk_errno);

    void OnSafeModeMarkCreated();
    void OnSafeModeMarkDeleted();
    void OnKickMarkCreated();
    void OnSelfNodeDeleted();
    void OnRootNodeCreated();
    void OnRootNodeDeleted();
    void OnRootNodeChanged(const std::string& root_tablet_addr);

    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list);
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value);
    virtual void OnNodeCreated(const std::string& path);
    virtual void OnNodeDeleted(const std::string& path);
    virtual void OnWatchFailed(const std::string& path, int watch_type, int err);
    virtual void OnSessionTimeout();

private:
    TabletNodeImpl * m_tabletnode_impl;
    std::string m_server_addr;
    std::string m_serve_node_path;
    std::string m_kick_node_path;
};

class FakeTabletNodeZkAdapter : public TabletNodeZkAdapterBase {
public:
    FakeTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                            const std::string& server_addr);
    virtual ~FakeTabletNodeZkAdapter() {}
    virtual void Init();
    virtual bool GetRootTableAddr(std::string* root_table_addr);

private:
    bool Register(const std::string& session_id, int* zk_code = NULL);
    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list) {}
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) {}
    virtual void OnNodeCreated(const std::string& path) {}
    virtual void OnNodeDeleted(const std::string& path) {}
    virtual void OnWatchFailed(const std::string& path, int watch_type, int err) {}
    virtual void OnSessionTimeout() {}

private:
    mutable Mutex m_mutex;
    TabletNodeImpl * m_tabletnode_impl;
    std::string m_server_addr;
    std::string m_serve_node_path;
    std::string m_kick_node_path;
    std::string m_fake_path;
};


class InsTabletNodeZkAdapter : public TabletNodeZkAdapterBase {
public:
    InsTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                            const std::string& server_addr);
    virtual ~InsTabletNodeZkAdapter() {}
    virtual void Init();
    virtual bool GetRootTableAddr(std::string* root_table_addr);
    void OnKickMarkCreated();
    void OnLockChange(std::string session_id, bool deleted);
    void OnMetaChange(std::string meta_addr, bool deleted);
private:
    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list) {}
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) {}
    virtual void OnNodeCreated(const std::string& path) {}
    virtual void OnNodeDeleted(const std::string& path) {}
    virtual void OnWatchFailed(const std::string& path, int watch_type, int err) {}
    virtual void OnSessionTimeout() {}

private:
    mutable Mutex m_mutex;
    TabletNodeImpl * m_tabletnode_impl;
    std::string m_server_addr;
    std::string m_serve_node_path;
    std::string m_kick_node_path;
    galaxy::ins::sdk::InsSDK* m_ins_sdk;
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_
