// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_
#define  TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_

#include <string>
#include <vector>

#include "tabletnode/tabletnode_impl.h"
#include "zk/zk_adapter.h"

namespace tera {
namespace tabletnode {

class TabletNodeZkAdapterBase : public zk::ZooKeeperAdapter {
public:
    virtual ~TabletNodeZkAdapterBase() {};
    virtual void Init() = 0;
};

class TabletNodeZkAdapter : public TabletNodeZkAdapterBase {
public:
    TabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                        const std::string & server_port);
    virtual ~TabletNodeZkAdapter();
    virtual void Init();

private:
    bool Register(std::string* session_id, int* zk_code);
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
                            const std::string& server_port) {}
    virtual ~FakeTabletNodeZkAdapter() {}
    virtual void Init() {}

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
    TabletNodeImpl * m_tabletnode_impl;
    std::string m_server_addr;
    std::string m_serve_node_path;
    std::string m_kick_node_path;
};
} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_
