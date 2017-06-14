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
    virtual bool Register(const std::string& session_id, int* zk_code);
    virtual bool Unregister(int* zk_code);

    virtual bool WatchMaster(std::string* master, int* zk_code);
    virtual bool WatchSafeModeMark(bool* is_exist, int* zk_code);
    virtual bool WatchKickMark(bool* is_exist, int* zk_code);
    virtual bool WatchSelfNode(bool* is_exist, int* zk_code);
    virtual bool WatchRootNode(bool* is_exist, std::string* root_tablet_addr, int* zk_errno);

    virtual void OnSafeModeMarkCreated();
    virtual void OnSafeModeMarkDeleted();
    virtual void OnKickMarkCreated();
    virtual void OnSelfNodeDeleted();
    virtual void OnRootNodeCreated();
    virtual void OnRootNodeDeleted();
    virtual void OnRootNodeChanged(const std::string& root_tablet_addr);

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
    TabletNodeImpl * tabletnode_impl_;
    std::string server_addr_;
    std::string serve_node_path_;
    std::string kick_node_path_;
};

class MockTabletNodeZkAdapter : public TabletNodeZkAdapter {
public:
    MockTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                        const std::string & server_addr) :
        TabletNodeZkAdapter(tabletnode_impl, server_addr) {}
    virtual ~MockTabletNodeZkAdapter() {}
private:
    virtual void OnKickMarkCreated() {}
    virtual void OnSelfNodeDeleted() {}
    virtual void OnWatchFailed(const std::string& /*path*/, int /*watch_type*/, int /*err*/) {}
    virtual void OnSessionTimeout() {}
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
    mutable Mutex mutex_;
    TabletNodeImpl * tabletnode_impl_;
    std::string server_addr_;
    std::string serve_node_path_;
    std::string kick_node_path_;
    std::string fake_path_;
};


class InsTabletNodeZkAdapter : public TabletNodeZkAdapterBase {
public:
    InsTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                            const std::string& server_addr);
    virtual ~InsTabletNodeZkAdapter() {}
    virtual void Init();
    virtual bool GetRootTableAddr(std::string* root_table_addr);
    virtual void OnKickMarkCreated();
    virtual void OnLockChange(std::string session_id, bool deleted);
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
    mutable Mutex mutex_;
    TabletNodeImpl * tabletnode_impl_;
    std::string server_addr_;
    std::string serve_node_path_;
    std::string kick_node_path_;
    galaxy::ins::sdk::InsSDK* ins_sdk_;
};

class MockInsTabletNodeZkAdapter : public InsTabletNodeZkAdapter {
public:
    MockInsTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                               const std::string& server_addr) :
        InsTabletNodeZkAdapter(tabletnode_impl, server_addr) {}
    virtual ~MockInsTabletNodeZkAdapter() {}
    virtual void OnKickMarkCreated() {}
    virtual void OnLockChange(std::string /*session_id*/, bool /*deleted*/) {}
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_ZK_ADAPTER_H_
