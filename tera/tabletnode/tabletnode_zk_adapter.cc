// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/tabletnode/tabletnode_zk_adapter.h"
#include "tera/types.h"
#include "tera/zk/zk_util.h"

DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_int32(tera_zk_timeout);
DECLARE_int64(tera_zk_retry_period);

namespace tera {
namespace tabletnode {

TabletNodeZkAdapter::TabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                                         const std::string& server_addr)
    : m_tabletnode_impl(tabletnode_impl), m_server_addr(server_addr) {
}

TabletNodeZkAdapter::~TabletNodeZkAdapter() {
}

void TabletNodeZkAdapter::Init() {
    int zk_errno;

    // init zk client
    while (!ZooKeeperAdapter::Init(FLAGS_tera_zk_addr_list,
                                FLAGS_tera_zk_root_path, FLAGS_tera_zk_timeout,
                                m_server_addr, &zk_errno)) {
        LOG(ERROR) << "fail to init zk : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "init zk success";

    // create my node
    std::string session_id;
    while (!Register(&session_id, &zk_errno)) {
        LOG(ERROR) << "fail to create serve-node : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "create serve-node success";

    bool is_exist = false;

    // watch my node
    while (!WatchSelfNode(&is_exist, &zk_errno)) {
        LOG(ERROR) << "fail to watch serve-node : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch serve-node success";
    if (!is_exist) {
        OnSelfNodeDeleted();
    }

    // watch kick node
    while (!WatchKickMark(&is_exist, &zk_errno)) {
        LOG(ERROR) << "fail to watch kick mark : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch kick mark success";
    if (is_exist) {
        OnKickMarkCreated();
    }

    // watch safemode node
    while (!WatchSafeModeMark(&is_exist, &zk_errno)) {
        LOG(ERROR) << "fail to watch safemode mark : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch safemode mark success";
    if (is_exist) {
        OnSafeModeMarkCreated();
    }

    // watch root node
    std::string root_tablet_addr;
    while (!WatchRootNode(&is_exist, &root_tablet_addr, &zk_errno)) {
        LOG(ERROR) << "fail to watch root node : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch root node success";
    if (!root_tablet_addr.empty()) {
        m_tabletnode_impl->SetRootTabletAddr(root_tablet_addr);
    }

    // enter running state
    m_tabletnode_impl->SetSessionId(session_id);
    m_tabletnode_impl->SetTabletNodeStatus(TabletNodeImpl::kIsRunning);
}

bool TabletNodeZkAdapter::Register(std::string* session_id, int* zk_errno) {
    // get session id
    int64_t session_id_int = 0;
    if (!GetSessionId(&session_id_int, zk_errno)) {
        LOG(ERROR) << "get session id fail : " << zk::ZkErrnoToString(*zk_errno);
        return false;
    }
    char session_id_str[32];
    sprintf(session_id_str, "%016llx", session_id_int);

    // create serve node
    std::string node_path = kTsListPath + "/" + session_id_str + "#";
    std::string node_value = m_server_addr;
    std::string ret_node_path;
    if (!CreateSequentialEphemeralNode(node_path, node_value, &ret_node_path,
                                       zk_errno)) {
        LOG(ERROR) << "create serve node fail";
        return false;
    }
    LOG(INFO) << "create serve node success";
    m_serve_node_path = ret_node_path;
    *session_id = zk::ZooKeeperUtil::GetNodeName(m_serve_node_path.c_str());
    m_kick_node_path = kKickPath + "/" + *session_id;
    SetZkAdapterCode(zk::ZE_OK, zk_errno);
    return true;
}

bool TabletNodeZkAdapter::Unregister(int* zk_errno) {
    if (!DeleteNode(m_serve_node_path, zk_errno)) {
        LOG(ERROR) << "delete serve node fail";
        return false;
    }
    LOG(INFO) << "delete serve node success";
    SetZkAdapterCode(zk::ZE_OK, zk_errno);
    return true;
}

bool TabletNodeZkAdapter::WatchMaster(std::string* master, int* zk_errno) {
    return ReadAndWatchNode(kMasterNodePath, master, zk_errno);
}

bool TabletNodeZkAdapter::WatchSafeModeMark(bool* is_exist, int* zk_errno) {
    return CheckAndWatchExist(kSafeModeNodePath, is_exist, zk_errno);
}

bool TabletNodeZkAdapter::WatchKickMark(bool* is_exist, int* zk_errno) {
    return CheckAndWatchExist(m_kick_node_path, is_exist, zk_errno);
}

bool TabletNodeZkAdapter::WatchSelfNode(bool* is_exist, int* zk_errno) {
    return CheckAndWatchExist(m_serve_node_path, is_exist, zk_errno);
}

bool TabletNodeZkAdapter::WatchRootNode(bool* is_exist, std::string* root_tablet_addr, int* zk_errno) {
    if (!CheckAndWatchExist(kRootTabletNodePath, is_exist, zk_errno)) {
        return false;
    }
    if (!*is_exist) {
        return true;
    }
    return ReadAndWatchNode(kRootTabletNodePath, root_tablet_addr, zk_errno);
}
/*
void TabletNodeZkAdapter::OnMasterNodeCreated(const std::string& master) {
    LOG(INFO) << "master node is created";
    m_tabletnode_impl->SetMaster(master);
}

void TabletNodeZkAdapter::OnMasterNodeDeleted() {
    LOG(INFO) << "master node is deleted";
    m_tabletnode_impl->SetMaster();
}

void TabletNodeZkAdapter::OnMasterNodeChanged(const std::string& master) {
    LOG(INFO) << "master node is changed";
    m_tabletnode_impl->SetMaster(master);
}
*/

void TabletNodeZkAdapter::OnRootNodeCreated() {
    LOG(INFO) << "root node is created";
    // watch root node
    int zk_errno = zk::ZE_OK;
    bool is_exist = false;
    std::string root_tablet_addr;
    while (!WatchRootNode(&is_exist, &root_tablet_addr, &zk_errno)) {
        LOG(ERROR) << "fail to root node : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch root node success";
    if (!root_tablet_addr.empty()) {
        m_tabletnode_impl->SetRootTabletAddr(root_tablet_addr);
    }
}

void TabletNodeZkAdapter::OnRootNodeDeleted() {
    LOG(INFO) << "root node is deleted";
    // watch root node
    int zk_errno = zk::ZE_OK;
    bool is_exist = false;
    std::string root_tablet_addr;
    while (!WatchRootNode(&is_exist, &root_tablet_addr, &zk_errno)) {
        LOG(ERROR) << "fail to root node : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch root node success";
    if (!root_tablet_addr.empty()) {
        m_tabletnode_impl->SetRootTabletAddr(root_tablet_addr);
    }
}

void TabletNodeZkAdapter::OnRootNodeChanged(const std::string& root_tablet_addr) {
    LOG(INFO) << "root node is changed";
    m_tabletnode_impl->SetRootTabletAddr(root_tablet_addr);
}

void TabletNodeZkAdapter::OnSafeModeMarkCreated() {
    LOG(INFO) << "safemode mark node is created";
    m_tabletnode_impl->EnterSafeMode();
}

void TabletNodeZkAdapter::OnSafeModeMarkDeleted() {
    LOG(INFO) << "safemode mark node is deleted";
    m_tabletnode_impl->LeaveSafeMode();
}

void TabletNodeZkAdapter::OnKickMarkCreated() {
    LOG(FATAL) << "kick mark node is created";
    exit(1);
//    Finalize();
//    m_tabletnode_impl->ExitService();
}

void TabletNodeZkAdapter::OnSelfNodeDeleted() {
    LOG(FATAL) << "self node is deleted";
    exit(1);
//    m_tabletnode_impl->ExitService();
}

void TabletNodeZkAdapter::OnChildrenChanged(const std::string& path,
                                            const std::vector<std::string>& name_list,
                                            const std::vector<std::string>& data_list) {
    LOG(ERROR) << "unexpected children event on path : " << path;
}

void TabletNodeZkAdapter::OnNodeValueChanged(const std::string& path,
                                             const std::string& value) {
    if (path.compare(kRootTabletNodePath) == 0) {
        OnRootNodeChanged(value);
    } else {
        LOG(ERROR) << "unexpected value event on path : " << path;
    }
}

void TabletNodeZkAdapter::OnNodeCreated(const std::string& path) {
    if (path.compare(kSafeModeNodePath) == 0) {
        OnSafeModeMarkCreated();
    } else if (path.compare(kRootTabletNodePath) == 0) {
        OnRootNodeCreated();
    } else if (path.compare(m_kick_node_path) == 0) {
        OnKickMarkCreated();
    } else {
        LOG(ERROR) << "unexcepted node create event on path : " << path;
    }
}

void TabletNodeZkAdapter::OnNodeDeleted(const std::string& path) {
    if (path.compare(kSafeModeNodePath) == 0) {
        OnSafeModeMarkDeleted();
    } else if (path.compare(kRootTabletNodePath) == 0) {
        OnRootNodeDeleted();
    } else if (path.compare(m_serve_node_path) == 0) {
        OnSelfNodeDeleted();
    } else {
        LOG(ERROR) << "unexcepted node delete event on path : " << path;
    }
}

void TabletNodeZkAdapter::OnWatchFailed(const std::string& path,
                                             int watch_type, int err) {
    LOG(ERROR) << "watch " << path << " fail!";
    _Exit(EXIT_FAILURE);
}

void TabletNodeZkAdapter::OnSessionTimeout() {
    LOG(ERROR) << "zk session timeout!";
    _Exit(EXIT_FAILURE);
}

} // namespace tabletnode
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
