// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/this_thread.h"

#include "tabletnode/tabletnode_zk_adapter.h"
#include "types.h"
#include "zk/zk_util.h"
#include "ins_sdk.h"

DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_string(tera_fake_zk_path_prefix);
DECLARE_string(tera_tabletnode_port);
DECLARE_int32(tera_zk_timeout);
DECLARE_int64(tera_zk_retry_period);
DECLARE_string(tera_ins_addr_list);
DECLARE_string(tera_ins_root_path);

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

    // enter running state
    int64_t session_id_int = 0;
    if (!GetSessionId(&session_id_int, &zk_errno)) {
        LOG(ERROR) << "get session id fail : " << zk::ZkErrnoToString(zk_errno);
        return;
    }
    char session_id_str[32];
    sprintf(session_id_str, "%016lx", session_id_int);
    m_tabletnode_impl->SetSessionId(session_id_str);
    m_tabletnode_impl->SetTabletNodeStatus(TabletNodeImpl::kIsRunning);

    // create my node
    while (!Register(m_tabletnode_impl->GetSessionId(), &zk_errno)) {
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
}

bool TabletNodeZkAdapter::GetRootTableAddr(std::string* root_table_addr) {
    return true;
}

bool TabletNodeZkAdapter::Register(const std::string& session_id, int* zk_errno) {
    // create serve node
    std::string node_path = kTsListPath + "/" + session_id + "#";
    std::string node_value = m_server_addr;
    std::string ret_node_path;
    if (!CreateSequentialEphemeralNode(node_path, node_value, &ret_node_path,
                                       zk_errno)) {
        LOG(ERROR) << "create serve node fail";
        return false;
    }
    m_serve_node_path = ret_node_path;
    m_kick_node_path = kKickPath + "/" + zk::ZooKeeperUtil::GetNodeName(m_serve_node_path.c_str());
    LOG(INFO) << "create serve node success, node_path " << node_path << ", " << ret_node_path
        << ", " << m_serve_node_path << ", " << m_kick_node_path;
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

bool TabletNodeZkAdapter::WatchRootNode(bool* is_exist,
                                        std::string* root_tablet_addr,
                                        int* zk_errno) {
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

FakeTabletNodeZkAdapter::FakeTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                                                 const std::string& server_addr)
    : m_tabletnode_impl(tabletnode_impl), m_server_addr(server_addr) {
    m_fake_path = FLAGS_tera_fake_zk_path_prefix + "/";
}

void FakeTabletNodeZkAdapter::Init() {
    // get session
    m_tabletnode_impl->SetSessionId(FLAGS_tera_tabletnode_port);
    m_tabletnode_impl->SetTabletNodeStatus(TabletNodeImpl::kIsRunning);

    if (!Register(m_tabletnode_impl->GetSessionId())) {
        LOG(FATAL) << "fail to create fake serve-node.";
    }
    LOG(INFO) << "create fake serve-node success: " << m_tabletnode_impl->GetSessionId();
}

bool FakeTabletNodeZkAdapter::Register(const std::string& session_id, int* zk_code) {
    MutexLock locker(&m_mutex);
    std::string node_name = m_fake_path + kTsListPath + "/" + session_id;

    if (!zk::FakeZkUtil::WriteNode(node_name, m_server_addr)) {
        LOG(FATAL) << "fake zk error: " << node_name
            << ", " << m_server_addr;
    }
    return true;
}

bool FakeTabletNodeZkAdapter::GetRootTableAddr(std::string* root_table_addr) {
    MutexLock locker(&m_mutex);
    std::string root_table = m_fake_path + kRootTabletNodePath;
    if (!zk::FakeZkUtil::ReadNode(root_table, root_table_addr)) {
        LOG(FATAL) << "fake zk error: " << root_table
            << ", " << *root_table_addr;
    }
    return true;
}

InsTabletNodeZkAdapter::InsTabletNodeZkAdapter(TabletNodeImpl* tabletnode_impl,
                                               const std::string& server_addr)
    : m_tabletnode_impl(tabletnode_impl), m_server_addr(server_addr) {

}

static void InsOnKick(const galaxy::ins::sdk::WatchParam& param,
                      galaxy::ins::sdk::SDKError error) {
    LOG(INFO) << "recv kick event" ;
    InsTabletNodeZkAdapter* ins_adp = static_cast<InsTabletNodeZkAdapter*>(param.context);
    ins_adp->OnKickMarkCreated();
}

static void InsOnLockChange(const galaxy::ins::sdk::WatchParam& param,
                           galaxy::ins::sdk::SDKError error) {
    LOG(INFO) << "recv lock change event" ;
    InsTabletNodeZkAdapter* ins_adp = static_cast<InsTabletNodeZkAdapter*>(param.context);
    ins_adp->OnLockChange(param.value, param.deleted);
}

static void InsOnMetaChange(const galaxy::ins::sdk::WatchParam& param,
                            galaxy::ins::sdk::SDKError error) {
    LOG(INFO) << "recv meta change event" ;
    InsTabletNodeZkAdapter* ins_adp = static_cast<InsTabletNodeZkAdapter*>(param.context);
    ins_adp->OnMetaChange(param.value, param.deleted);
}

void InsTabletNodeZkAdapter::Init() {
    std::string root_path = FLAGS_tera_ins_root_path;
    galaxy::ins::sdk::SDKError err;
    // create session
    m_ins_sdk = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);

    // get session id
    std::string session_id = m_ins_sdk->GetSessionID();
    m_tabletnode_impl->SetSessionId(session_id);
    m_tabletnode_impl->SetTabletNodeStatus(TabletNodeImpl::kIsRunning);

    // create node
    std::string lock_key = root_path + kTsListPath + "/" + m_server_addr;
    CHECK(m_ins_sdk->Lock(lock_key, &err)) << "register fail";
    LOG(INFO) << "create ts-node success: " << session_id;

    // create watch node
    std::string kick_key = root_path + kKickPath + "/" + session_id;
    CHECK(m_ins_sdk->Watch(kick_key, &InsOnKick, this, &err)) << "watch kick fail";
    CHECK(m_ins_sdk->Watch(lock_key, &InsOnLockChange, this, &err))
          << "watch lock fail";
    std::string meta_table = root_path + kRootTabletNodePath;
    CHECK(m_ins_sdk->Watch(meta_table, &InsOnMetaChange, this, &err))
          << "watch meta table fail";
}

void InsTabletNodeZkAdapter::OnMetaChange(std::string meta_addr, bool deleted) {
    (void) meta_addr;
    (void) deleted;
    std::string cur_meta;
    std::string root_path = FLAGS_tera_ins_root_path;
    std::string meta_table = root_path + kRootTabletNodePath;
    galaxy::ins::sdk::SDKError err;
    GetRootTableAddr(&cur_meta);
    CHECK(m_ins_sdk->Watch(meta_table, &InsOnMetaChange, this, &err))
          << "watch meta table fail";
    if (!cur_meta.empty()) {
        MutexLock locker(&m_mutex);
        m_tabletnode_impl->SetRootTabletAddr(cur_meta);
    }
}

void InsTabletNodeZkAdapter::OnKickMarkCreated() {
    LOG(FATAL) << "I am kicked by master";
    abort();
}

void InsTabletNodeZkAdapter::OnLockChange(std::string session_id, bool deleted) {
    if (deleted || session_id != m_ins_sdk->GetSessionID()) {
        LOG(FATAL) << "I lost my lock , so quit";
        abort();
    }
}

bool InsTabletNodeZkAdapter::GetRootTableAddr(std::string* root_table_addr) {
    MutexLock locker(&m_mutex);
    std::string root_path = FLAGS_tera_ins_root_path;
    std::string meta_table = root_path + kRootTabletNodePath;
    galaxy::ins::sdk::SDKError err;
    std::string value;
    CHECK(m_ins_sdk->Get(meta_table, &value, &err));
    *root_table_addr = value;
    return true;
}

} // namespace tabletnode
} // namespace tera
