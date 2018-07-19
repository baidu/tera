// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/master_zk_adapter.h"

#include "common/file/file_path.h"
#include "types.h"
#include "zk/zk_util.h"
#include "ins_sdk.h"

DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_string(tera_fake_zk_path_prefix);
DECLARE_int32(tera_zk_timeout);
DECLARE_int64(tera_zk_retry_period);
DECLARE_int32(tera_zk_retry_max_times);
DECLARE_string(tera_ins_addr_list);
DECLARE_string(tera_ins_root_path);
DECLARE_int64(tera_master_ins_session_timeout);

namespace tera {
namespace master {

MasterZkAdapter::MasterZkAdapter(MasterImpl * master_impl,
                                 const std::string& server_addr)
    : master_impl_(master_impl), server_addr_(server_addr) {
}

MasterZkAdapter::~MasterZkAdapter() {
}

bool MasterZkAdapter::Init(std::string* root_tablet_addr,
                           std::map<std::string, std::string>* tabletnode_list,
                           bool* safe_mode) {
    MutexLock lock(&mutex_);

    if (!Setup()) {
        return false;
    }

    if (!LockMasterLock()) {
        Reset();
        return false;
    }

    if (!WatchMasterLock()) {
        UnlockMasterLock();
        Reset();
        return false;
    }

    if (!CreateMasterNode()) {
        UnlockMasterLock();
        Reset();
        return false;
    }

    bool root_tablet_node_exist = false;
    if (!WatchRootTabletNode(&root_tablet_node_exist, root_tablet_addr)) {
        DeleteMasterNode();
        UnlockMasterLock();
        Reset();
        return false;
    }

    if (!WatchSafeModeMark(safe_mode)) {
        DeleteMasterNode();
        UnlockMasterLock();
        Reset();
        return false;
    }

    if (!WatchTabletNodeList(tabletnode_list)) {
        DeleteMasterNode();
        UnlockMasterLock();
        Reset();
        return false;
    }

    return true;
}

bool MasterZkAdapter::Setup() {
    LOG(INFO) << "try init zk...";
    int zk_errno = zk::ZE_OK;
    int32_t retry_count = 0;
    while (!ZooKeeperAdapter::Init(FLAGS_tera_zk_addr_list,
                                   FLAGS_tera_zk_root_path,
                                   FLAGS_tera_zk_timeout,
                                   server_addr_, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to init zk: " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "init zk fail: " << zk::ZkErrnoToString(zk_errno)
            << ". retry in " << FLAGS_tera_zk_retry_period << " ms, retry: "
            << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "init zk success";
    return true;
}

void MasterZkAdapter::Reset() {
    Finalize();
}

bool MasterZkAdapter::LockMasterLock() {
    LOG(INFO) << "try lock master-lock...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!SyncLock(kMasterLockPath, &zk_errno, -1)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to acquire master lock " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry lock master-lock in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "acquire master lock success";
    return true;
}

bool MasterZkAdapter::UnlockMasterLock() {
    LOG(INFO) << "try release master-lock...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!Unlock(kMasterLockPath, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to release master-lock";
            return false;
        }
        LOG(ERROR) << "retry unlock master-lock in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "release master-lock success";
    return true;
}

bool MasterZkAdapter::CreateMasterNode() {
    LOG(INFO) << "try create master node...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!CreateEphemeralNode(kMasterNodePath, server_addr_, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to create master node " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry create master node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "create master node success";
    return true;
}

bool MasterZkAdapter::DeleteMasterNode() {
    LOG(INFO) << "try delete master node...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!DeleteNode(kMasterNodePath, &zk_errno)
        && zk_errno != zk::ZE_NOT_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to delete master node " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry delete master node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "delete master node success";
    return true;
}

bool MasterZkAdapter::KickTabletServer(const std::string& ts_host,
                                       const std::string& ts_zk_id) {
    MutexLock lock(&mutex_);
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!CreatePersistentNode(kKickPath + "/" + ts_zk_id, ts_host, &zk_errno)
        && zk_errno != zk::ZE_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to kick ts [" << ts_host << "] "
                << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry kick ts in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "kick ts [" << ts_host << "] success";
    return true;
}

bool MasterZkAdapter::MarkSafeMode() {
    MutexLock lock(&mutex_);
    LOG(INFO) << "try mark safemode...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!CreatePersistentNode(kSafeModeNodePath, "safemode", &zk_errno)
        && zk_errno != zk::ZE_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to mark safemode " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry mark safemode in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "mark safemode success";
    return true;
}

bool MasterZkAdapter::UnmarkSafeMode() {
    MutexLock lock(&mutex_);
    LOG(INFO) << "try unmark safemode...";
    int zk_errno = zk::ZE_OK;
    int32_t retry_count = 0;
    while (!DeleteNode(kSafeModeNodePath, &zk_errno)
        && zk_errno != zk::ZE_NOT_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to unmark safemode " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry unmark safemode in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "unmark safemode success";
    return true;
}

bool MasterZkAdapter::UpdateRootTabletNode(const std::string& root_tablet_addr) {
    MutexLock lock(&mutex_);
    LOG(INFO) << "try update root node...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!WriteNode(kRootTabletNodePath, root_tablet_addr, &zk_errno)
        && zk_errno != zk::ZE_NOT_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(INFO) << "fail to update root node " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry update root node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    if (zk_errno == zk::ZE_OK) {
        LOG(INFO) << "update root node success";
        return true;
    }

    LOG(INFO) << "root node not exist, try create root node...";
    retry_count = 0;
    zk_errno = zk::ZE_OK;
    while (!CreatePersistentNode(kRootTabletNodePath, root_tablet_addr,
                                 &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to create root node " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry create root node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "create root node success";
    return true;
}

bool MasterZkAdapter::WatchRootTabletNode(bool* is_exist,
                                          std::string* root_tablet_addr) {
    LOG(INFO) << "try check root node exist...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!CheckAndWatchExist(kRootTabletNodePath, is_exist, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to check root node exist " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry check root node exist in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    if (!*is_exist) {
        LOG(INFO) << "root node not exist";
        return true;
    }

    LOG(INFO) << "root node exist, try read root node...";
    retry_count = 0;
    zk_errno = zk::ZE_OK;
    while (!ReadAndWatchNode(kRootTabletNodePath, root_tablet_addr, &zk_errno)
        && zk_errno != zk::ZE_NOT_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to read root node " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry read root node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    if (zk_errno == zk::ZE_NOT_EXIST) {
        *is_exist = false;
        LOG(INFO) << "root node not exist";
        return true;
    }
    LOG(INFO) << "root node value=[" << *root_tablet_addr << "]";
    return true;
}

bool MasterZkAdapter::WatchSafeModeMark(bool* is_safemode) {
    LOG(INFO) << "try watch safemode mark...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!CheckAndWatchExist(kSafeModeNodePath, is_safemode, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to watch safemode mark" << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry watch safe mode mark in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "watch safemode success";
    return true;
}

bool MasterZkAdapter::WatchTabletNodeList(std::map<std::string, std::string>* tabletnode_list) {
    LOG(INFO) << "try watch tabletnode list...";
    std::vector<std::string> name_list;
    std::vector<std::string> data_list;
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!ListAndWatchChildren(kTsListPath, &name_list, &data_list,
                                 &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to watch tabletnode list " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry watch tabletnode list in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    size_t list_count = name_list.size();
    for (size_t i = 0; i < list_count; i++) {
        const std::string& name = name_list[i];
        const std::string& data = data_list[i];
        int seq_num = zk::ZooKeeperUtil::GetSequenceNo(name);
        if (seq_num < 0) {
            LOG(ERROR) << "ignore non-sequential node";
            continue;
        }
        if (data == "") {
            LOG(ERROR) << "cannot get value of child : " << name;
            continue;
        }
        // keep larger(newer) sequence id
        std::map<std::string, std::string>::iterator it = tabletnode_list->find(data);
        if (it != tabletnode_list->end()) {
            int prev_seq_num = zk::ZooKeeperUtil::GetSequenceNo(it->second);
            if (prev_seq_num > seq_num) {
                VLOG(5) << "ignore old node: " << data << " " << name;
                continue;
            }
        }
        // TODO: check value
        (*tabletnode_list)[data] = name;
    }
    LOG(INFO) << "watch tabletnode list success";
    return true;
}

bool MasterZkAdapter::WatchMasterLock() {
    LOG(INFO) << "watch master lock ...";
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!WatchZkLock(kMasterLockPath, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to watch master lock " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "retry watch master-lock in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "watch master lock success";
    return true;
}

void MasterZkAdapter::OnSafeModeMarkCreated() {
    LOG(ERROR) << "safemode mark node is created";
}

void MasterZkAdapter::OnSafeModeMarkDeleted() {
    LOG(ERROR) << "safemode mark node is deleted";
}

void MasterZkAdapter::OnTabletNodeListDeleted() {
    LOG(ERROR) << "ts dir node is deleted";
    if (!MarkSafeMode()) {
        master_impl_->SetMasterStatus(MasterImpl::kIsSecondary);
        master_impl_->DisableQueryTabletNodeTimer();
        DeleteMasterNode();
        UnlockMasterLock();
        Reset();
    }
}

void MasterZkAdapter::OnRootTabletNodeDeleted() {
    LOG(ERROR) << "root tablet node is deleted";
    std::string root_tablet_addr;
    if (master_impl_->GetMetaTabletAddr(&root_tablet_addr)) {
        if (!UpdateRootTabletNode(root_tablet_addr)) {
            master_impl_->SetMasterStatus(MasterImpl::kIsSecondary);
            master_impl_->DisableQueryTabletNodeTimer();
            DeleteMasterNode();
            UnlockMasterLock();
            Reset();
        }
    } else {
        LOG(ERROR) << "root tablet not loaded, will not update zk";
    }
}

void MasterZkAdapter::OnMasterNodeDeleted() {
    LOG(ERROR) << "master node deleted";
    // TODO: not support from kRuning to secondary
    master_impl_->SetMasterStatus(MasterImpl::kIsSecondary);
    master_impl_->DisableQueryTabletNodeTimer();
    UnlockMasterLock();
    Reset();
}

void MasterZkAdapter::OnZkLockDeleted() {
    LOG(ERROR) << "master lock deleted, kill-self";
    master_impl_->DisableQueryTabletNodeTimer();
    Reset();
    _Exit(EXIT_FAILURE);
}

void MasterZkAdapter::OnTabletServerKickMarkCreated() {
}

void MasterZkAdapter::OnTabletServerKickMarkDeleted() {
}

void MasterZkAdapter::OnTabletServerStart(const std::string& ts_host) {
}

void MasterZkAdapter::OnTabletServerExist(const std::string& ts_host) {
}

void MasterZkAdapter::OnChildrenChanged(const std::string& path,
                                        const std::vector<std::string>& name_list,
                                        const std::vector<std::string>& data_list) {
    VLOG(5) << "OnChilerenChanged: path=[" << path << "]";
    if (path.compare(kTsListPath) != 0) {
        return;
    }
    std::map<std::string, std::string> ts_node_list;

    mutex_.Lock();
    size_t list_count = name_list.size();
    for (size_t i = 0; i < list_count; i++) {
        const std::string& name = name_list[i];
        const std::string& data = data_list[i];
        int seq_num = zk::ZooKeeperUtil::GetSequenceNo(name);
        if (seq_num < 0) {
            LOG(ERROR) << "ignore non-sequential node";
            continue;
        }
        if (data == "") {
            LOG(ERROR) << "cannot get value of child : " << name;
            continue;
        }
        // keep larger(newer) sequence id
        std::map<std::string, std::string>::iterator it = ts_node_list.find(data);
        if (it != ts_node_list.end()) {
            int prev_seq_num = zk::ZooKeeperUtil::GetSequenceNo(it->second);
            if (prev_seq_num > seq_num) {
                VLOG(5) << "ignore old node: " << data << " " << name;
                continue;
            }
        }
        // TODO: check value
        ts_node_list[data] = name;
    }
    mutex_.Unlock();
    master_impl_->RefreshTabletNodeList(ts_node_list);
}

void MasterZkAdapter::OnNodeValueChanged(const std::string& path,
                                         const std::string& value) {
    VLOG(5) << "OnNodeValueChanged: path=[" << path << "], value=["
        << value << "]";
    MutexLock lock(&mutex_);
}

void MasterZkAdapter::OnNodeCreated(const std::string& path) {
    VLOG(5) << "OnNodeCreated: path=[" << path << "]";
    MutexLock lock(&mutex_);
}

void MasterZkAdapter::OnNodeDeleted(const std::string& path) {
    VLOG(5) << "OnNodeDeleted: path=[" << path << "]";

    MutexLock lock(&mutex_);
    if (path.compare(kSafeModeNodePath) == 0) {
        OnSafeModeMarkDeleted();
    } else if (path.compare(kTsListPath) == 0) {
        OnTabletNodeListDeleted();
    } else if (path.compare(kRootTabletNodePath) == 0) {
        OnRootTabletNodeDeleted();
    } else if (path.compare(kMasterNodePath) == 0) {
        OnMasterNodeDeleted();
    } else {
    }
}

void MasterZkAdapter::OnWatchFailed(const std::string& path, int watch_type,
                                    int err) {
    LOG(ERROR) << "OnWatchFailed: path=[" << path << "], watch_type="
        << watch_type << ", err=" << err;
    // MutexLock lock(&mutex_);
    _Exit(EXIT_FAILURE);
}


void MasterZkAdapter::OnSessionTimeout() {
    LOG(ERROR) << "zk session timeout!";
    _Exit(EXIT_FAILURE);
}

FakeMasterZkAdapter::FakeMasterZkAdapter(MasterImpl * master_impl,
                                 const std::string& server_addr)
    : master_impl_(master_impl), server_addr_(server_addr) {
    fake_path_ = FLAGS_tera_fake_zk_path_prefix + "/";
}

FakeMasterZkAdapter::~FakeMasterZkAdapter() {
}

bool FakeMasterZkAdapter::Init(std::string* root_tablet_addr,
                               std::map<std::string, std::string>* tabletnode_list,
                               bool* safe_mode) {
    std::string master_lock = fake_path_ + kMasterLockPath;
    std::string master_path = fake_path_ + kMasterNodePath;
    std::string ts_list_path = fake_path_ + kTsListPath;
    std::string kick_path = fake_path_ + kKickPath;
    std::string root_path = fake_path_ + kRootTabletNodePath;

    // setup master-lock
    if (!IsEmpty(master_lock)) {
        LOG(ERROR) << "fake zk error: " << master_lock;
        _Exit(EXIT_FAILURE);
    }
    if (!zk::FakeZkUtil::WriteNode(master_lock + "/0", server_addr_)) {
        LOG(ERROR) << "fake zk error: " << master_lock + "/0, "
            << server_addr_;
        _Exit(EXIT_FAILURE);
    }
    if (!zk::FakeZkUtil::WriteNode(master_path, server_addr_)) {
        LOG(ERROR) << "fake zk error: " << master_path + ", "
            << server_addr_;
        _Exit(EXIT_FAILURE);
    }

    // get all ts
    std::vector<std::string> allts;
    if (!zk::FakeZkUtil::ListNodes(ts_list_path, &allts) && allts.size() == 0) {
        LOG(ERROR) << "fake zk error: " << ts_list_path;
        _Exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < allts.size(); ++i) {
        std::string value;
        std::string node_path = ts_list_path + "/" + allts[i];
        if (!zk::FakeZkUtil::ReadNode(node_path, &value)) {
            LOG(ERROR) << "fake zk error: " << allts[i];
            _Exit(EXIT_FAILURE);
        }
        (*tabletnode_list)[value] = allts[i];
    }

    return true;
}

bool FakeMasterZkAdapter::KickTabletServer(const std::string& ts_host,
                                           const std::string& ts_zk_id) {
    return true;
}

bool FakeMasterZkAdapter::MarkSafeMode() {
    return true;
}

bool FakeMasterZkAdapter::UnmarkSafeMode() {
    return true;
}

bool FakeMasterZkAdapter::UpdateRootTabletNode(const std::string& root_tablet_addr) {
    std::string root_table = fake_path_ + kRootTabletNodePath;
    if (!zk::FakeZkUtil::WriteNode(root_table, root_tablet_addr)) {
        LOG(ERROR) << "fake zk error: " << root_table
            << ", " << root_tablet_addr;
        _Exit(EXIT_FAILURE);
    }
    LOG(INFO) << "update fake root_table_addr: " << root_tablet_addr;
    return true;
}

void FakeMasterZkAdapter::OnChildrenChanged(const std::string& path,
                                            const std::vector<std::string>& name_list,
                                            const std::vector<std::string>& data_list) {

}

void FakeMasterZkAdapter::OnNodeValueChanged(const std::string& path,
                                             const std::string& value) {

}

void FakeMasterZkAdapter::OnNodeCreated(const std::string& path) {
}

void FakeMasterZkAdapter::OnNodeDeleted(const std::string& path) {

}

void FakeMasterZkAdapter::OnWatchFailed(const std::string& path,
                                        int watch_type,
                                        int err) {
}

void FakeMasterZkAdapter::OnSessionTimeout() {
}



InsMasterZkAdapter::InsMasterZkAdapter(MasterImpl * master_impl,
                                 const std::string& server_addr)
    : master_impl_(master_impl), server_addr_(server_addr), ins_sdk_(NULL) {

}

InsMasterZkAdapter::~InsMasterZkAdapter() {
    if (ins_sdk_) {
        std::string root_path = FLAGS_tera_ins_root_path;
        std::string master_lock = root_path + kMasterLockPath;
        galaxy::ins::sdk::SDKError err;
        ins_sdk_->UnLock(master_lock, &err);
    }
}

static void InsOnTsChange(const galaxy::ins::sdk::WatchParam& param,
                          galaxy::ins::sdk::SDKError error) {
    LOG(INFO) << "ts on ins changed event" ;
    InsMasterZkAdapter* ins_adp = static_cast<InsMasterZkAdapter*>(param.context);
    ins_adp->RefreshTabletNodeList();
}

static void InsOnLockChange(const galaxy::ins::sdk::WatchParam& param,
                            galaxy::ins::sdk::SDKError error) {
    InsMasterZkAdapter* ins_adp = static_cast<InsMasterZkAdapter*>(param.context);
    ins_adp->OnLockChange(param.value, param.deleted);
}

static void InsOnSessionTimeout(void * context) {
    InsMasterZkAdapter* ins_adp = static_cast<InsMasterZkAdapter*>(context);
    ins_adp->OnSessionTimeout();
}

bool InsMasterZkAdapter::Init(std::string* root_tablet_addr,
                               std::map<std::string, std::string>* tabletnode_list,
                               bool* safe_mode) {
    MutexLock lock(&mutex_);
    ins_sdk_ = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);
    ins_sdk_->SetTimeoutTime(FLAGS_tera_master_ins_session_timeout);
    std::string root_path = FLAGS_tera_ins_root_path;
    std::string master_lock = root_path + kMasterLockPath;
    std::string master_path = root_path + kMasterNodePath;
    std::string ts_list_path = root_path + kTsListPath;
    galaxy::ins::sdk::SDKError err;
    CHECK(ins_sdk_->Lock(master_lock, &err)) << "lock master_lock fail";
    CHECK(ins_sdk_->Put(master_path, server_addr_, &err)) << "writer master fail";
    CHECK(ins_sdk_->Watch(master_lock, InsOnLockChange, this, &err))
         << "watch master-lock fail";
    CHECK(ins_sdk_->Watch(ts_list_path, &InsOnTsChange, this, &err))
         << "watch ts list failed";
    galaxy::ins::sdk::ScanResult* result = ins_sdk_->Scan(ts_list_path+"/!",
                                                           ts_list_path+"/~");
    while (!result->Done()) {
        CHECK_EQ(result->Error(), galaxy::ins::sdk::kOK);
        std::string session_id = result->Value();
        std::string key = result->Key();
        size_t preifx_len = (ts_list_path + "/").size();
        std::string ts_addr = key.substr(preifx_len);
        (*tabletnode_list)[ts_addr] = session_id;
        result->Next();
    }
    delete result;
    ins_sdk_->RegisterSessionTimeout(InsOnSessionTimeout, this);
    return true;
}

void InsMasterZkAdapter::RefreshTabletNodeList() {
    std::string root_path = FLAGS_tera_ins_root_path;
    std::string ts_list_path = root_path + kTsListPath;
    galaxy::ins::sdk::SDKError err;
    CHECK(ins_sdk_->Watch(ts_list_path, &InsOnTsChange,
                     this, &err)) << "watch ts failed";
    galaxy::ins::sdk::ScanResult* result = ins_sdk_->Scan(ts_list_path+"/!",
                                                           ts_list_path+"/~");

    std::map<std::string, std::string> tabletnode_list;
    while (!result->Done()) {
        CHECK_EQ(result->Error(), galaxy::ins::sdk::kOK);
        std::string session_id = result->Value();
        std::string key = result->Key();
        size_t preifx_len = (ts_list_path + "/").size();
        std::string ts_addr = key.substr(preifx_len);
        tabletnode_list[ts_addr] = session_id;
        result->Next();
    }
    delete result;
    master_impl_->RefreshTabletNodeList(tabletnode_list);
}

void InsMasterZkAdapter::OnLockChange(std::string session_id, bool deleted) {
    if (deleted || session_id != ins_sdk_->GetSessionID()) {
        LOG(ERROR) << "master lock lost";
        exit(1);
    }
}

bool InsMasterZkAdapter::KickTabletServer(const std::string& ts_host,
                                          const std::string& ts_zk_id) {
    std::string root_path = FLAGS_tera_ins_root_path;
    std::string kick_path = root_path + kKickPath;
    galaxy::ins::sdk::SDKError err;
    bool ret = ins_sdk_->Put(kick_path + "/" + ts_zk_id, ts_host, &err);
    return ret;
}

bool InsMasterZkAdapter::UpdateRootTabletNode(const std::string& root_tablet_addr) {
    std::string root_path = FLAGS_tera_ins_root_path;
    std::string meta_path = root_path + kRootTabletNodePath;
    galaxy::ins::sdk::SDKError err;
    bool ret = ins_sdk_->Put(meta_path, root_tablet_addr, &err);
    return ret;
}

void InsMasterZkAdapter::OnSessionTimeout() {
    MutexLock lock(&mutex_);
    LOG(ERROR) << "ins sessiont timeout";
    _Exit(EXIT_FAILURE);
}

} // namespace master
} // namespace tera
