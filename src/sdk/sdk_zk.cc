// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_zk.h"

#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gflags/gflags.h>

#include "common/this_thread.h"
#include "ins_sdk.h"
#include "types.h"
#include "utils/utils_cmd.h"
#include "zk/zk_adapter.h"

DECLARE_string(tera_zk_lib_log_path);
DECLARE_string(tera_fake_zk_path_prefix);
DECLARE_bool(tera_zk_enabled);
DECLARE_bool(tera_mock_zk_enabled);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_int32(tera_zk_timeout);
DECLARE_int32(tera_zk_retry_max_times);
DECLARE_int64(tera_zk_retry_period);
DECLARE_bool(tera_ins_enabled);
DECLARE_string(tera_ins_root_path);
DECLARE_string(tera_ins_addr_list);
DECLARE_int64(tera_sdk_ins_session_timeout);
DECLARE_bool(tera_mock_ins_enabled);
DECLARE_bool(tera_timeoracle_mock_enabled);
DECLARE_string(tera_timeoracle_mock_root_path);
DECLARE_string(tera_coord_type);

namespace tera {
namespace sdk {

static pthread_once_t zk_init_once = PTHREAD_ONCE_INIT;

static void InitZkLogOnce() {
    zk::ZooKeeperLightAdapter::SetLibraryLogOutput(FLAGS_tera_zk_lib_log_path);
}

bool ClientZkAdapter::Init() {
    pthread_once(&zk_init_once, InitZkLogOnce);
    MutexLock lock(&mutex_);
    LOG(INFO) << "try init zk ...";
    int zk_errno = zk::ZE_OK;
    int32_t retry_cnt = 0;
    int wait_time = 60000;
    while (!ZooKeeperAdapter::Init(FLAGS_tera_zk_addr_list,
                                   FLAGS_tera_zk_root_path,
                                   FLAGS_tera_zk_timeout,
                                   "", &zk_errno, wait_time)) {
        if (retry_cnt++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to init zk: " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "init zk fail: " << zk::ZkErrnoToString(zk_errno)
            << ". retry in " << FLAGS_tera_zk_retry_period << " ms, retry: "
            << retry_cnt;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "init zk success";
    return true;
}

bool ClientZkAdapter::RegisterClient(std::string* path) {
    int64_t session_id = 0;
    int zk_errno = zk::ZE_OK;
    int32_t retry_cnt = 0;
    LOG(INFO) << "try get client sesssion";
    while (!GetSessionId(&session_id, &zk_errno)) {
        if (retry_cnt++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to get client session : " 
                       << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "get client session fail: " << zk::ZkErrnoToString(zk_errno)
            << ". retry in " << FLAGS_tera_zk_retry_period << " ms, retry: "
            << retry_cnt;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    std::string internal_path = utils::GetLocalHostAddr()
            + "-" + std::to_string(getpid())
            + "-" + std::to_string(session_id);
    LOG(INFO) << "get client session success : " << internal_path;
    zk_errno = zk::ZE_OK;
    retry_cnt = 0;
    LOG(INFO) << "try create client node : " << internal_path;
    while (!CreateEphemeralNode(kClientsNodePath + "/" + internal_path, 
                                "", 
                                &zk_errno)) {
        if (retry_cnt++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to create client node : " 
                       << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "create client node fail: " << zk::ZkErrnoToString(zk_errno)
            << ". retry in " << FLAGS_tera_zk_retry_period << " ms, retry: "
            << retry_cnt;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "create client node success";
    *path = internal_path;
    return true;
}

bool ClientZkAdapter::IsClientAlive(const std::string& path) {
    VLOG(12) << "try check client alive : " << path;
    int32_t retry_cnt = 0;
    int zk_errno = zk::ZE_OK;
    bool ret = true;
    while (!CheckExist(kClientsNodePath + "/" + path, &ret, &zk_errno)) {
        if (retry_cnt++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to check client alive : " 
                       << zk::ZkErrnoToString(zk_errno);
            // when zk server error, client should think other client is alive
            return true;
        }
        LOG(ERROR) << "check client alive fail: " << zk::ZkErrnoToString(zk_errno)
            << ". retry in " << FLAGS_tera_zk_retry_period << " ms, retry: "
            << retry_cnt;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    VLOG(12) << "check client alive success";
    return ret;
}

bool ClientZkAdapter::ReadNode(const std::string& path, std::string* value) {
    VLOG(12) << "try read node : " << path;
    int32_t retry_cnt = 0;
    int zk_errno = zk::ZE_OK;
    while (!ZooKeeperAdapter::ReadNode(path, value, &zk_errno)) {
        if (retry_cnt++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to read node : " 
                       << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        LOG(ERROR) << "read node fail: " << zk::ZkErrnoToString(zk_errno)
            << ". retry in " << FLAGS_tera_zk_retry_period << " ms, retry: "
            << retry_cnt;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    VLOG(12) << "read node success";
    return true;
}

bool InsClientZkAdapter::Init() {
    ins_sdk_ = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);
    ins_sdk_->SetTimeoutTime(FLAGS_tera_sdk_ins_session_timeout);
    return true;
}

bool InsClientZkAdapter::RegisterClient(std::string* path) {
    std::string internal_path = utils::GetLocalHostAddr()
            + "-" + std::to_string(getpid())
            + "-" + ins_sdk_->GetSessionID();
    LOG(INFO) << "get client session success : " << internal_path;
    std::string client_path = FLAGS_tera_ins_root_path + kClientsNodePath 
        + "/" + internal_path;
    galaxy::ins::sdk::SDKError err;
    bool ret = ins_sdk_->Put(client_path, "", &err);
    if (ret) {
        *path = internal_path;
    }
    return ret;
}

bool InsClientZkAdapter::IsClientAlive(const std::string& path) {
    std::string client_path = kClientsNodePath + "/" + path;
    std::string value;
    return ReadNode(client_path, &value);
}

bool InsClientZkAdapter::ReadNode(const std::string& path, std::string* value) {
    std::string target_path = FLAGS_tera_ins_root_path + path;
    galaxy::ins::sdk::SDKError err;
    if (!ins_sdk_->Get(target_path, value, &err)) {
        LOG(ERROR) << "ins read " << target_path << " fail: " << err;
        return false;
    }
    return true;
}

std::string ClusterFinder::MasterAddr(bool update) {
    std::string master_addr;
    if (update || master_addr_ == "") {
        if (!ReadNode(kMasterNodePath, &master_addr)) {
            master_addr = "";
        }
    }
    if (!master_addr.empty()) {
        MutexLock lock(&mutex_);
        master_addr_ = master_addr;
        LOG(INFO) << "master addr: " << master_addr_;
    }
    return master_addr_;
}

std::string ClusterFinder::TimeoracleAddr(bool update) {
    std::string timeoracle_addr;
    if (update || timeoracle_addr_ == "") {
        if (!ReadNode(kTimeoracleNodePath, &timeoracle_addr)) {
            timeoracle_addr = "";
        }
    }
    if (!timeoracle_addr.empty()) {
        MutexLock lock(&mutex_);
        timeoracle_addr_ = timeoracle_addr;
        LOG(INFO) << "timeoracle addr: " << timeoracle_addr_;
    }
    return timeoracle_addr_;
}

std::string ClusterFinder::RootTableAddr(bool update) {
    std::string root_table_addr;
    if (update || root_table_addr_ == "") {
        if (!ReadNode(kRootTabletNodePath, &root_table_addr)) {
            root_table_addr = "";
        }
    }
    if (!root_table_addr.empty()) {
        MutexLock lock(&mutex_);
        root_table_addr_ = root_table_addr;
        LOG(INFO) << "root addr: " << root_table_addr_;
    }
    return root_table_addr_;
}

std::string ClusterFinder::ClusterId() {
    std::string name = Name();
    std::string authority = Authority();
    std::string path = Path();
    std::string cluster_id = name + "://" + authority;
    if (path[0] != '/') {
        cluster_id += "/";
    }
    cluster_id += path;
    return cluster_id;
}

ZkClusterFinder::ZkClusterFinder(const std::string& zk_root_path,
                                 const std::string& zk_addr_list,
                                 ClientZkAdapterBase* zk_adapter)
    : zk_root_path_(zk_root_path), 
      zk_addr_list_(zk_addr_list), 
      zk_adapter_(zk_adapter) {
}

bool ZkClusterFinder::ReadNode(const std::string& name, std::string* value) {
    if (zk_adapter_ == NULL) {
        pthread_once(&zk_init_once, InitZkLogOnce);

        int zk_errno = tera::zk::ZE_OK;
        zk::ZooKeeperLightAdapter zk_adapter;
        if (!zk_adapter.Init(zk_addr_list_, zk_root_path_, 1000 * 15, "", &zk_errno)) {
            LOG(ERROR) << "Init zookeeper fail: " << tera::zk::ZkErrnoToString(zk_errno);
            return false;
        }

        if (!zk_adapter.ReadNode(name, value, &zk_errno)) {
            LOG(ERROR) << "zk read " << name << " fail: " << zk::ZkErrnoToString(zk_errno);
            return false;
        }
        return true;
    } else {
        return zk_adapter_->ReadNode(name, value);
    }
}

InsClusterFinder::InsClusterFinder(const std::string& ins_root_path,
                                   const std::string& ins_addr_list,
                                   ClientZkAdapterBase* zk_adapter)
    : ins_root_path_(ins_root_path), 
      ins_addr_list_(ins_addr_list), 
      zk_adapter_(zk_adapter) {
}

bool InsClusterFinder::ReadNode(const std::string& name, std::string* value) {
    if (zk_adapter_ == NULL) {
        galaxy::ins::sdk::InsSDK ins_sdk(ins_addr_list_);
        galaxy::ins::sdk::SDKError err;
        if (!ins_sdk.Get(ins_root_path_ + name, value, &err)) {
            LOG(ERROR) << "ins read " << name << " fail: " << err;
            return false;
        }
        return true;
    } else {
        return zk_adapter_->ReadNode(name, value);
    }
}

FakeZkClusterFinder::FakeZkClusterFinder(const std::string& fake_zk_path_prefix)
    : fake_zk_path_prefix_(fake_zk_path_prefix) {
}

bool FakeZkClusterFinder::ReadNode(const std::string& name, std::string* value) {
    return zk::FakeZkUtil::ReadNode(fake_zk_path_prefix_ + name, value);
}

MockTimeoracleClusterFinder::MockTimeoracleClusterFinder(const std::string& mock_root_path) {
    mock_root_path_ = mock_root_path;
}

bool MockTimeoracleClusterFinder::ReadNode(const std::string& kpath, std::string* value) {
    std::string path = mock_root_path_ + kpath;
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        return false;
    }

    value->resize(1024);
    char *buf = &(*value)[0];
    ssize_t len = ::pread(fd, buf, sizeof(buf), 0);
    ::close(fd);
    if (len < 0) {
        return false;
    }
    value->resize(len);
    return true;
}

ClientZkAdapterBase* NewClientZkAdapter() {
    if (FLAGS_tera_coord_type.empty()) {
        LOG(ERROR) << "Note: We don't recommend that use '--tera_[zk|ins|mock_zk|mock_ins]_enabled' flag for your cluster coord"
                   << " replace by '--tera_coord_type=[zk|ins|mock_zk|mock_ins|fake_zk]' flag is usually recommended.";
    }

    if (FLAGS_tera_coord_type == "zk"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_zk_enabled)) {
        return new sdk::ClientZkAdapter();
    } else if (FLAGS_tera_coord_type == "ins"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_ins_enabled)) {
        return new sdk::InsClientZkAdapter();
    } else if (FLAGS_tera_coord_type == "mock_zk"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_mock_zk_enabled)) {
        return new sdk::MockClientZkAdapter();
    } else if (FLAGS_tera_coord_type == "mock_ins"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_mock_ins_enabled)) {
        return new sdk::MockInsClientZkAdapter();
    }
    return NULL;
}

ClusterFinder* NewClusterFinder(ClientZkAdapterBase* zk_adapter) {
    if (FLAGS_tera_coord_type.empty()) {
        LOG(ERROR) << "Note: We don't recommend that use '--tera_[zk|ins|mock_zk|mock_ins]_enabled' flag for your cluster coord"
                   << " replace by '--tera_coord_type=[zk|ins|mock_zk|mock_ins|fake_zk]' flag is usually recommended.";
    }
    if (FLAGS_tera_coord_type == "zk" 
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_zk_enabled)) {
        return new sdk::ZkClusterFinder(FLAGS_tera_zk_root_path, FLAGS_tera_zk_addr_list, zk_adapter);
    } else if (FLAGS_tera_coord_type == "ins" 
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_ins_enabled)) {
        return new sdk::InsClusterFinder(FLAGS_tera_ins_root_path, FLAGS_tera_ins_addr_list, zk_adapter);
    } else if (FLAGS_tera_coord_type == "mock_zk" 
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_mock_zk_enabled)) {
        return new sdk::MockZkClusterFinder(FLAGS_tera_zk_root_path, FLAGS_tera_zk_addr_list);
    } else if (FLAGS_tera_coord_type == "mock_ins" 
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_mock_ins_enabled)) {
        return new sdk::MockInsClusterFinder(FLAGS_tera_ins_root_path, FLAGS_tera_ins_addr_list);
    } else if (FLAGS_tera_coord_type == "fake_zk" 
            || FLAGS_tera_coord_type.empty()) {
        return new sdk::FakeZkClusterFinder(FLAGS_tera_fake_zk_path_prefix);
    }
    return nullptr;
}

ClusterFinder* NewTimeoracleClusterFinder() {
    if (FLAGS_tera_timeoracle_mock_enabled) {
        return new sdk::MockTimeoracleClusterFinder(FLAGS_tera_timeoracle_mock_root_path);
    } else if (FLAGS_tera_coord_type == "zk") {
        return new sdk::ZkClusterFinder(FLAGS_tera_zk_root_path, FLAGS_tera_zk_addr_list);
    } else if (FLAGS_tera_coord_type == "ins") {
        return new sdk::InsClusterFinder(FLAGS_tera_ins_root_path, FLAGS_tera_ins_addr_list);
    }

    return nullptr;
}

}  // namespace sdk
}  // namespace tera
