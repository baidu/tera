// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_zk.h"

#include <iostream>
#include <gflags/gflags.h>

#include "ins_sdk.h"

#include "types.h"
#include "zk/zk_adapter.h"

DECLARE_string(tera_zk_lib_log_path);
DECLARE_string(tera_fake_zk_path_prefix);
DECLARE_bool(tera_zk_enabled);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_bool(tera_ins_enabled);
DECLARE_string(tera_ins_root_path);
DECLARE_string(tera_ins_addr_list);

namespace tera {
namespace sdk {

std::string ClusterFinder::MasterAddr(bool update) {
    std::string master_addr;
    if (update || _master_addr == "") {
        if (!ReadNode(kMasterNodePath, &master_addr)) {
            master_addr = "";
        }
    }
    if (!master_addr.empty()) {
        MutexLock lock(&_mutex);
        _master_addr = master_addr;
        LOG(INFO) << "master addr: " << _master_addr;
    }
    return _master_addr;
}

std::string ClusterFinder::RootTableAddr(bool update) {
    std::string root_table_addr;
    if (update || _root_table_addr == "") {
        if (!ReadNode(kRootTabletNodePath, &root_table_addr)) {
            root_table_addr = "";
        }
    }
    if (!root_table_addr.empty()) {
        MutexLock lock(&_mutex);
        _root_table_addr = root_table_addr;
        LOG(INFO) << "root addr: " << _root_table_addr;
    }
    return _root_table_addr;
}

ZkClusterFinder::ZkClusterFinder(const std::string& zk_root_path,
                                 const std::string& zk_addr_list)
    : _zk_root_path(zk_root_path), _zk_addr_list(zk_addr_list) {
}

static pthread_once_t zk_init_once = PTHREAD_ONCE_INIT;

static void InitZkLogOnce() {
    zk::ZooKeeperLightAdapter::SetLibraryLogOutput(FLAGS_tera_zk_lib_log_path);
}

bool ZkClusterFinder::ReadNode(const std::string& name, std::string* value) {
    pthread_once(&zk_init_once, InitZkLogOnce);

    int zk_errno = tera::zk::ZE_OK;
    zk::ZooKeeperLightAdapter zk_adapter;
    if (!zk_adapter.Init(_zk_addr_list, _zk_root_path, 1000 * 15, "", &zk_errno)) {
        LOG(ERROR) << "Init zookeeper fail: " << tera::zk::ZkErrnoToString(zk_errno);
        return false;
    }

    if (!zk_adapter.ReadNode(name, value, &zk_errno)) {
        LOG(ERROR) << "zk read " << name << " fail: " << zk::ZkErrnoToString(zk_errno);
        return false;
    }
    return true;
}

InsClusterFinder::InsClusterFinder(const std::string& ins_root_path,
                                   const std::string& ins_addr_list)
    : _ins_root_path(ins_root_path), _ins_addr_list(ins_addr_list) {
}

bool InsClusterFinder::ReadNode(const std::string& name, std::string* value) {
    galaxy::ins::sdk::InsSDK ins_sdk(_ins_addr_list);
    galaxy::ins::sdk::SDKError err;
    if (!ins_sdk.Get(_ins_root_path + name, value, &err)) {
        LOG(ERROR) << "ins read " << name << " fail: " << err;
        return false;
    }
    return true;
}

FakeZkClusterFinder::FakeZkClusterFinder(const std::string& fake_zk_path_prefix)
    : _fake_zk_path_prefix(fake_zk_path_prefix) {
}

bool FakeZkClusterFinder::ReadNode(const std::string& name, std::string* value) {
    return zk::FakeZkUtil::ReadNode(_fake_zk_path_prefix + name, value);
}

ClusterFinder* NewClusterFinder() {
    if (FLAGS_tera_ins_enabled) {
        return new sdk::InsClusterFinder(FLAGS_tera_ins_root_path, FLAGS_tera_ins_addr_list);
    } else if (!FLAGS_tera_zk_enabled) {
        return new sdk::FakeZkClusterFinder(FLAGS_tera_fake_zk_path_prefix);
    } else {
        return new sdk::ZkClusterFinder(FLAGS_tera_zk_root_path, FLAGS_tera_zk_addr_list);
    }
}

}  // namespace sdk
}  // namespace tera
