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
DECLARE_bool(tera_mock_ins_enabled);

namespace tera {
namespace sdk {

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
    if (name.empty() || authority.empty() || path.empty()) {
        LOG(FATAL) << "cluster name/authority/path must be non-empty";
    }
    std::string cluster_id = name + "://" + authority;
    if (path[0] != '/') {
        cluster_id += "/";
    }
    cluster_id += path;
    return cluster_id;
}

ZkClusterFinder::ZkClusterFinder(const std::string& zk_root_path,
                                 const std::string& zk_addr_list)
    : zk_root_path_(zk_root_path), zk_addr_list_(zk_addr_list) {
}

static pthread_once_t zk_init_once = PTHREAD_ONCE_INIT;

static void InitZkLogOnce() {
    zk::ZooKeeperLightAdapter::SetLibraryLogOutput(FLAGS_tera_zk_lib_log_path);
}

bool ZkClusterFinder::ReadNode(const std::string& name, std::string* value) {
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
}

InsClusterFinder::InsClusterFinder(const std::string& ins_root_path,
                                   const std::string& ins_addr_list)
    : ins_root_path_(ins_root_path), ins_addr_list_(ins_addr_list) {
}

bool InsClusterFinder::ReadNode(const std::string& name, std::string* value) {
    galaxy::ins::sdk::InsSDK ins_sdk(ins_addr_list_);
    galaxy::ins::sdk::SDKError err;
    if (!ins_sdk.Get(ins_root_path_ + name, value, &err)) {
        LOG(ERROR) << "ins read " << name << " fail: " << err;
        return false;
    }
    return true;
}

FakeZkClusterFinder::FakeZkClusterFinder(const std::string& fake_zk_path_prefix)
    : fake_zk_path_prefix_(fake_zk_path_prefix) {
}

bool FakeZkClusterFinder::ReadNode(const std::string& name, std::string* value) {
    return zk::FakeZkUtil::ReadNode(fake_zk_path_prefix_ + name, value);
}

ClusterFinder* NewClusterFinder() {
    if (FLAGS_tera_ins_enabled) {
        return new sdk::InsClusterFinder(FLAGS_tera_ins_root_path, FLAGS_tera_ins_addr_list);
    } else if (FLAGS_tera_mock_ins_enabled) {
        return new sdk::MockInsClusterFinder(FLAGS_tera_ins_root_path, FLAGS_tera_ins_addr_list);
    } else if (!FLAGS_tera_zk_enabled) {
        return new sdk::FakeZkClusterFinder(FLAGS_tera_fake_zk_path_prefix);
    } else {
        return new sdk::ZkClusterFinder(FLAGS_tera_zk_root_path, FLAGS_tera_zk_addr_list);
    }
}

}  // namespace sdk
}  // namespace tera
