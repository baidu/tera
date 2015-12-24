// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_zk.h"

#include <iostream>
#include <gflags/gflags.h>

#include "types.h"
#include "zk/zk_adapter.h"

DECLARE_string(tera_zk_lib_log_path);
DECLARE_string(tera_fake_zk_path_prefix);
DECLARE_bool(tera_zk_enabled);
DECLARE_bool(tera_ins_enabled);
DECLARE_string(tera_ins_root_path);

namespace tera {
namespace sdk {

pthread_once_t ClusterFinder::_zk_init_once = PTHREAD_ONCE_INIT;

ClusterFinder::ClusterFinder(std::string zk_root_path, const std::string& zk_addr_list)
    : _zk_root_path(zk_root_path),
      _zk_addr_list(zk_addr_list) {
}

void ClusterFinder::InitZkLogOnce() {
    zk::ZooKeeperLightAdapter::SetLibraryLogOutput(FLAGS_tera_zk_lib_log_path);
}

bool ClusterFinder::ReadZkNode(const std::string path, std::string* value) {
    pthread_once(&_zk_init_once, InitZkLogOnce);

    int zk_errno = tera::zk::ZE_OK;

    zk::ZooKeeperLightAdapter zk_adapter;
    if (!zk_adapter.Init(_zk_addr_list, _zk_root_path,
                         1000*15, "", &zk_errno)) {
        std::cout << "Init zookeeper fail: "
            << tera::zk::ZkErrnoToString(zk_errno) << std::endl;
        return false;
    }

    if (!zk_adapter.ReadNode(path, value, &zk_errno)) {
        std::cout<< "ReadZkNode fail: "<< zk::ZkErrnoToString(zk_errno) << std::endl;
        return false;
    }
    return true;
}

std::string ClusterFinder::MasterAddr(bool update) {
    std::string master_addr;
    if (FLAGS_tera_ins_enabled) {
        // ins
        std::string master_node =
            FLAGS_tera_ins_root_path + kMasterNodePath;
        if (update || _master_addr == "") {
            if (!zk::InsUtil::ReadNode(master_node, &master_addr)) {
                LOG(WARNING) << "fail to read ins master node: " << master_node;
                master_addr = "";
            }
        }
    } else if (!FLAGS_tera_zk_enabled) {
        // use local file system as a fake zk
        std::string master_node =
            FLAGS_tera_fake_zk_path_prefix + kMasterLockPath + "/0";
        if (!zk::FakeZkUtil::ReadNode(master_node, &master_addr)) {
            LOG(FATAL) << "fail to read fake master node: " << master_node;
        }
    } else if (update || _master_addr == "") {
        // zookeeper
        if (!ReadZkNode(kMasterNodePath, &master_addr)) {
            master_addr = "";
        }
    }
    if (!master_addr.empty()) {
        MutexLock lock(&_mutex);
        _master_addr = master_addr;
    }
    return _master_addr;
}

std::string ClusterFinder::RootTableAddr(bool update) {
    std::string root_table_addr;
    if (FLAGS_tera_ins_enabled) {
        std::string root_node = FLAGS_tera_ins_root_path + kRootTabletNodePath;
        if (update || _root_table_addr == "") {
            if (!zk::InsUtil::ReadNode(root_node, &root_table_addr)) {
                root_table_addr = "";
                LOG(WARNING) << "fail to read ins meta node: " << root_node;
            }
        }
    } else if (!FLAGS_tera_zk_enabled) {
        // use local file system as a fake zk
        std::string root_node = FLAGS_tera_fake_zk_path_prefix + kRootTabletNodePath;
        if (!zk::FakeZkUtil::ReadNode(root_node, &root_table_addr)) {
            LOG(FATAL) << "fail to read fake master node: " << root_node;
        }
    } else if (update || _root_table_addr == "") {
        if (!ReadZkNode(kRootTabletNodePath, &root_table_addr)) {
            root_table_addr = "";
        }
    }
    if (!root_table_addr.empty()) {
        MutexLock lock(&_mutex);
        _root_table_addr = root_table_addr;
    }
    return _root_table_addr;
}

}  // namespace sdk
}  // namespace tera
