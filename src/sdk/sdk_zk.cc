// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_zk.h"

#include <iostream>
#include "types.h"
#include "zk/zk_adapter.h"
#include "gflags/gflags.h"

DECLARE_string(tera_zk_lib_log_path);

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
    if (update || _master_addr == "") {
        if (!ReadZkNode(kMasterNodePath, &_master_addr)) {
            _master_addr = "";
        }
    }
    return _master_addr;
}

std::string ClusterFinder::RootTableAddr(bool update) {
    if (update || _root_table_addr == "") {
        if (!ReadZkNode(kRootTabletNodePath, &_root_table_addr)) {
            _root_table_addr = "";
        }
    }
    return _root_table_addr;
}

}
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
