// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlocknode/rowlocknode_zk_adapter.h"

#include <gflags/gflags.h>

#include "common/this_thread.h"
#include "ins_sdk.h"
#include "types.h"

DECLARE_string(rowlock_zk_root_path);
DECLARE_string(tera_zk_addr_list);
DECLARE_int32(rowlock_server_node_num);
DECLARE_int64(tera_zk_retry_period); 
DECLARE_int32(tera_zk_timeout);
DECLARE_int32(tera_zk_retry_max_times);

namespace tera {
namespace observer {

RowlockNodeZkAdapter::RowlockNodeZkAdapter(RowlockNodeImpl* rowlocknode_impl,
        const std::string& server_addr) :
    rowlocknode_impl_(rowlocknode_impl), server_addr_(server_addr) {
}

RowlockNodeZkAdapter::~RowlockNodeZkAdapter() {
}

void RowlockNodeZkAdapter::Init() {
    std::string root_path = FLAGS_rowlock_zk_root_path;
    std::string node_num_key = root_path + kRowlockNodeNumPath;

    int zk_errno = zk::ZE_OK;;
    // init zk client
    while (!ZooKeeperAdapter::Init(FLAGS_tera_zk_addr_list,
                                   FLAGS_rowlock_zk_root_path, FLAGS_tera_zk_timeout,
                                   server_addr_, &zk_errno)) {
        LOG(ERROR) << "fail to init zk : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "init zk success";
    
    // get session id
    int64_t session_id_int = 0;
    if (!GetSessionId(&session_id_int, &zk_errno)) {
        LOG(ERROR) << "get session id fail : " << zk::ZkErrnoToString(zk_errno);
        return;
    }

    // put server_node_num
    zk_errno = zk::ZE_OK;
    bool is_exist = true;
    int32_t retry_count = 0;
    std::string value = std::to_string(FLAGS_rowlock_server_node_num);
    CheckExist(node_num_key, &is_exist, &zk_errno);
    if (!is_exist) {
        while (!CreateEphemeralNode(node_num_key, value, &zk_errno)) {
            if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
                LOG(ERROR) << "fail to create master node";
                return;
            }
            LOG(ERROR) << "retry create rowlock number node in "
                       << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
            ThisThread::Sleep(FLAGS_tera_zk_retry_period);
            zk_errno = zk::ZE_OK;
        }
    } else {
        WriteNode(node_num_key, value, &zk_errno);
        zk_errno = zk::ZE_OK;
    }

    value = server_addr_;

    // create node
    int id = 0;
    std::string id_lock_key;
    std::string host_lock_key;

    while (true) {
        id_lock_key = root_path + kRowlockNodeIdListPath + "/" + std::to_string(id);
        zk_errno = zk::ZE_OK;

        if (!CreateEphemeralNode(id_lock_key, server_addr_, &zk_errno)) {
            LOG(ERROR) << "create rowlock node fail: " << id_lock_key;
        } else {
            break;
        }
        LOG(ERROR) << "fail to create serve-node : " << zk::ZkErrnoToString(zk_errno);

        if (++id >= FLAGS_rowlock_server_node_num) {
            id = 0;
        }
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "create serve-node success";

    is_exist = false;

    // watch my node
    while (!CheckAndWatchExist(id_lock_key, &is_exist, &zk_errno)) {
        LOG(ERROR) << "fail to watch serve-node : " << zk::ZkErrnoToString(zk_errno);
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
    LOG(INFO) << "watch rowlock-node success";

    if (!is_exist) {
        OnLockChange();
    }
}

void RowlockNodeZkAdapter::OnLockChange() {
    _Exit(EXIT_FAILURE);
}

} // namespace observer
} // namespace tera

