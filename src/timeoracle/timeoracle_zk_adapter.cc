// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "timeoracle/timeoracle_zk_adapter.h"
#include "common/file/file_path.h"
#include "common/this_thread.h"
#include "types.h"
#include "zk/zk_util.h"

DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_string(tera_fake_zk_path_prefix);
DECLARE_int32(tera_zk_timeout);
DECLARE_int64(tera_zk_retry_period);
DECLARE_int32(tera_zk_retry_max_times);

namespace tera {
namespace timeoracle {

void TimeoracleZkAdapterBase::OnNodeValueChanged(const std::string& path,
        const std::string& value) {
    LOG(INFO) << "zk OnNodeValueChanged, path=" << path;
}

void TimeoracleZkAdapterBase::OnChildrenChanged(const std::string& path,
        const std::vector<std::string>& name_list,
        const std::vector<std::string>& data_list) {
    LOG(INFO) << "zk OnChildrenChanged, path=" << path;
}

void TimeoracleZkAdapterBase::OnNodeCreated(const std::string& path) {
    LOG(INFO) << "zk OnNodeCreated, path=" << path;
}

void TimeoracleZkAdapterBase::OnNodeDeleted(const std::string& path) {
    LOG(INFO) << "zk OnNodeDeleted, path=" << path;
}

void TimeoracleZkAdapterBase::OnWatchFailed(const std::string& path, int watch_type,
                               int err) {
    LOG(INFO) << "zk OnWatchFailed, path=" << path;
}

void TimeoracleZkAdapterBase::OnSessionTimeout() {
    LOG(ERROR) << "zk session timeout!";
    _Exit(EXIT_FAILURE);
}

bool TimeoracleZkAdapter::Init(uint64_t* last_timestamp) {
    if (!InitZk()) {
        return false;
    }

    if (!LockTimeoracleLock()) {
        return false;
    }

    if (ReadTimestamp(last_timestamp)) {
        LOG(INFO) << "read timestamp sucess,get start_timestamp=" << *last_timestamp;
        return CreateTimeoracleNode();
    }

    return false;
}

bool TimeoracleZkAdapter::CreateTimeoracleNode() {
    LOG(INFO) << "try create timeoracle nod,path=" << kTimeoracleNodePath;
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!CreateEphemeralNode(kTimeoracleNodePath, server_addr_, &zk_errno)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to create timeoracle node";
            return false;
        }
        LOG(ERROR) << "retry create timeoracle node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "create timeoracle node success";
    return true;
}

bool TimeoracleZkAdapter::InitZk() {
    LOG(INFO) << "try to init zk,zk_addr_list=" << FLAGS_tera_zk_addr_list
        << ",zk_root_path=" << FLAGS_tera_zk_root_path;
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

bool TimeoracleZkAdapter::LockTimeoracleLock() {
    LOG(INFO) << "try to lock timeoracle lock,path=" << kTimeoracleLockPath;
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!SyncLock(kTimeoracleLockPath, &zk_errno, -1)) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to acquire timeoracle lock";
            return false;
        }
        LOG(ERROR) << "retry lock timeoracle lock in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    LOG(INFO) << "acquire timeoracle lock success";
    return true;
}

bool TimeoracleZkAdapter::ReadTimestamp(uint64_t* timestamp) {
    LOG(INFO) << "try to read timestamp, path=" << kTimeoracleTimestampPath;

    std::string timestamp_str;
    int32_t retry_count = 0;
    int zk_errno = zk::ZE_OK;
    while (!ReadNode(kTimeoracleTimestampPath, &timestamp_str, &zk_errno)
        && zk_errno != zk::ZE_NOT_EXIST) {
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(ERROR) << "fail to read timestamp node";
            return false;
        }
        LOG(ERROR) << "retry read timestamp node in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
    }
    if (zk_errno == zk::ZE_NOT_EXIST) {
        *timestamp = 0;
        return true;
    }

    char * pEnd = nullptr;
    *timestamp = ::strtoull(timestamp_str.c_str(), &pEnd, 10);
    if (*pEnd != '\0') {
        // TODO (chenzongjia)
        LOG(WARNING) << "read invalid timestamp value=" << timestamp_str;
        return false;
    }

    LOG(INFO) << "read timestamp value=" << timestamp_str;

    return true;
}

bool TimeoracleZkAdapter::UpdateTimestamp(uint64_t timestamp) {
    char timestamp_str[64];
    snprintf(timestamp_str, sizeof(timestamp_str), "%lu", timestamp);
    LOG(INFO) << "try to update timestamp to " << timestamp;
    int zk_errno = zk::ZE_OK;
    while (!WriteNode(kTimeoracleTimestampPath, timestamp_str, &zk_errno)
        && zk_errno != zk::ZE_NOT_EXIST) {
        return false;
        /*
        if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
            LOG(INFO) << "fail to update timestamp";
            return false;
        }
        LOG(ERROR) << "retry update timestamp in "
            << FLAGS_tera_zk_retry_period << " ms, retry=" << retry_count;
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
        zk_errno = zk::ZE_OK;
        */
    }
    if (zk_errno == zk::ZE_OK) {
        LOG(INFO) << "update zk path=" << kTimeoracleTimestampPath << " to "
            << timestamp_str << " success.";
        return true;
    }

    LOG(INFO) << "timestamp node not exist, try create timestamp node";
    zk_errno = zk::ZE_OK;
    while (!CreatePersistentNode(kTimeoracleTimestampPath, timestamp_str, &zk_errno)) {
        return false;
    }
    LOG(INFO) << "create timestamp node success";
    return true;

}

} // namespace timeoracle
} // namespace tera
