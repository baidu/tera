// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <unistd.h>
#include <sys/file.h>
#include "timeoracle/timeoracle_zk_adapter.h"
#include "common/file/file_path.h"
#include "common/this_thread.h"
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
DECLARE_string(tera_timeoracle_mock_root_path);

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
    Finalize();
    _Exit(EXIT_FAILURE);
}

void TimeoracleZkAdapterBase::OnWatchFailed(const std::string& path, int watch_type,
                               int err) {
    LOG(INFO) << "zk OnWatchFailed, path=" << path;
    Finalize();
    _Exit(EXIT_FAILURE);
}

void TimeoracleZkAdapterBase::OnSessionTimeout() {
    LOG(ERROR) << "zk session timeout!";
    _Exit(EXIT_FAILURE);
}

TimeoracleZkAdapter::~TimeoracleZkAdapter() {
}

bool TimeoracleZkAdapter::Init(int64_t* last_timestamp) {
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

bool TimeoracleZkAdapter::ReadTimestamp(int64_t* timestamp) {
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

bool TimeoracleZkAdapter::UpdateTimestamp(int64_t timestamp) {
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

TimeoracleInsAdapter::~TimeoracleInsAdapter() {
    if (ins_sdk_) {
        std::string lock_path = FLAGS_tera_ins_root_path + kTimeoracleLockPath;
        galaxy::ins::sdk::SDKError err;
        ins_sdk_->UnLock(lock_path, &err);
    }
}

bool TimeoracleInsAdapter::Init(int64_t* last_timestamp) {
    if (!InitInsAndLock()) {
        return false;
    }

    if (ReadTimestamp(last_timestamp)) {
        LOG(INFO) << "read timestamp sucess,get start_timestamp=" << *last_timestamp;
        return CreateTimeoracleNode();
    }

    return false;
}

bool TimeoracleInsAdapter::CreateTimeoracleNode() {
    std::string put_path = FLAGS_tera_ins_root_path + kTimeoracleNodePath;

    LOG(INFO) << "try write timeoracle nod,path=" << put_path;

    galaxy::ins::sdk::SDKError err;

    if (!ins_sdk_->Put(put_path, server_addr_, &err)) {
        LOG(ERROR) << "update timestamp node, path=" << put_path << ",failed "
            << ins_sdk_->ErrorToString(err);
        return false;
    }

    LOG(INFO) << "update timeoracle node success";
    return true;
}

static void InsOnSessionTimeout(void * context) {
    TimeoracleInsAdapter* ins_adp = static_cast<TimeoracleInsAdapter*>(context);
    ins_adp->OnSessionTimeout();
}

static void InsOnLockChange(const galaxy::ins::sdk::WatchParam& param,
                            galaxy::ins::sdk::SDKError error) {
    TimeoracleInsAdapter* ins_adp = static_cast<TimeoracleInsAdapter*>(param.context);
    ins_adp->OnLockChange(param.value, param.deleted);
}

bool TimeoracleInsAdapter::InitInsAndLock() {
    MutexLock lock(&mutex_);
    LOG(INFO) << "try to init ins,ins_addr_list=" << FLAGS_tera_ins_addr_list
        << ",ins_root_path=" << FLAGS_tera_ins_root_path;
    ins_sdk_ = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);
    ins_sdk_->SetTimeoutTime(FLAGS_tera_master_ins_session_timeout);

    std::string lock_path = FLAGS_tera_ins_root_path + kTimeoracleLockPath;

    galaxy::ins::sdk::SDKError err;

    ins_sdk_->RegisterSessionTimeout(InsOnSessionTimeout, this);

    if (!ins_sdk_->Lock(lock_path, &err)) {
        LOG(ERROR) << "try to lock timeoracle lock,path=" << kTimeoracleLockPath << " failed,"
                << ins_sdk_->ErrorToString(err);
        return false;
    }

    LOG(INFO) << "try to lock timeoracle lock,path=" << kTimeoracleLockPath << " success";

    if (!ins_sdk_->Watch(lock_path, InsOnLockChange, this, &err)) {
        LOG(ERROR) << "try to watch timeoracle lock,path=" << kTimeoracleLockPath << " failed,"
                << ins_sdk_->ErrorToString(err);
        return false;
    }

    LOG(INFO) << "try to watch timeoracle lock,path=" << kTimeoracleLockPath << " success";

    return true;
}

bool TimeoracleInsAdapter::ReadTimestamp(int64_t* timestamp) {
    std::string read_path = FLAGS_tera_ins_root_path + kTimeoracleTimestampPath;

    LOG(INFO) << "try to read timestamp, path=" << read_path;

    std::string timestamp_str;
    galaxy::ins::sdk::SDKError err;

    if (!ins_sdk_->Get(read_path, &timestamp_str, &err)) {
        if (err == galaxy::ins::sdk::SDKError::kNoSuchKey) {
            *timestamp = 0;
            return true;
        }

        LOG(ERROR) << "try to read timestamp, path=" << read_path << ",failed "
            << ins_sdk_->ErrorToString(err);
        return false;
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

bool TimeoracleInsAdapter::UpdateTimestamp(int64_t timestamp) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%lu", timestamp);
    LOG(INFO) << "try to update timestamp to " << timestamp;

    std::string timestamp_str(buf);
    galaxy::ins::sdk::SDKError err;
    std::string put_path = FLAGS_tera_ins_root_path + kTimeoracleTimestampPath;

    if (!ins_sdk_->Put(put_path, timestamp_str, &err)) {
        LOG(ERROR) << "update timestamp, path=" << put_path << ",failed "
            << ins_sdk_->ErrorToString(err);
        return false;
    }

    return true;
}

void TimeoracleInsAdapter::OnLockChange(std::string session_id, bool deleted) {
    if (deleted || session_id != ins_sdk_->GetSessionID()) {
        LOG(ERROR) << "timeoracle lock losted";
        exit(1);
    }
}

class FdGuard {
public:
    explicit FdGuard(int fd) : fd_(fd) {}

    FdGuard() : fd_(-1) {}

    ~FdGuard() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    operator int() const {
        return fd_;
    }

    void reset(int fd) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = fd;
    }

    int relese() {
        const int ret = fd_;
        fd_ = -1;
        return ret;
    }

private:
    FdGuard(const FdGuard&) = delete;
    void operator=(const FdGuard&) = delete;
    int fd_;
};

// not thread safe
bool TimeoracleMockAdapter::Init(int64_t* last_timestamp) {
    std::string lock_path = FLAGS_tera_timeoracle_mock_root_path + kTimeoracleLockPath;
    static FdGuard lock_fd(::open(lock_path.c_str(), O_CREAT | O_RDWR, 0666));

    if (lock_fd < 0) {
        return false;
    }

    LOG(INFO) << "TimeoracleMockAdapter try to get lock for file=" << lock_path;

    if (::flock(lock_fd, LOCK_EX) < 0) {
        LOG(WARNING) << "lock file failed for path=" << lock_path;
        return false;
    }

    LOG(INFO) << "TimeoracleMockAdapter got the lock for file=" << lock_path;

    std::string get_path = FLAGS_tera_timeoracle_mock_root_path + kTimeoracleTimestampPath;

    FdGuard tmp_fd(::open(get_path.c_str(), O_CREAT | O_RDWR, 0666));

    if (tmp_fd < 0) {
        LOG(WARNING) << "open file failed for file=" << get_path;
        return false;
    }

    char buf[64];

    ssize_t len = pread(tmp_fd, buf, sizeof(buf), 0);
    if (len < 0) {
        LOG(WARNING) << "read file failed for file=" << get_path;
        return false;
    }

    if (len == 0) {
        *last_timestamp = 0;
        return true;
    }

    buf[len] = '\0';
    char * pEnd = nullptr;
    *last_timestamp = ::strtoull(buf, &pEnd, 10);
    if (*pEnd != '\0') {
        // TODO (chenzongjia)
        LOG(WARNING) << "read invalid timestamp value=" << buf;
        return false;
    }

    LOG(INFO) << "read timestamp value=" << *last_timestamp;

    std::string put_path = FLAGS_tera_timeoracle_mock_root_path + kTimeoracleNodePath;

    tmp_fd.reset(::open(put_path.c_str(), O_CREAT | O_RDWR, 0666));

    if (tmp_fd < 0) {
        LOG(WARNING) << "open file failed for file=" << put_path;
        return false;
    }

    if (::pwrite(tmp_fd, server_addr_.data(), server_addr_.size(), 0)
            != (ssize_t)server_addr_.size()) {
        LOG(WARNING) << "write file failed for file=" << put_path;
        return false;
    }

    return true;
}

// not thread safe
bool TimeoracleMockAdapter::UpdateTimestamp(int64_t new_timestamp) {
    std::string put_path = FLAGS_tera_timeoracle_mock_root_path + kTimeoracleTimestampPath;
    FdGuard tmp_fd(::open(put_path.c_str(), O_CREAT | O_RDWR, 0666));

    if (tmp_fd < 0) {
        LOG(WARNING) << "open file failed for file=" << put_path;
        return false;
    }

    char buf[64];
    snprintf(buf, sizeof(buf), "%lu", new_timestamp);
    std::string timestamp_str(buf);
    LOG(INFO) << "try to update timestamp to " << put_path;

    if (::pwrite(tmp_fd, timestamp_str.data(), timestamp_str.size(), 0)
            != (ssize_t)timestamp_str.size()) {
        LOG(WARNING) << "write file failed for file=" << put_path;
        return false;
    }

    return true;
}

} // namespace timeoracle
} // namespace tera
