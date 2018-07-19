// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlockproxy/rowlock_proxy_impl.h"

#include <functional>

#include "common/timer.h"
#include "utils/utils_cmd.h"

DECLARE_string(rowlock_proxy_port);
DECLARE_string(tera_coord_type);
DECLARE_bool(rowlock_proxy_async_enable);
DECLARE_int32(observer_rowlock_client_thread_num);

namespace tera {
namespace observer {

RowlockProxyImpl::RowlockProxyImpl()
    : server_addrs_(new std::vector<std::string>),
      server_number_(1) {}

RowlockProxyImpl::~RowlockProxyImpl() {
}

bool RowlockProxyImpl::Init() {
    if (FLAGS_tera_coord_type == "zk") {
        zk_adapter_.reset(new RowlockProxyZkAdapter(this,
                                                    tera::utils::GetLocalHostName() + ":" + FLAGS_rowlock_proxy_port));
    } else {
        zk_adapter_.reset(new InsRowlockProxyZkAdapter(this,
                                                       tera::utils::GetLocalHostName() + ":" + FLAGS_rowlock_proxy_port));
    }

    if (!zk_adapter_->Init()) {
        LOG(ERROR) << "init zk adapter fail";
        return false;
    }

    LOG(INFO) << "Rowlock node init finish";
    return true;
}

void RowlockProxyImpl::TryLock(const RowlockRequest* request,
                               RowlockResponse* response,
                               google::protobuf::Closure* done) {

    uint64_t rowlock_key = GetRowKey(request->table_name(), request->row());
    std::string addr = ScheduleRowKey(rowlock_key);

    RowlockStub client(addr);
    client.TryLock(request, response);
    VLOG(12) << "lock row: " << rowlock_key;
    done->Run();
}

void RowlockProxyImpl::UnLock(const RowlockRequest* request,
                              RowlockResponse* response,
                              google::protobuf::Closure* done) {

    uint64_t rowlock_key = GetRowKey(request->table_name(), request->row());
    std::string addr = ScheduleRowKey(rowlock_key);

    RowlockStub client(addr);
    client.UnLock(request, response);
    VLOG(12) << "unlock row: " << rowlock_key;
    done->Run();

}

uint64_t RowlockProxyImpl::GetRowKey(const std::string& table_name,
                                     const std::string& row) const {
    std::string rowkey_str = table_name + row;
    return std::hash<std::string>()(rowkey_str);
}

std::string RowlockProxyImpl::ScheduleRowKey(uint64_t row_key) {
    std::shared_ptr<std::vector<std::string>> server_addrs_copy;

    MutexLock locker(&server_addrs_mutex_);
    // copy for copy-on-write， ref +1
    server_addrs_copy = server_addrs_;

    return (*server_addrs_copy)[row_key % server_number_];
}

void RowlockProxyImpl::SetServerNumber(uint32_t number) {
    MutexLock locker(&server_addrs_mutex_);

    server_number_ = number;

    if (server_addrs_->size() < number) {
        server_addrs_->resize(number);
    }
}

void RowlockProxyImpl::UpdateServers(uint32_t id, const std::string& addr) {
    // update data first
    {
        MutexLock locker(&server_addrs_mutex_);
        (*server_addrs_)[id] = addr;
    }
}

uint32_t RowlockProxyImpl::GetServerNumber() {
    return server_number_;
}

} // namespace observer
} // namespace tera



