// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_IMPL_H_
#define TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_IMPL_H_

#include <glog/logging.h>
#include <memory>
#include <pthread.h>

#include "common/counter.h"
#include "common/mutex.h"
#include "observer/rowlockproxy/rowlock_proxy_zk_adapter.h"
#include "proto/rowlocknode_rpc.pb.h"
#include "sdk/rowlock_client.h"
#include "zk/zk_adapter.h"

namespace tera {
namespace observer {

class RowlockProxyZkAdapterBase;
class RowLockStub;

class RowlockProxyImpl {
public:
    RowlockProxyImpl();
    ~RowlockProxyImpl();

    bool Init();

    void TryLock(const RowlockRequest* request,
                 RowlockResponse* response,
                 google::protobuf::Closure* done);

    void UnLock(const RowlockRequest* request,
                RowlockResponse* response,
                google::protobuf::Closure* done);

    // for zk
    void SetServerNumber(uint32_t number);
    uint32_t GetServerNumber();
    void UpdateServers(uint32_t id, const std::string& addr);
private:
    uint64_t GetRowKey(const std::string& table_name,
                       const std::string& row) const;
    // rowkey -> server addr
    std::string ScheduleRowKey(uint64_t row_key);
    void ProxyCallBack(google::protobuf::Closure* done,
                       const RowlockRequest* request,
                       RowlockResponse* response,
                       bool failed,
                       int error_code);

private:
    common::Mutex server_addrs_mutex_;
    // a map from virtual node to server addr
    // key: vector index, virtual node number
    // value: vector value, server address
    // shared_ptr: used for copy-on-write
    std::shared_ptr<std::vector<std::string>> server_addrs_;

    uint32_t server_number_;
    std::unique_ptr<RowlockProxyZkAdapterBase> zk_adapter_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_IMPL_H_
