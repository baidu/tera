// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_IMPL_H_
#define TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_IMPL_H_

#include <glog/logging.h>
#include <memory>
#include <pthread.h>

#include "common/base/scoped_ptr.h"
#include "common/counter.h"
#include "common/mutex.h"
#include "observer/rowlocknode/fake_rowlocknode_zk_adapter.h"
#include "observer/rowlocknode/rowlock_db.h"
#include "observer/rowlocknode/rowlocknode_zk_adapter.h"
#include "proto/rowlocknode_rpc.pb.h"
#include "zk/zk_adapter.h"

namespace tera {
namespace observer {

class RowlockNodeImpl {
public:
    RowlockNodeImpl();
    ~RowlockNodeImpl();

    bool Init();

    bool Exit();

    void TryLock(const RowlockRequest* request,
            RowlockResponse* response,
            google::protobuf::Closure* done);

    void UnLock(const RowlockRequest* request,
            RowlockResponse* response,
            google::protobuf::Closure* done);

    void PrintQPS();
private:
    uint64_t GetRowlockKey(const std::string& table_name, const std::string& row) const;
private:
    ShardedRowlockDB rowlock_db_;
    std::unique_ptr<RowlockNodeZkAdapterBase> zk_adapter_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_IMPL_H_
