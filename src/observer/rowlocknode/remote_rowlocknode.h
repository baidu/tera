// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_REMOTE_ROWLOCKNODE_H_
#define TERA_OBSERVER_ROWLOCKNODE_REMOTE_ROWLOCKNODE_H_

#include "common/base/scoped_ptr.h"
#include "common/thread_pool.h"
#include "observer/rowlocknode/rowlocknode_impl.h"

namespace tera {
namespace observer {

class RemoteRowlockNode : public RowlockService {
public:
    explicit RemoteRowlockNode(RowlockNodeImpl* rowlocknode_impl);
    ~RemoteRowlockNode();

    void Lock(google::protobuf::RpcController* controller,
            const RowlockRequest* request,
            RowlockResponse* response,
            google::protobuf::Closure* done);

    void UnLock(google::protobuf::RpcController* controller,
            const RowlockRequest* request,
            RowlockResponse* response,
            google::protobuf::Closure* done);

private:
    RowlockNodeImpl* rowlocknode_impl_;
};

} // namespace observer
} // namespace tera
#endif  // TERA_OBSERVER_ROWLOCKNODE_REMOTE_ROWLOCKNODE_H_

