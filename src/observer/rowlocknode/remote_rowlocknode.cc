// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlocknode/remote_rowlocknode.h"

#include "gflags/gflags.h"

DECLARE_int32(rowlock_thread_max_num);

namespace tera {
namespace observer {

RemoteRowlockNode::RemoteRowlockNode(RowlockNodeImpl* rowlocknode_impl) :
    rowlocknode_impl_(rowlocknode_impl) {
}

RemoteRowlockNode::~RemoteRowlockNode() {
}

void RemoteRowlockNode::Lock(google::protobuf::RpcController* controller,
                                const RowlockRequest* request,
                                RowlockResponse* response,
                                google::protobuf::Closure* done) {
    rowlocknode_impl_->TryLock(request, response, done);
}

void RemoteRowlockNode::UnLock(google::protobuf::RpcController* controller,
                               const RowlockRequest* request,
                               RowlockResponse* response,
                               google::protobuf::Closure* done) {
    rowlocknode_impl_->UnLock(request, response, done);
}

} // namespace observer
} // namespace tera
