// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
 
#include "observer/rowlockproxy/remote_rowlock_proxy.h"

#include "gflags/gflags.h"

DECLARE_int32(rowlock_thread_max_num);

namespace tera {
namespace observer {

RemoteRowlockProxy::RemoteRowlockProxy(RowlockProxyImpl* rowlock_proxy_impl) : 
    rowlock_proxy_impl_(rowlock_proxy_impl) {
}

RemoteRowlockProxy::~RemoteRowlockProxy() {
}

void RemoteRowlockProxy::Lock(google::protobuf::RpcController* controller,
                                const RowlockRequest* request,
                                RowlockResponse* response,
                                google::protobuf::Closure* done) {
    rowlock_proxy_impl_->TryLock(request, response, done);
}

void RemoteRowlockProxy::UnLock(google::protobuf::RpcController* controller,
                               const RowlockRequest* request,
                               RowlockResponse* response,
                               google::protobuf::Closure* done) { 
    rowlock_proxy_impl_->UnLock(request, response, done);
}

} // namespace observer
} // namespace tera
