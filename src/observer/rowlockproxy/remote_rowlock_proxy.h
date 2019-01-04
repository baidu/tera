// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKPROXY_REMOTE_ROWLOCK_PROXY_H_
#define TERA_OBSERVER_ROWLOCKPROXY_REMOTE_ROWLOCK_PROXY_H_

#include <memory>

#include "common/base/scoped_ptr.h"
#include "common/thread_pool.h"
#include "observer/rowlockproxy/rowlock_proxy_impl.h"

namespace tera {
namespace observer {

class RemoteRowlockProxy : public RowlockService {
 public:
  explicit RemoteRowlockProxy(RowlockProxyImpl* rowlock_proxy_impl);
  ~RemoteRowlockProxy();

  void Lock(google::protobuf::RpcController* controller, const RowlockRequest* request,
            RowlockResponse* response, google::protobuf::Closure* done);

  void UnLock(google::protobuf::RpcController* controller, const RowlockRequest* request,
              RowlockResponse* response, google::protobuf::Closure* done);

 private:
  RowlockProxyImpl* rowlock_proxy_impl_;
};

}  // namespace observer
}  // namespace tera
#endif  // TERA_OBSERVER_ROWLOCKPROXY_REMOTE_ROWLOCK_PROXY_H_
