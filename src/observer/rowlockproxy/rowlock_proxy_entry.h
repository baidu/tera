// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_ENTRY_H_
#define TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_ENTRY_H_

#include <memory>

#include <sofa/pbrpc/pbrpc.h>

#include "observer/rowlockproxy/remote_rowlock_proxy.h"
#include "observer/rowlockproxy/rowlock_proxy_impl.h"
#include "tera/tera_entry.h"

namespace tera {
namespace observer {

class RowlockProxyEntry : public tera::TeraEntry {
 public:
  RowlockProxyEntry();
  virtual ~RowlockProxyEntry();

  virtual bool StartServer();
  virtual bool Run();
  virtual void ShutdownServer();

 private:
  std::unique_ptr<RowlockProxyImpl> rowlock_proxy_impl_;
  RemoteRowlockProxy* remote_rowlock_proxy_;
  std::unique_ptr<sofa::pbrpc::RpcServer> rpc_server_;
};

}  // namespace observer
}  // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_ENTRY_H_
