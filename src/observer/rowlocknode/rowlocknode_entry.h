// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_ENTRY_H_
#define TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_ENTRY_H_

#include <sofa/pbrpc/pbrpc.h>

#include "common/base/scoped_ptr.h"
#include "observer/rowlocknode/remote_rowlocknode.h"
#include "observer/rowlocknode/rowlocknode_impl.h"
#include "tera/tera_entry.h"

namespace tera {
namespace observer {

class RowlockNodeEntry : public tera::TeraEntry {
 public:
  RowlockNodeEntry();
  virtual ~RowlockNodeEntry();

  virtual bool StartServer();
  virtual bool Run();
  virtual void ShutdownServer();
  void SetProcessorAffinity();

 private:
  common::Mutex mutex_;

  scoped_ptr<RowlockNodeImpl> rowlocknode_impl_;
  RemoteRowlockNode* remote_rowlocknode_;
  scoped_ptr<sofa::pbrpc::RpcServer> rpc_server_;
};

}  // namespace observer
}  // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_ENTRY_H_
