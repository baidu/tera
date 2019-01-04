// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
#ifndef TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_ZK_ADAPTER_BASE_H_
#define TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_ZK_ADAPTER_BASE_H_

#include "zk/zk_adapter.h"

namespace tera {
namespace observer {

class RowlockNodeZkAdapterBase : public tera::zk::ZooKeeperAdapter {
 public:
  virtual ~RowlockNodeZkAdapterBase() {}
  virtual void Init() = 0;
};

}  // namespace observer
}  // namespace tera
#endif  // TERA_OBSERVER_ROWLOCKNODE_ROWLOCKNODE_ZK_ADAPTER_BASE_H_
