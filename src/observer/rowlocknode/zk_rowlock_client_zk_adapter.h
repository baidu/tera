// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_ZK_ROWLOCK_CLIENT_ZK_ADAPTER_H_
#define TERA_OBSERVER_ROWLOCKNODE_ZK_ROWLOCK_CLIENT_ZK_ADAPTER_H_

#include "zk/zk_adapter.h"

namespace tera {
namespace observer {

class RowlockClient;

class ZkRowlockClientZkAdapter : public zk::ZooKeeperLightAdapter {
public:
    ZkRowlockClientZkAdapter(RowlockClient* server_client, const std::string& server_addr);
    virtual ~ZkRowlockClientZkAdapter();
    virtual bool Init();

private:
    RowlockClient* client_;
    std::string server_addr_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKNODE_ZK_ROWLOCK_CLIENT_ZK_ADAPTER_H_
