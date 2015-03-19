// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_ZK_DUMMY_ZK_ADAPTER_H_
#define TERA_ZK_DUMMY_ZK_ADAPTER_H_

#include "zk/zk_adapter.h"

namespace tera {
namespace zk {

class DummyNodeZkAdapter: public ZooKeeperAdapter {
public:
    virtual ~DummyNodeZkAdapter() {}

protected:
    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list) {}
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) {}
    virtual void OnNodeCreated(const std::string& path) {}
    virtual void OnNodeDeleted(const std::string& path) {}
    virtual void OnWatchFailed(const std::string& path, int watch_type, int err) {}
    virtual void OnSessionTimeout() {}
};

} // namespace zk
} // namespace tera

#endif // TERA_ZK_DUMMY_ZK_ADAPTER_H_
