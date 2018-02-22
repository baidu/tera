// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_FAKE_ROWLOCKNODE_ZK_ADAPTER_H_
#define TERA_OBSERVER_ROWLOCKNODE_FAKE_ROWLOCKNODE_ZK_ADAPTER_H_

#include <string>
#include <vector>

#include "observer/rowlocknode/rowlocknode_impl.h"
#include "observer/rowlocknode/rowlocknode_zk_adapter_base.h"
#include "zk/zk_adapter.h"

namespace galaxy {
namespace ins {
namespace sdk {
    class InsSDK;
} // namespace sdk
} // namespace ins
} // namespace galaxy

namespace tera {
namespace observer {

class RowlockNodeImpl;

class FakeRowlockNodeZkAdapter : public RowlockNodeZkAdapterBase {
public:
    FakeRowlockNodeZkAdapter(RowlockNodeImpl* rowlocknode_impl, const std::string& server_addr);
    virtual ~FakeRowlockNodeZkAdapter();
    virtual void Init();
    void OnLockChange(std::string session_id, bool deleted);

private:
    virtual void OnChildrenChanged(const std::string& path,
            const std::vector<std::string>& name_list,
            const std::vector<std::string>& data_list) {}
    virtual void OnNodeValueChanged(const std::string& path,
            const std::string& value) {}
    virtual void OnNodeCreated(const std::string& path) {}
    virtual void OnNodeDeleted(const std::string& path) {}
    virtual void OnWatchFailed(const std::string& path, int watch_type,
            int err) {}
    virtual void OnSessionTimeout() {}

private:
    RowlockNodeImpl* rowlocknode_impl_;
    std::string server_addr_;
};

} // namespace observer
} // namespace tera
#endif  // TERA_OBSERVER_ROWLOCKNODE_FAKE_ROWLOCKNODE_ZK_ADAPTER_H_

