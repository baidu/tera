// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_INS_ROWLOCK_CLIENT_ZK_ADAPTER_H_
#define TERA_OBSERVER_ROWLOCKNODE_INS_ROWLOCK_CLIENT_ZK_ADAPTER_H_

#include "observer/rowlocknode/zk_rowlock_client_zk_adapter.h"
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

class RowlockClient;

class InsRowlockClientZkAdapter : public ZkRowlockClientZkAdapter {
public:
    InsRowlockClientZkAdapter(RowlockClient* server_client, const std::string& server_addr);
    virtual ~InsRowlockClientZkAdapter() {};
    virtual bool Init();
protected:
	virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) {}
    virtual void OnWatchFailed(const std::string& path, int watch_type,
                               int err) {}
    virtual void OnNodeDeleted(const std::string& path) {}
    virtual void OnSessionTimeout() {}
    virtual void OnNodeCreated(const std::string& path) {}
    virtual void OnChildrenChanged(const std::string& path,
		                           const std::vector<std::string>& name_list,
		                           const std::vector<std::string>& data_list) {}

private:
    RowlockClient* client_;
    std::string server_addr_;
    galaxy::ins::sdk::InsSDK* ins_sdk_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKNODE_INS_ROWLOCK_CLIENT_ZK_ADAPTER_H_
