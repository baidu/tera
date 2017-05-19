// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TIMEORACLE_TIMEORACLE_ZK_ADAPTER_H
#define  TERA_TIMEORACLE_TIMEORACLE_ZK_ADAPTER_H

#include <string>
#include <vector>
#include "zk/zk_adapter.h"

namespace tera {
namespace timeoracle {

class TimeoracleZkAdapterBase: public zk::ZooKeeperAdapter {
public:
    virtual ~TimeoracleZkAdapterBase() {};

    // not thread safe
    virtual bool Init(uint64_t* last_timestamp) = 0;

    // not thread safe
    virtual bool UpdateTimestamp(uint64_t new_timestamp) = 0;

    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list) override;

    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) override;

    virtual void OnNodeCreated(const std::string& path) override;

    virtual void OnNodeDeleted(const std::string& path) override;

    virtual void OnWatchFailed(const std::string& path, int watch_type,
                               int err) override;

    virtual void OnSessionTimeout() final;
};

class TimeoracleZkAdapter : public TimeoracleZkAdapterBase {
public:
    TimeoracleZkAdapter(const std::string& server_addr) : server_addr_(server_addr) {
    }

    virtual bool Init(uint64_t* last_timestamp) override;

    virtual bool UpdateTimestamp(uint64_t new_timestamp) override;

private:
    bool InitZk();

    bool LockTimeoracleLock();

    bool ReadTimestamp(uint64_t* timestamp);

    bool CreateTimeoracleNode();

private:
    std::string     server_addr_;
};

/*
 * This is not zookeeper!
 * Just used on onebox for tasting tera briefly.
 * This is implemented through local file system.
 * Not support watching.
 */
class FakeTimeoracleZkAdapter: public TimeoracleZkAdapterBase {
public:
    FakeTimeoracleZkAdapter(const std::string& server_addr) : server_addr_(server_addr) {
    }

    // not thread safe
    virtual bool Init(uint64_t* last_timestamp) override {
        *last_timestamp = 0;
        return true;
    }

    // not thread safe
    virtual bool UpdateTimestamp(uint64_t new_timestamp) override {
        return true;
    }

private:
    std::string     server_addr_;
};

} // namespace timeoracle
} // namespace tera

#endif // TERA_TIMEORACLE_TIMEORACLE_ZK_ADAPTER_H
