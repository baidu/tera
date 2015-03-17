// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_MOCK_ZK_ADAPTER_H
#define TERA_TABLETNODE_MOCK_ZK_ADAPTER_H

#include "zk/dump_zk_adapter.h"

namespace tera {
namespace tabletnode {

class MockDummyNodeZkAdapter : public zk::DummyNodeZkAdapter {
public:
    MOCK_METHOD3(OnChildrenChanged,
        void(const std::string& path,
             const std::vector<std::string>& name_list,
             const std::vector<std::string>& data_list));
    MOCK_METHOD2(OnNodeValueChanged,
        void(const std::string& path,
             const std::string& value));
    MOCK_METHOD1(OnNodeCreated,
        void(const std::string& path));
    MOCK_METHOD1(OnNodeDeleted,
        void(const std::string& path));
    MOCK_METHOD3(OnWatchFailed,
        void(const std::string& path,
             int watch_type,
             int err));
    MOCK_METHOD1(OnSessionTimeout,
        void());
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_MOCK_ZK_ADAPTER_H
