// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_MOCK_TABLET_MANAGER_H
#define TERA_TABLETNODE_MOCK_TABLET_MANAGER_H

#include "tera/tabletnode/tablet_manager.h"

#include "gmock/gmock.h"

namespace tera {
namespace tabletnode {

class MockTabletManager : public TabletManager {
public:
    MOCK_METHOD6(AddTablet,
        bool(const std::string& table_name,
             const std::string& table_path,
             const std::string& key_start,
             const std::string& key_end,
             io::TabletIO** tablet_io,
             StatusCode* status));
    MOCK_METHOD4(RemoveTablet,
        bool(const std::string& table_name,
             const std::string& key_start,
             const std::string& key_end,
             StatusCode* status));
    MOCK_METHOD4(GetTablet,
        io::TabletIO*(const std::string& table_name,
                      const std::string& key_start,
                      const std::string& key_end,
                      StatusCode* status));
    MOCK_METHOD3(GetTablet,
        io::TabletIO*(const std::string& table_name,
                      const std::string& key,
                      StatusCode* status));
    MOCK_METHOD1(GetAllTabletMeta,
        void(std::vector<TabletMeta*>* tablet_meta_list));
    MOCK_METHOD1(GetAllTablets,
        void(std::vector<io::TabletIO*>* taletio_list));
    MOCK_METHOD2(RemoveAllTablets,
        bool(bool force, StatusCode* status));
};

}  // namespace tabletnode
}  // namespace tera

#endif // TERA_TABLETNODE_MOCK_TABLET_MANAGER_H
