// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SDK_ZK_H_
#define  TERA_SDK_SDK_ZK_H_

#include <pthread.h>
#include <string>
#include <common/mutex.h>

namespace tera {
namespace zk {
    class ZooKeeperLightAdapter;
}

namespace sdk {
class ClusterFinder
{
public:
    ClusterFinder(std::string zk_root_path, const std::string& zk_addr_list);
    std::string MasterAddr(bool update = false);
    std::string RootTableAddr(bool update = false);
private:
    static void InitZkLogOnce();

    bool ReadZkNode(const std::string path, std::string* value);

    mutable Mutex _mutex;

    std::string _master_addr;
    std::string _root_table_addr;

    std::string _zk_root_path;
    std::string _zk_addr_list;

    static pthread_once_t _zk_init_once;
};

}  // namespace sdk
}  // namespace tera

#endif  // TERA_SDK_SDK_ZK_H_
