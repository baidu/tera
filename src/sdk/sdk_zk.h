// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SDK_ZK_H_
#define  TERA_SDK_SDK_ZK_H_

#include <pthread.h>
#include <string>
#include <common/mutex.h>

namespace tera {
namespace sdk {

class ClusterFinder
{
public:
    ClusterFinder() {}
    virtual ~ClusterFinder() {}
    std::string MasterAddr(bool update = false);
    std::string RootTableAddr(bool update = false);

private:
    virtual bool ReadNode(const std::string& path, std::string* value) = 0;

    mutable Mutex _mutex;
    std::string _master_addr;
    std::string _root_table_addr;
};

class ZkClusterFinder : public ClusterFinder {
public:
    ZkClusterFinder(const std::string& zk_root_path, const std::string& zk_addr_list);
private:
    virtual bool ReadNode(const std::string& path, std::string* value);
    std::string _zk_root_path;
    std::string _zk_addr_list;
};

class InsClusterFinder : public ClusterFinder {
public:
    InsClusterFinder(const std::string& ins_root_path, const std::string& ins_addr_list);
private:
    virtual bool ReadNode(const std::string& path, std::string* value);
    std::string _ins_root_path;
    std::string _ins_addr_list;
};

class FakeZkClusterFinder : public ClusterFinder {
public:
    FakeZkClusterFinder(const std::string& fake_zk_path_prefix);
private:
    virtual bool ReadNode(const std::string& path, std::string* value);
    std::string _fake_zk_path_prefix;
};

ClusterFinder* NewClusterFinder();

}  // namespace sdk
}  // namespace tera

#endif  // TERA_SDK_SDK_ZK_H_
