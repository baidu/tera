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
    std::string ClusterId(); // cluster URI: <scheme>://<authority>/<path>

protected:
    virtual bool ReadNode(const std::string& path, std::string* value) = 0;
    virtual std::string Name() = 0;
    virtual std::string Authority() = 0;
    virtual std::string Path() = 0;

private:
    mutable Mutex mutex_;
    std::string master_addr_;
    std::string root_table_addr_;
};

class ZkClusterFinder : public ClusterFinder {
public:
    ZkClusterFinder(const std::string& zk_root_path, const std::string& zk_addr_list);
protected:
    virtual bool ReadNode(const std::string& path, std::string* value);
    virtual std::string Name() { return "zk"; };
    virtual std::string Authority() { return zk_addr_list_; }
    virtual std::string Path() { return zk_root_path_; }
private:
    std::string zk_root_path_;
    std::string zk_addr_list_;
};

class InsClusterFinder : public ClusterFinder {
public:
    InsClusterFinder(const std::string& ins_root_path, const std::string& ins_addr_list);
protected:
    virtual bool ReadNode(const std::string& path, std::string* value);
    virtual std::string Name() { return "ins"; }
    virtual std::string Authority() { return ins_addr_list_; }
    virtual std::string Path() { return ins_root_path_; }
private:
    std::string ins_root_path_;
    std::string ins_addr_list_;
};

class MockInsClusterFinder : public InsClusterFinder {
public:
    MockInsClusterFinder(const std::string& ins_root_path, const std::string& ins_addr_list) :
        InsClusterFinder(ins_root_path, ins_addr_list) {}
protected:
    virtual std::string Name() { return "mock ins"; }
};

class FakeZkClusterFinder : public ClusterFinder {
public:
    FakeZkClusterFinder(const std::string& fake_zk_path_prefix);
protected:
    virtual bool ReadNode(const std::string& path, std::string* value);
    virtual std::string Name() { return "fakezk"; };
    virtual std::string Authority() { return "localhost"; }
    virtual std::string Path() { return fake_zk_path_prefix_; }
private:
    std::string fake_zk_path_prefix_;
};

ClusterFinder* NewClusterFinder();

}  // namespace sdk
}  // namespace tera

#endif  // TERA_SDK_SDK_ZK_H_
