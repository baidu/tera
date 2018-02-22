// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SDK_ZK_H_
#define  TERA_SDK_SDK_ZK_H_

#include <pthread.h>
#include <string>
#include <common/mutex.h>

#include "ins_sdk.h"
#include "zk/zk_adapter.h"

namespace galaxy{
namespace ins{
namespace sdk {
    class InsSDK;
}
}
}

namespace tera {
namespace sdk {

class ClientZkAdapterBase : public zk::ZooKeeperLightAdapter {
public:
    virtual ~ClientZkAdapterBase() {};
    virtual bool Init() = 0;
    virtual bool RegisterClient(std::string* session_str) = 0;
    virtual bool IsClientAlive(const std::string& path) = 0;
    virtual bool ReadNode(const std::string& path, std::string* value) = 0;
};

class ClientZkAdapter : public ClientZkAdapterBase {
public:
    ClientZkAdapter() {}
    virtual ~ClientZkAdapter() {}
    virtual bool Init();
    virtual bool RegisterClient(std::string* session_str);
    virtual bool IsClientAlive(const std::string& path);
    virtual bool ReadNode(const std::string& path, std::string* value);
private:
    mutable Mutex mutex_;
};

class MockClientZkAdapter : public ClientZkAdapter {
public:
    MockClientZkAdapter(): ClientZkAdapter() {}
    virtual ~MockClientZkAdapter() {}
    virtual bool Init() { return true; }
    virtual bool RegisterClient(std::string* session_str) { 
        *session_str = "localhost";
        return true;
    }
    virtual bool IsClientAlive(const std::string& path) {
        return true;
    }
    virtual bool ReadNode(const std::string& path, std::string* value) {
        *value = "mock_zk_value";
        return true;
    }
};

class InsClientZkAdapter : public ClientZkAdapterBase {
public:
    InsClientZkAdapter() : ins_sdk_(NULL) {}
    virtual ~InsClientZkAdapter() {
        if (ins_sdk_ != NULL) {
            delete ins_sdk_;
        }
    }
    virtual bool Init ();
    virtual bool RegisterClient(std::string* session_str);
    virtual bool IsClientAlive(const std::string& path);
    virtual bool ReadNode(const std::string& path, std::string* value);
private:
    galaxy::ins::sdk::InsSDK* ins_sdk_;
};

class MockInsClientZkAdapter : public InsClientZkAdapter {
public:
    MockInsClientZkAdapter() : InsClientZkAdapter() {}
    virtual ~MockInsClientZkAdapter() {}
    virtual bool Init() { return true; }
    virtual bool RegisterClient(std::string* session_str) { 
        *session_str = "localhost";
        return true;
    }
    virtual bool IsClientAlive(const std::string& path) {
        return true;
    }
    virtual bool ReadNode(const std::string& path, std::string* value) {
        *value = "mock_ins_value";
        return true;
    }
};

ClientZkAdapterBase* NewClientZkAdapter();

class ClusterFinder
{
public:
    ClusterFinder() {}
    virtual ~ClusterFinder() {}
    std::string MasterAddr(bool update = false);
    std::string RootTableAddr(bool update = false);
    std::string TimeoracleAddr(bool update = false);
    std::string ClusterId(); // cluster URI: <scheme>://<authority>/<path>

protected:
    virtual bool ReadNode(const std::string& path, std::string* value) = 0;
    virtual std::string Name() = 0;
    virtual std::string Authority() = 0;
    virtual std::string Path() = 0;

private:
    mutable Mutex mutex_;
    std::string master_addr_;
    std::string timeoracle_addr_;
    std::string root_table_addr_;
};

class ZkClusterFinder : public ClusterFinder {
public:
    ZkClusterFinder(const std::string& zk_root_path,
                    const std::string& zk_addr_list,
                    ClientZkAdapterBase* zk_adapter = NULL);
protected:
    virtual bool ReadNode(const std::string& path, std::string* value);
    virtual std::string Name() { return "zk"; };
    virtual std::string Authority() { return zk_addr_list_; }
    virtual std::string Path() { return zk_root_path_; }
private:
    std::string zk_root_path_;
    std::string zk_addr_list_;
    ClientZkAdapterBase* zk_adapter_;
};

class MockZkClusterFinder : public ZkClusterFinder {
public:
    MockZkClusterFinder(const std::string& zk_root_path, const std::string& zk_addr_list) :
        ZkClusterFinder(zk_root_path, zk_addr_list) {}
protected:
    virtual std::string Name() { return "mock zk"; }
};

class InsClusterFinder : public ClusterFinder {
public:
    InsClusterFinder(const std::string& ins_root_path,
                     const std::string& ins_addr_list,
                     ClientZkAdapterBase* zk_adapter = NULL);
protected:
    virtual bool ReadNode(const std::string& path, std::string* value);
    virtual std::string Name() { return "ins"; }
    virtual std::string Authority() { return ins_addr_list_; }
    virtual std::string Path() { return ins_root_path_; }
private:
    std::string ins_root_path_;
    std::string ins_addr_list_;
    ClientZkAdapterBase* zk_adapter_;
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

class MockTimeoracleClusterFinder : public ClusterFinder {
public:
    MockTimeoracleClusterFinder(const std::string& mock_root_path);

protected:
    virtual bool ReadNode(const std::string& path, std::string* value);

    virtual std::string Name() { return "fakezk"; };

    virtual std::string Authority() { return "localhost"; }

    virtual std::string Path() { return mock_root_path_; }
private:
    std::string mock_root_path_;
};

ClusterFinder* NewTimeoracleClusterFinder();
ClusterFinder* NewClusterFinder(ClientZkAdapterBase* zk_adapter = NULL);

}  // namespace sdk
}  // namespace tera

#endif  // TERA_SDK_SDK_ZK_H_
