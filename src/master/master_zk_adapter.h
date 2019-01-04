// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MASTER_ZK_ADAPTER_H_
#define TERA_MASTER_MASTER_ZK_ADAPTER_H_

#include <string>
#include <vector>

#include "master/master_impl.h"
#include "zk/zk_adapter.h"

namespace galaxy {
namespace ins {
namespace sdk {
class InsSDK;
}
}
}

namespace tera {
namespace master {

class MasterZkAdapterBase : public zk::ZooKeeperAdapter {
 public:
  virtual ~MasterZkAdapterBase(){};
  virtual bool Init(std::string* root_tablet_addr,
                    std::map<std::string, std::string>* tabletnode_list, bool* safe_mode) = 0;

  virtual bool KickTabletServer(const std::string& ts_host, const std::string& ts_zk_id) = 0;
  virtual bool MarkSafeMode() = 0;
  virtual bool UnmarkSafeMode() = 0;
  virtual bool HasSafeModeNode() = 0;
  virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr) = 0;
};

class MasterZkAdapter : public MasterZkAdapterBase {
 public:
  MasterZkAdapter(MasterImpl* master_impl, const std::string& server_addr);
  virtual ~MasterZkAdapter();
  virtual bool Init(std::string* root_tablet_addr,
                    std::map<std::string, std::string>* tabletnode_list, bool* safe_mode);

  virtual bool KickTabletServer(const std::string& ts_host, const std::string& ts_zk_id);
  virtual bool MarkSafeMode();
  virtual bool HasSafeModeNode();
  virtual bool UnmarkSafeMode();
  virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr);

 protected:
  virtual bool Setup();
  virtual void Reset();

  virtual bool LockMasterLock();
  virtual bool UnlockMasterLock();
  virtual bool WatchMasterLock();
  virtual bool CreateMasterNode();
  virtual bool DeleteMasterNode();

  virtual bool WatchRootTabletNode(bool* is_exist, std::string* root_tablet_addr);
  virtual bool WatchSafeModeMark(bool* is_safemode);
  virtual bool WatchTabletNodeList(std::map<std::string, std::string>* tabletnode_list);

  virtual void OnSafeModeMarkCreated();
  virtual void OnSafeModeMarkDeleted();
  virtual void OnTabletNodeListDeleted();
  virtual void OnRootTabletNodeDeleted();
  virtual void OnMasterNodeDeleted();
  virtual void OnZkLockDeleted();
  virtual void OnTabletServerKickMarkCreated();
  virtual void OnTabletServerKickMarkDeleted();
  virtual void OnTabletServerStart(const std::string& ts_host);
  virtual void OnTabletServerExist(const std::string& ts_host);

  virtual void OnChildrenChanged(const std::string& path, const std::vector<std::string>& name_list,
                                 const std::vector<std::string>& data_list);
  virtual void OnNodeValueChanged(const std::string& path, const std::string& value);
  virtual void OnNodeCreated(const std::string& path);
  virtual void OnNodeDeleted(const std::string& path);
  virtual void OnWatchFailed(const std::string& path, int watch_type, int err);
  virtual void OnSessionTimeout();

 private:
  mutable Mutex mutex_;
  MasterImpl* master_impl_;
  std::string server_addr_;
};

class MockMasterZkAdapter : public MasterZkAdapter {
 public:
  MockMasterZkAdapter(MasterImpl* master_impl, const std::string& server_addr)
      : MasterZkAdapter(master_impl, server_addr) {}
  virtual ~MockMasterZkAdapter() {}
};

/*
 * This is not zookeeper!
 * Just used on onebox for tasting tera briefly.
 * This is implemented through local file system.
 * Not support watching.
 */
class FakeMasterZkAdapter : public MasterZkAdapterBase {
 public:
  FakeMasterZkAdapter(MasterImpl* master_impl, const std::string& server_addr);
  virtual ~FakeMasterZkAdapter();
  virtual bool Init(std::string* root_tablet_addr,
                    std::map<std::string, std::string>* tabletnode_list, bool* safe_mode);

  virtual bool KickTabletServer(const std::string& ts_host, const std::string& ts_zk_id);
  virtual bool MarkSafeMode();
  virtual bool HasSafeModeNode() { return false; }
  virtual bool UnmarkSafeMode();
  virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr);

 private:
  virtual void OnChildrenChanged(const std::string& path, const std::vector<std::string>& name_list,
                                 const std::vector<std::string>& data_list);
  virtual void OnNodeValueChanged(const std::string& path, const std::string& value);
  virtual void OnNodeCreated(const std::string& path);
  virtual void OnNodeDeleted(const std::string& path);
  virtual void OnWatchFailed(const std::string& path, int watch_type, int err);
  virtual void OnSessionTimeout();

 private:
  mutable Mutex mutex_;
  MasterImpl* master_impl_;
  std::string server_addr_;
  std::string fake_path_;
};

class InsMasterZkAdapter : public MasterZkAdapterBase {
 public:
  InsMasterZkAdapter(MasterImpl* master_impl, const std::string& server_addr);
  virtual ~InsMasterZkAdapter();
  virtual bool Init(std::string* root_tablet_addr,
                    std::map<std::string, std::string>* tabletnode_list, bool* safe_mode);

  virtual bool KickTabletServer(const std::string& ts_host, const std::string& ts_zk_id);
  virtual bool MarkSafeMode() { return true; }
  virtual bool HasSafeModeNode() { return false; }
  virtual bool UnmarkSafeMode() { return true; }
  virtual bool UpdateRootTabletNode(const std::string& root_tablet_addr);
  void RefreshTabletNodeList();
  void OnLockChange(std::string session_id, bool deleted);
  void OnSessionTimeout();

 private:
  virtual void OnChildrenChanged(const std::string& path, const std::vector<std::string>& name_list,
                                 const std::vector<std::string>& data_list) {}
  virtual void OnNodeValueChanged(const std::string& path, const std::string& value) {}
  virtual void OnNodeCreated(const std::string& path) {}
  virtual void OnNodeDeleted(const std::string& path) {}
  virtual void OnWatchFailed(const std::string& path, int watch_type, int err) {}

 private:
  mutable Mutex mutex_;
  MasterImpl* master_impl_;
  std::string server_addr_;
  galaxy::ins::sdk::InsSDK* ins_sdk_;
};

class MockInsMasterZkAdapter : public InsMasterZkAdapter {
 public:
  MockInsMasterZkAdapter(MasterImpl* master_impl, const std::string& server_addr)
      : InsMasterZkAdapter(master_impl, server_addr) {}
  virtual ~MockInsMasterZkAdapter() {}
};

}  // namespace master
}  // namespace tera

#endif  // TERA_MASTER_MASTER_ZK_ADAPTER_H_
