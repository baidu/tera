// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_ZK_ADAPTER_H_
#define TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_ZK_ADAPTER_H_

#include "zk/zk_adapter.h"

namespace galaxy {
namespace ins {
namespace sdk {
class InsSDK;
}  // namespace sdk
}  // namespace ins
}  // namespace galaxy

namespace tera {
namespace observer {

class RowlockProxyImpl;

class RowlockProxyZkAdapterBase : public zk::ZooKeeperAdapter {
 public:
  virtual ~RowlockProxyZkAdapterBase() {}
  virtual bool Init() = 0;
};

class RowlockProxyZkAdapter : public RowlockProxyZkAdapterBase {
 public:
  RowlockProxyZkAdapter(RowlockProxyImpl* rowlock_proxy_impl, const std::string& server_addr);
  virtual ~RowlockProxyZkAdapter() {}
  virtual bool Init();

 protected:
  virtual void OnNodeValueChanged(const std::string& path, const std::string& value);
  virtual void OnWatchFailed(const std::string& path, int watch_type, int err);
  virtual void OnNodeDeleted(const std::string& path);
  virtual void OnSessionTimeout();
  virtual void OnNodeCreated(const std::string& path);
  virtual void OnChildrenChanged(const std::string& path, const std::vector<std::string>& name_list,
                                 const std::vector<std::string>& data_list);

 private:
  RowlockProxyImpl* rowlock_proxy_impl_;
  std::string server_addr_;
};

class InsRowlockProxyZkAdapter : public RowlockProxyZkAdapterBase {
 public:
  InsRowlockProxyZkAdapter(RowlockProxyImpl* rowlock_proxy_impl, const std::string& server_addr);
  virtual ~InsRowlockProxyZkAdapter() {}
  virtual bool Init();

  void OnValueChange(const std::string& path, const std::string& value);
  void OnServerChange();

 protected:
  virtual void OnNodeValueChanged(const std::string& path, const std::string& value) {}
  virtual void OnWatchFailed(const std::string& path, int watch_type, int err) {}
  virtual void OnNodeDeleted(const std::string& path) {}
  virtual void OnSessionTimeout() {}
  virtual void OnNodeCreated(const std::string& path) {}
  virtual void OnChildrenChanged(const std::string& path, const std::vector<std::string>& name_list,
                                 const std::vector<std::string>& data_list) {}

 private:
  RowlockProxyImpl* rowlock_proxy_impl_;
  std::string server_addr_;
  galaxy::ins::sdk::InsSDK* ins_sdk_;
};

}  // namespace observer
}  // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKPROXY_ROWLOCK_PROXY_ZK_ADAPTER_H_
