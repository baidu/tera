// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "sdk/rowlock_client.h"
#include "zk/zk_adapter.h"
#include "ins_sdk.h"

namespace galaxy {
namespace ins {
namespace sdk {
class InsSDK;
}  // namespace sdk
}  // namespace ins
}  // namespace galaxy

namespace tera {
namespace observer {

class RowlockClient;

class ZkRowlockClientZkAdapter : public zk::ZooKeeperLightAdapter {
 public:
  ZkRowlockClientZkAdapter(RowlockClient* server_client, const std::string& server_addr);
  virtual ~ZkRowlockClientZkAdapter();
  virtual bool Init();

 private:
  RowlockClient* client_;
  std::string server_addr_;
};

class InsRowlockClientZkAdapter : public ZkRowlockClientZkAdapter {
 public:
  InsRowlockClientZkAdapter(RowlockClient* server_client, const std::string& server_addr);
  virtual ~InsRowlockClientZkAdapter(){};
  virtual bool Init();

 protected:
  virtual void OnNodeValueChanged(const std::string& path, const std::string& value) {}
  virtual void OnWatchFailed(const std::string& path, int watch_type, int err) {}
  virtual void OnNodeDeleted(const std::string& path) {}
  virtual void OnSessionTimeout() {}
  virtual void OnNodeCreated(const std::string& path) {}
  virtual void OnChildrenChanged(const std::string& path, const std::vector<std::string>& name_list,
                                 const std::vector<std::string>& data_list) {}

 private:
  RowlockClient* client_;
  std::string server_addr_;
  galaxy::ins::sdk::InsSDK* ins_sdk_;
};

}  // namespace observer
}  // namespace tera
