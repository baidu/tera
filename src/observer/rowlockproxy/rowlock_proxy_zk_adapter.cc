// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlockproxy/rowlock_proxy_zk_adapter.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_number.h"
#include "observer/rowlockproxy/rowlock_proxy_impl.h"
#include "types.h"
#include "ins_sdk.h"

DECLARE_string(rowlock_zk_root_path);
DECLARE_string(tera_zk_addr_list);
DECLARE_int32(rowlock_server_node_num);
DECLARE_int64(tera_zk_retry_period);
DECLARE_int32(tera_zk_timeout);
DECLARE_int32(tera_zk_retry_max_times);

DECLARE_string(rowlock_ins_root_path);
DECLARE_string(tera_ins_addr_list);

namespace tera {
namespace observer {

RowlockProxyZkAdapter::RowlockProxyZkAdapter(RowlockProxyImpl* rowlock_proxy_impl,
                                             const std::string& server_addr)
    : rowlock_proxy_impl_(rowlock_proxy_impl), server_addr_(server_addr) {}

bool RowlockProxyZkAdapter::Init() {
  std::string root_path = FLAGS_rowlock_zk_root_path;
  std::string node_num_key = root_path + kRowlockNodeNumPath;
  std::string id_lock_path;
  std::string proxy_path = root_path + kRowlockProxyPath + "/" + server_addr_;

  int zk_errno = zk::ZE_OK;
  int32_t retry_count = 0;
  // init zk client
  while (!ZooKeeperAdapter::Init(FLAGS_tera_zk_addr_list, FLAGS_rowlock_zk_root_path,
                                 FLAGS_tera_zk_timeout, server_addr_, &zk_errno)) {
    if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
      LOG(ERROR) << "fail to init zk: " << zk::ZkErrnoToString(zk_errno);
      return false;
    }

    LOG(ERROR) << "init zk fail: " << zk::ZkErrnoToString(zk_errno) << ". retry in "
               << FLAGS_tera_zk_retry_period << " ms, retry: " << retry_count;
    ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    zk_errno = zk::ZE_OK;
  }
  LOG(INFO) << "init zk success";

  // get session id
  int64_t session_id_int = 0;
  if (!GetSessionId(&session_id_int, &zk_errno)) {
    LOG(ERROR) << "get session id fail : " << zk::ZkErrnoToString(zk_errno);
    return false;
  }

  bool is_exist = false;
  uint32_t node_num;
  while (!is_exist) {
    CheckExist(node_num_key, &is_exist, &zk_errno);
    if (!is_exist) {
      LOG(ERROR) << "rowlock service number node not found: " << node_num_key
                 << "  make sure rowlock zk available";
      ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }
  }
  std::string value;
  ReadAndWatchNode(node_num_key, &value, &zk_errno);

  if (!StringToNumber(value, &node_num)) {
    LOG(ERROR) << "read number node fail";
    return false;
  }

  rowlock_proxy_impl_->SetServerNumber(node_num);

  retry_count = 0;
  id_lock_path = root_path + kRowlockNodeIdListPath;
  std::vector<std::string> name_list;
  std::vector<std::string> data_list;

  while (!ListAndWatchChildren(id_lock_path, &name_list, &data_list, &zk_errno) ||
         name_list.size() != node_num) {
    if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
      LOG(ERROR) << "fail to watch rowlock server list or lack rowlock server";
      return false;
    }
    LOG(ERROR) << "retry watch rowlock server list in " << FLAGS_tera_zk_retry_period
               << " ms, retry=" << retry_count << " node_num: " << node_num
               << " list size: " << name_list.size();
    ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    zk_errno = zk::ZE_OK;
  }
  size_t list_count = name_list.size();
  for (size_t i = 0; i < list_count; i++) {
    const std::string& name = name_list[i];
    const std::string& data = data_list[i];

    uint32_t id;
    StringToNumber(name, &id);
    rowlock_proxy_impl_->UpdateServers(id, data);
  }

  // create proxy node
  retry_count = 0;
  while (!CreateEphemeralNode(proxy_path, server_addr_, &zk_errno)) {
    if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
      LOG(ERROR) << "fail to create proxy node";
      return false;
    }
    LOG(ERROR) << "retry create rowlock number node in " << FLAGS_tera_zk_retry_period
               << " ms, retry=" << retry_count;
    ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    zk_errno = zk::ZE_OK;
  }
  return true;
}
void RowlockProxyZkAdapter::OnNodeValueChanged(const std::string& path, const std::string& value) {
  std::string value_str;
  int zk_errno = zk::ZE_OK;
  std::string node_num_key = FLAGS_rowlock_zk_root_path + kRowlockNodeNumPath;

  if (path == node_num_key) {
    LOG(WARNING) << "rowlock service server number changed to " << value;
    uint32_t node_num;
    StringToNumber(value, &node_num);
    rowlock_proxy_impl_->SetServerNumber(node_num);
    ReadAndWatchNode(node_num_key, &value_str, &zk_errno);
  }
}

void RowlockProxyZkAdapter::OnWatchFailed(const std::string& path, int watch_type, int err) {
  LOG(ERROR) << "watch failed ! " << path;
  _Exit(EXIT_FAILURE);
}

void RowlockProxyZkAdapter::OnSessionTimeout() {
  LOG(ERROR) << "zk session timeout!";
  _Exit(EXIT_FAILURE);
}

void RowlockProxyZkAdapter::OnNodeCreated(const std::string& path) {
  std::string value;
  int zk_errno = zk::ZE_OK;

  if (path == FLAGS_rowlock_zk_root_path + kRowlockNodeNumPath) {
    LOG(WARNING) << "rowlock service number node create";
    ReadAndWatchNode(path, &value, &zk_errno);
    uint32_t node_num;
    StringToNumber(value, &node_num);
    rowlock_proxy_impl_->SetServerNumber(node_num);
  } else {
    std::string id_str = path.substr(path.find_last_of("/"), path.size() - path.find_last_of("/"));
    uint32_t id;
    StringToNumber(id_str, &id);
    ReadAndWatchNode(path, &value, &zk_errno);
    rowlock_proxy_impl_->UpdateServers(id, value);
  }
}

void RowlockProxyZkAdapter::OnNodeDeleted(const std::string& path) {
  LOG(ERROR) << "node deleted: " << path;

  int zk_errno = zk::ZE_OK;
  bool is_exist = false;
  if (path == FLAGS_rowlock_zk_root_path + kRowlockNodeNumPath) {
    while (!is_exist) {
      CheckExist(path, &is_exist, &zk_errno);
      if (!is_exist) {
        LOG(ERROR) << "rowlock service number node not found: " << path
                   << "  make sure rowlock zk available";
        ThisThread::Sleep(FLAGS_tera_zk_retry_period);
      }

      std::string value;
      ReadAndWatchNode(path, &value, &zk_errno);
      uint32_t node_num;
      if (!StringToNumber(value, &node_num)) {
        LOG(ERROR) << "read number node fail";
        return;
      }

      rowlock_proxy_impl_->SetServerNumber(node_num);
    }
    return;
  }
  // server node
  std::string id_str = path.substr(path.find_last_of("/"), path.size() - path.find_last_of("/"));
  uint32_t id;
  StringToNumber(id_str, &id);

  if (id >= rowlock_proxy_impl_->GetServerNumber()) {
    return;
  }

  while (!is_exist) {
    CheckExist(path, &is_exist, &zk_errno);
    if (!is_exist) {
      LOG(ERROR) << "rowlock server node not found: " << path;
      ThisThread::Sleep(FLAGS_tera_zk_retry_period);
    }

    std::string value;
    ReadAndWatchNode(path, &value, &zk_errno);
    uint32_t node_num;
    if (!StringToNumber(value, &node_num)) {
      LOG(ERROR) << "read number node fail";
      return;
    }

    rowlock_proxy_impl_->UpdateServers(node_num, value);
  }
}

void RowlockProxyZkAdapter::OnChildrenChanged(const std::string& path,
                                              const std::vector<std::string>& name_list,
                                              const std::vector<std::string>& data_list) {
  std::string root_path = FLAGS_rowlock_ins_root_path;
  int32_t retry_count = 0;
  int zk_errno = zk::ZE_OK;
  std::string id_lock_path = root_path + kRowlockNodeIdListPath;
  std::vector<std::string> names;
  std::vector<std::string> datum;

  while (!ListAndWatchChildren(id_lock_path, &names, &datum, &zk_errno)) {
    if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
      LOG(ERROR) << "fail to watch rowlock server list or lack rowlock server";
      _Exit(EXIT_FAILURE);
    }
    LOG(ERROR) << "retry watch rowlock server list in " << FLAGS_tera_zk_retry_period
               << " ms, retry=" << retry_count;
    ThisThread::Sleep(FLAGS_tera_zk_retry_period);
  }
  size_t list_count = name_list.size();
  for (size_t i = 0; i < list_count; i++) {
    const std::string& name = names[i];
    const std::string& data = datum[i];

    uint32_t id;
    StringToNumber(name, &id);
    rowlock_proxy_impl_->UpdateServers(id, data);
  }
}

// ins

InsRowlockProxyZkAdapter::InsRowlockProxyZkAdapter(RowlockProxyImpl* rowlock_proxy_impl,
                                                   const std::string& server_addr)
    : rowlock_proxy_impl_(rowlock_proxy_impl), server_addr_(server_addr) {}

static void InsOnNumberChange(const galaxy::ins::sdk::WatchParam& param,
                              galaxy::ins::sdk::SDKError error) {
  InsRowlockProxyZkAdapter* ins_adp = static_cast<InsRowlockProxyZkAdapter*>(param.context);
  ins_adp->OnValueChange(param.key, param.value);
}

static void InsOnServerChange(const galaxy::ins::sdk::WatchParam& param,
                              galaxy::ins::sdk::SDKError error) {
  InsRowlockProxyZkAdapter* ins_adp = static_cast<InsRowlockProxyZkAdapter*>(param.context);
  ins_adp->OnServerChange();
}

bool InsRowlockProxyZkAdapter::Init() {
  std::string root_path = FLAGS_rowlock_ins_root_path;
  std::string node_num_key = root_path + kRowlockNodeNumPath;
  std::string proxy_path = root_path + kRowlockProxyPath + "/" + server_addr_;
  std::string value;
  galaxy::ins::sdk::SDKError err;

  ins_sdk_ = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);

  LOG(INFO) << "init ins success";

  if (!ins_sdk_->Get(node_num_key, &value, &err)) {
    LOG(ERROR) << "ins rowlock service number node not found: " << node_num_key
               << "  make sure rowlock ins available";
    return false;
  }

  uint32_t node_num;
  if (!StringToNumber(value, &node_num)) {
    LOG(ERROR) << "read number node fail";
    return false;
  }
  rowlock_proxy_impl_->SetServerNumber(node_num);

  if (!ins_sdk_->Watch(node_num_key, InsOnNumberChange, this, &err)) {
    LOG(ERROR) << "try to watch number node ,path=" << node_num_key << " failed,"
               << ins_sdk_->ErrorToString(err);
    return false;
  }

  // read server addr
  int32_t retry_count = 0;
  std::string id_lock_path = root_path + kRowlockNodeIdListPath;

  while (!ins_sdk_->Watch(id_lock_path, InsOnServerChange, this, &err)) {
    LOG(ERROR) << "try to watch server node ,path=" << id_lock_path << " failed,"
               << ins_sdk_->ErrorToString(err);
    if (retry_count++ > FLAGS_tera_zk_retry_max_times) {
      return false;
    }
  }

  galaxy::ins::sdk::ScanResult* result = ins_sdk_->Scan(id_lock_path + "/!", id_lock_path + "/~");
  while (!result->Done()) {
    CHECK_EQ(result->Error(), galaxy::ins::sdk::kOK);
    std::string value = result->Value();
    std::string key = result->Key();
    VLOG(12) << "Key: " << key << " value: " << value;

    uint32_t node_num;
    uint32_t pos = key.find_last_of("/") + 1;
    key = key.substr(pos, key.length() - pos);
    VLOG(12) << "key: " << key;
    if (!StringToNumber(key, &node_num)) {
      LOG(ERROR) << "read number node fail";
      _Exit(EXIT_FAILURE);
    }

    rowlock_proxy_impl_->UpdateServers(node_num, value);
    result->Next();
  }
  delete result;

  // create proxy node
  retry_count = 0;
  while (!ins_sdk_->Put(proxy_path, server_addr_, &err)) {
    if (retry_count++ >= FLAGS_tera_zk_retry_max_times) {
      LOG(ERROR) << "fail to create proxy node";
      return false;
    }
    LOG(ERROR) << "retry create rowlock number node in " << FLAGS_tera_zk_retry_period
               << " ms, retry=" << retry_count;
    ThisThread::Sleep(FLAGS_tera_zk_retry_period);
  }
  return true;
}

void InsRowlockProxyZkAdapter::OnValueChange(const std::string& path, const std::string& value) {
  uint32_t node_num;
  galaxy::ins::sdk::SDKError err;

  if (!StringToNumber(value, &node_num)) {
    LOG(ERROR) << "read number node fail";
    return;
  }
  rowlock_proxy_impl_->SetServerNumber(node_num);

  if (!ins_sdk_->Watch(path, InsOnNumberChange, this, &err)) {
    LOG(ERROR) << "try to watch number node ,path=" << path << " failed,"
               << ins_sdk_->ErrorToString(err);
    return;
  }
}

void InsRowlockProxyZkAdapter::OnServerChange() {
  galaxy::ins::sdk::SDKError err;
  std::string root_path = FLAGS_rowlock_ins_root_path;

  int32_t retry_count = 0;
  std::string id_lock_path = root_path + kRowlockNodeIdListPath;

  while (!ins_sdk_->Watch(id_lock_path, InsOnServerChange, this, &err)) {
    LOG(ERROR) << "try to watch server node ,path=" << id_lock_path << " failed,"
               << ins_sdk_->ErrorToString(err);
    if (retry_count++ > FLAGS_tera_zk_retry_max_times) {
      _Exit(EXIT_FAILURE);
    }
  }

  galaxy::ins::sdk::ScanResult* result = ins_sdk_->Scan(id_lock_path + "/!", id_lock_path + "/~");
  while (!result->Done()) {
    CHECK_EQ(result->Error(), galaxy::ins::sdk::kOK);
    std::string value = result->Value();
    std::string key = result->Key();

    uint32_t node_num;
    uint32_t pos = key.find_last_of("/") + 1;
    key = key.substr(pos, key.length() - pos);
    VLOG(12) << "key: " << key;
    if (!StringToNumber(key, &node_num)) {
      LOG(ERROR) << "read number node fail";
      _Exit(EXIT_FAILURE);
    }

    rowlock_proxy_impl_->UpdateServers(node_num, value);
    result->Next();
  }
  delete result;
}

}  // namespace observer
}  // namespace tera
