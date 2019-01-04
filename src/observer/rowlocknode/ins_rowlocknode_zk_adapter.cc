// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gflags/gflags.h>

#include "common/this_thread.h"
#include "ins_sdk.h"
#include "observer/rowlocknode/ins_rowlocknode_zk_adapter.h"
#include "types.h"

DECLARE_int64(tera_zk_retry_period);
DECLARE_string(rowlock_ins_root_path);
DECLARE_string(tera_ins_addr_list);
DECLARE_int32(rowlock_server_node_num);
DECLARE_string(rowlock_fake_root_path);

namespace tera {
namespace observer {

InsRowlockNodeZkAdapter::InsRowlockNodeZkAdapter(RowlockNodeImpl* rowlocknode_impl,
                                                 const std::string& server_addr)
    : rowlocknode_impl_(rowlocknode_impl), server_addr_(server_addr) {}

InsRowlockNodeZkAdapter::~InsRowlockNodeZkAdapter() {}

static void InsOnLockChange(const galaxy::ins::sdk::WatchParam& param,
                            galaxy::ins::sdk::SDKError error) {
  LOG(ERROR) << "recv lock change event";
  InsRowlockNodeZkAdapter* ins_adp = static_cast<InsRowlockNodeZkAdapter*>(param.context);
  ins_adp->OnLockChange(param.value, param.deleted);
}

void InsRowlockNodeZkAdapter::Init() {
  std::string root_path = FLAGS_rowlock_ins_root_path;
  galaxy::ins::sdk::SDKError err;
  // create session
  ins_sdk_ = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);
  // get session id
  std::string session_id = ins_sdk_->GetSessionID();

  // put server_node_num
  std::string node_num_key = root_path + kRowlockNodeNumPath;
  if (!ins_sdk_->Put(node_num_key, std::to_string(FLAGS_rowlock_server_node_num), &err)) {
    LOG(WARNING) << "put NodeNum fail";
  }

  // create node
  int id = 0;
  std::string id_lock_key;
  std::string host_lock_key;
  while (true) {
    id_lock_key = root_path + kRowlockNodeIdListPath + "/" + std::to_string(id);
    if (ins_sdk_->Put(id_lock_key, server_addr_, &err) && galaxy::ins::sdk::kOK == err) {
      host_lock_key = root_path + kRowlockNodeHostListPath + "/" + server_addr_;
      CHECK(ins_sdk_->Lock(host_lock_key, &err)) << "register fail";
      break;
    }
    if (++id >= FLAGS_rowlock_server_node_num) {
      id = 0;
    }
    ThisThread::Sleep(FLAGS_tera_zk_retry_period);
  }

  // create watch node
  CHECK(ins_sdk_->Watch(host_lock_key, &InsOnLockChange, this, &err)) << "watch lock fail";

  LOG(ERROR) << "RowlockNode Id=" << id << " host=" << server_addr_
             << " nodenum=" << FLAGS_rowlock_server_node_num;
}

void InsRowlockNodeZkAdapter::OnLockChange(std::string session_id, bool deleted) {
  _Exit(EXIT_FAILURE);
}

}  // namespace observer
}  // namespace tera
