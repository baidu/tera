// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlocknode/fake_rowlocknode_zk_adapter.h"

#include <stdlib.h>

#include <gflags/gflags.h>

#include "common/this_thread.h"
#include "ins_sdk.h"
#include "observer/rowlocknode/rowlocknode_zk_adapter_base.h"
#include "types.h"

DECLARE_string(rowlock_ins_root_path);
DECLARE_int32(rowlock_server_node_num);
DECLARE_string(rowlock_fake_root_path);

namespace tera {
namespace observer {

FakeRowlockNodeZkAdapter::FakeRowlockNodeZkAdapter(RowlockNodeImpl* rowlocknode_impl,
                                                   const std::string& server_addr)
    : rowlocknode_impl_(rowlocknode_impl), server_addr_(server_addr) {}

FakeRowlockNodeZkAdapter::~FakeRowlockNodeZkAdapter() {}

void FakeRowlockNodeZkAdapter::Init() {
  std::string root_path = FLAGS_rowlock_fake_root_path;

  std::string node_num_key = root_path + kRowlockNodeNumPath;
  zk::FakeZkUtil::WriteNode(node_num_key, std::to_string(FLAGS_rowlock_server_node_num));

  // create node
  int id = 0;
  std::string id_lock_key;
  std::string host_lock_key;
  while (true) {
    id_lock_key = root_path + kRowlockNodeIdListPath + "/" + std::to_string(id);
    std::string file_path = "mkdir -p " + root_path + kRowlockNodeIdListPath;
    system(file_path.c_str());
    if (zk::FakeZkUtil::WriteNode(id_lock_key, std::to_string(id))) {
      break;
    } else {
      LOG(ERROR) << "[Fake rowlock zk]: write node " << id_lock_key << " failed";
    }
    if (++id >= FLAGS_rowlock_server_node_num) {
      id = 0;
    }
    ThisThread::Sleep(1);
  }

  LOG(INFO) << "RowlockNode Id=" << id << " host=" << server_addr_
            << " nodenum=" << FLAGS_rowlock_server_node_num;
}

void FakeRowlockNodeZkAdapter::OnLockChange(std::string session_id, bool deleted) {
  _Exit(EXIT_FAILURE);
}

}  // namespace observer
}  // namespace tera
