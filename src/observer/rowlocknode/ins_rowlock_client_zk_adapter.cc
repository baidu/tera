// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlocknode/ins_rowlock_client_zk_adapter.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "ins_sdk.h"

#include "sdk/rowlock_client.h"
#include "types.h"

DECLARE_string(rowlock_ins_root_path);
DECLARE_string(tera_ins_addr_list);
DECLARE_int32(rowlock_server_node_num);
DECLARE_int64(tera_zk_retry_period); 
DECLARE_int32(tera_zk_timeout);
DECLARE_int32(tera_zk_retry_max_times);

namespace tera {
namespace observer {

InsRowlockClientZkAdapter::InsRowlockClientZkAdapter(RowlockClient* server_client, 
											   const std::string& server_addr)
    : ZkRowlockClientZkAdapter(server_client, server_addr),
      client_(server_client),
      server_addr_(server_addr) {}
      
bool InsRowlockClientZkAdapter::Init() {
    std::string root_path = FLAGS_rowlock_ins_root_path;
    std::vector<std::string> value;
    // create session
    ins_sdk_ = new galaxy::ins::sdk::InsSDK(FLAGS_tera_ins_addr_list);

    // put server_node_num
    std::string rowlock_proxy_path = root_path + kRowlockProxyPath;

    galaxy::ins::sdk::ScanResult* result = ins_sdk_->Scan(rowlock_proxy_path + "/!",
                                                          rowlock_proxy_path + "/~");
    while (!result->Done()) {
        CHECK_EQ(result->Error(), galaxy::ins::sdk::kOK);
        value.push_back(result->Value());
        result->Next();
    }
    delete result;

    client_->Update(value);
    return true;
}

} // namespace observer
} // namespace tera

