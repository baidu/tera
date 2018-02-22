// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/rowlocknode/rowlocknode_impl.h"

#include "common/timer.h"
#include "observer/rowlocknode/fake_rowlocknode_zk_adapter.h"
#include "observer/rowlocknode/ins_rowlocknode_zk_adapter.h"
#include "observer/rowlocknode/rowlocknode_zk_adapter.h"
#include "utils/utils_cmd.h"

DECLARE_string(rowlock_server_port);
DECLARE_string(tera_coord_type);

namespace tera {
namespace observer {

RowlockNodeImpl::RowlockNodeImpl() {}

RowlockNodeImpl::~RowlockNodeImpl() {}

bool RowlockNodeImpl::Init() {
    std::string local_addr = tera::utils::GetLocalHostName() + ":" + FLAGS_rowlock_server_port;
    if (FLAGS_tera_coord_type == "zk") {
        zk_adapter_.reset(new RowlockNodeZkAdapter(this, local_addr));
    } else if (FLAGS_tera_coord_type == "ins") {
        zk_adapter_.reset(new InsRowlockNodeZkAdapter(this, local_addr));
    } else {
        zk_adapter_.reset(new FakeRowlockNodeZkAdapter(this, local_addr));
    }

    zk_adapter_->Init();

    LOG(INFO) << "Rowlock node init finish";
    return true;
}

bool RowlockNodeImpl::Exit() {
    return true;
}

void RowlockNodeImpl::TryLock(const RowlockRequest* request,
        RowlockResponse* response,
        google::protobuf::Closure* done) {
    uint64_t rowlock_key = GetRowlockKey(request->table_name(), request->row());
    if (rowlock_db_.TryLock(rowlock_key)) {
        response->set_lock_status(kLockSucc);
    } else {
        response->set_lock_status(kLockFail);
        LOG(WARNING) << " table name: " << request->table_name()
                     << " row :" << request->row();
    }

    done->Run();
}

void RowlockNodeImpl::UnLock(const RowlockRequest* request,
        RowlockResponse* response,
        google::protobuf::Closure* done) {
    uint64_t rowlock_key = GetRowlockKey(request->table_name(), request->row());
    rowlock_db_.UnLock(rowlock_key);
    response->set_lock_status(kLockSucc);
    done->Run();
}

void RowlockNodeImpl::PrintQPS() {
    return;
}

uint64_t RowlockNodeImpl::GetRowlockKey(const std::string& table_name, 
                                        const std::string& row) const {
    // RowlockKey : TableName + Row
    std::string rowlock_key_str = table_name + row;
    return std::hash<std::string>()(rowlock_key_str);

}


} // namespace observer
} // namespace tera

