// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_RPC_SCHEDULE_H_
#define TERA_TABLETNODE_RPC_SCHEDULE_H_

#include <queue>

#include "common/mutex.h"

#include "proto/tabletnode_rpc.pb.h"
#include "tabletnode/rpc_schedule_policy.h"

namespace tera {
namespace tabletnode {

struct RpcTask {
    uint8_t rpc_type;
    RpcTask(uint8_t type) : rpc_type(type) {}
};

class RpcSchedule {
public:
    RpcSchedule(SchedulePolicy* policy);
    ~RpcSchedule();

    void EnqueueRpc(const std::string& table_name, RpcTask* rpc);

    bool DequeueRpc(RpcTask** rpc);

    bool FinishRpc(const std::string& table_name);

private:
    mutable Mutex m_mutex;
    SchedulePolicy* m_policy;

    typedef std::string TableName;
    struct TaskQueue : public std::queue<RpcTask*> {
        uint64_t pending_count;
        uint64_t running_count;

        TaskQueue() : pending_count(0), running_count(0) {}
    };

    typedef std::map<TableName, ScheduleEntity*> TableList;

    TableList m_table_list;
    uint64_t m_pending_task_count;
    uint64_t m_running_task_count;
};

} // namespace tabletnode
} // namespace tera

#endif  // TERA_TABLETNODE_RPC_SCHEDULE_H_
