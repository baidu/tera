// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/tabletnode/rpc_schedule.h"

#include "thirdparty/glog/logging.h"

namespace tera {
namespace tabletnode {

RpcSchedule::RpcSchedule(SchedulePolicy* policy)
    : m_policy(policy), m_pending_task_count(0), m_running_task_count(0) {}

RpcSchedule::~RpcSchedule() {
    delete m_policy;
}

void RpcSchedule::EnqueueRpc(const std::string& table_name, RpcTask* rpc) {
    MutexLock lock(&m_mutex);

    ScheduleEntity* entity = NULL;
    TableList::iterator it = m_table_list.find(table_name);
    if (it != m_table_list.end()) {
        entity = it->second;
    } else {
        entity = m_table_list[table_name] = m_policy->NewScheEntity(new TaskQueue);
    }

    TaskQueue* task_queue = (TaskQueue*)entity->user_ptr;
    task_queue->push(rpc);

    task_queue->pending_count++;
    m_pending_task_count++;

    if (task_queue->pending_count == 1) {
        m_policy->Enable(entity);
    }
}

bool RpcSchedule::DequeueRpc(RpcTask** rpc) {
    MutexLock lock(&m_mutex);
    if (m_pending_task_count == 0) {
        return false;
    }

    TableList::iterator it = m_policy->Pick(&m_table_list);
    CHECK(it != m_table_list.end());

    ScheduleEntity* entity = (ScheduleEntity*)it->second;
    TaskQueue* task_queue = (TaskQueue*)entity->user_ptr;
    CHECK_GT(task_queue->size(), 0);

    *rpc = task_queue->front();
    task_queue->pop();

    task_queue->pending_count--;
    task_queue->running_count++;
    m_pending_task_count--;
    m_running_task_count++;

    if (task_queue->pending_count == 0) {
        m_policy->Disable(entity);
    }
    return true;
}

bool RpcSchedule::FinishRpc(const std::string& table_name) {
    MutexLock lock(&m_mutex);
    if (m_running_task_count == 0) {
        return false;
    }
    TableList::iterator it = m_table_list.find(table_name);
    if (it == m_table_list.end()) {
        return false;
    }

    ScheduleEntity* entity = (ScheduleEntity*)it->second;
    m_policy->Done(entity);

    TaskQueue* task_queue = (TaskQueue*)entity->user_ptr;
    task_queue->running_count--;
    m_running_task_count--;

    if (task_queue->running_count == 0
        && task_queue->pending_count == 0) {
        delete task_queue;
        delete entity;
        m_table_list.erase(it);
    }
    return true;
}

} // namespace tabletnode
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
