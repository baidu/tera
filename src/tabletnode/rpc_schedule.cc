// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/rpc_schedule.h"

#include "glog/logging.h"

namespace tera {
namespace tabletnode {

RpcSchedule::RpcSchedule(SchedulePolicy* policy)
    : policy_(policy), pending_task_count_(0), running_task_count_(0) {}

RpcSchedule::~RpcSchedule() {
    delete policy_;
}

void RpcSchedule::EnqueueRpc(const std::string& table_name, RpcTask* rpc) {
    MutexLock lock(&mutex_);

    ScheduleEntity* entity = NULL;
    TableList::iterator it = table_list_.find(table_name);
    if (it != table_list_.end()) {
        entity = it->second;
    } else {
        entity = table_list_[table_name] = policy_->NewScheEntity(new TaskQueue);
    }

    TaskQueue* task_queue = (TaskQueue*)entity->user_ptr;
    task_queue->push(rpc);

    task_queue->pending_count++;
    pending_task_count_++;

    if (task_queue->pending_count == 1) {
        policy_->Enable(entity);
    }
}

bool RpcSchedule::DequeueRpc(RpcTask** rpc) {
    MutexLock lock(&mutex_);
    if (pending_task_count_ == 0) {
        return false;
    }

    TableList::iterator it = policy_->Pick(&table_list_);
    CHECK(it != table_list_.end());

    ScheduleEntity* entity = (ScheduleEntity*)it->second;
    TaskQueue* task_queue = (TaskQueue*)entity->user_ptr;
    CHECK_GT(task_queue->size(), 0U);

    *rpc = task_queue->front();
    task_queue->pop();

    task_queue->pending_count--;
    task_queue->running_count++;
    pending_task_count_--;
    running_task_count_++;

    if (task_queue->pending_count == 0) {
        policy_->Disable(entity);
    }
    return true;
}

bool RpcSchedule::FinishRpc(const std::string& table_name) {
    MutexLock lock(&mutex_);
    if (running_task_count_ == 0) {
        return false;
    }
    TableList::iterator it = table_list_.find(table_name);
    if (it == table_list_.end()) {
        return false;
    }

    ScheduleEntity* entity = (ScheduleEntity*)it->second;
    policy_->Done(entity);

    TaskQueue* task_queue = (TaskQueue*)entity->user_ptr;
    task_queue->running_count--;
    running_task_count_--;

    if (task_queue->running_count == 0
        && task_queue->pending_count == 0) {
        delete task_queue;
        delete entity;
        table_list_.erase(it);
    }
    return true;
}

} // namespace tabletnode
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
