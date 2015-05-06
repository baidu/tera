// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Description:  concurrency control with priority for (load/unload/split/merge)

#include "task_spatula.h"

#include <glog/logging.h>

namespace tera {
namespace master {

TaskSpatula::TaskSpatula(int32_t max)
    :m_pending_count(0), m_running_count(0), m_max_concurrency(max) {}

TaskSpatula::~TaskSpatula() {
}

void TaskSpatula::EnQueueTask(const ConcurrencyTask& atask) {
    MutexLock lock(&m_mutex);
    m_queue.push(atask);
    m_pending_count++;
}

bool TaskSpatula::DeQueueTask(ConcurrencyTask* atask) {
    MutexLock lock(&m_mutex);
    assert(atask != NULL);
    if(m_queue.size() <= 0) {
        return false;
    }
    *atask = m_queue.top();
    m_queue.pop();
    m_pending_count--;
    return true;
}

void TaskSpatula::FinishTask() {
    MutexLock lock(&m_mutex);
    assert(m_running_count > 0);
    m_running_count--;
}

int32_t TaskSpatula::TryDraining() {
    boost::function<void ()> dummy_func = boost::bind(&TaskSpatula::TryDraining, this);
    ConcurrencyTask atask(0, dummy_func);
    int done = 0;
    LOG(INFO) << "running: " << m_running_count
        << "max: " << m_max_concurrency;
    while(m_running_count < m_max_concurrency
          && DeQueueTask(&atask)) {
        atask.async_call();
        done++;
        {
            MutexLock lock(&m_mutex);
            m_running_count++;
        }
    }
    return done;
}

int32_t TaskSpatula::GetRunningCount() {
    return m_running_count;
}

} // namespace master
} // namespace tera
