// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Description:  concurrency control with priority for (load/unload/split/merge)

#include "task_spatula.h"

namespace tera {
namespace master {

TaskSpatula::TaskSpatula(int32_t max)
    :m_pending_count(0), m_running_count(0), m_max_concurrency(max) {}

TaskSpatula::~TaskSpatula() {
    assert(m_queue.size() == 0); // TODO copy from ts-a to ts-b, clear m_queue of a
}

void TaskSpatula::EnQueueTask(concurrency_task_t atask) {
    MutexLock lock(&m_mutex);
    m_queue.push(atask);
    m_pending_count++;
}

bool TaskSpatula::DeQueueTask(concurrency_task_t *atask) {
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

void TaskSpatula::TryDrain() {
    boost::function<void ()> dummy_func = boost::bind(&TaskSpatula::TryDrain, this);
    concurrency_task_t atask("dummy data", dummy_func);
    while(m_running_count < m_max_concurrency
          && DeQueueTask(&atask)) {
        atask.async_call();
        {
            MutexLock lock(&m_mutex);
            m_running_count++;
        }
    }

    /*  for debug
    if (m_running_count < m_max_concurrency) {
        LOG(INFO) << "[task spatula] queue is empty";
    } else {
        LOG(INFO) << "[task spatula] m_running_count : " << m_running_count; 
    } 
    */
}

} // namespace master
} // namespace tera
