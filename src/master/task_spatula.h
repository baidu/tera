// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Description:  concurrency control with priority for (load/unload/split/merge)

#ifndef TERA_MASTER_TASK_SPATULA_H_
#define TERA_MASTER_TASK_SPATULA_H_

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <map>
#include <queue>

#include "common/mutex.h"
#include "master/tablet_manager.h"

namespace tera {
namespace master {

// type of item in concurrency control queue
struct ConcurrencyTask {
    // great number comes great priority
    int32_t priority;
    TabletPtr tablet;
    boost::function<void ()> async_call;

    ConcurrencyTask(int p, TabletPtr t, boost::function<void ()>& call)
        : priority(p), tablet(t), async_call(call) {}

    bool operator< (const ConcurrencyTask& t) const {
        return priority < t.priority;
    }
};

class TaskSpatula {
public:
    TaskSpatula(int32_t concurrency_max);
    ~TaskSpatula();

    // the function adds a item(`task') to concurrency control queue
    void EnQueueTask(const ConcurrencyTask& task);

    // the function do its best to executes task in concurrency control queue,
    // until:
    // 1) reachs the threshold (running count >= concurrency_max), 
    //    concurrency_max is the parameter of constructor
    // 2) concurrency control queue has no item anymore
    int32_t TryDraining();

    // the function minus (count of running task) one
    // users must call this function when a async task done!
    void FinishTask();

    // the function return the count of task is running
    int32_t GetRunningCount();

private:
    // the function deletes a item (`task') from concurrency control queue
    // and stores the deleted item at the location given by `task'.
    //
    // return value:
    // if concurrency control queue is empty before deletes, return false;
    // otherwise, returns true.
    bool DeQueueTask(ConcurrencyTask* task);

    void AddTablet(TabletPtr tablet);
    void DeleteTablet(TabletPtr tablet);

    mutable Mutex m_mutex;
    std::priority_queue<ConcurrencyTask> m_queue; // concurrency control queue
    int32_t m_running_count; // count of task is running
    int32_t m_max_concurrency;

    std::set<TabletPtr> m_tablets;
};

} // namespace master
} // namespace tera

#endif  // TERA_MASTER_TASK_SPATULA_H_
