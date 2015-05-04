// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Description:  concurrency control with priority for (load/unload/split/merge)

#ifndef TERA_MASTER_TASK_SPATULA_H_
#define TERA_MASTER_TASK_SPATULA_H_

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <glog/logging.h>
#include <map>
#include <queue>

#include "common/mutex.h"

namespace tera {
namespace master {

// TODO (jinxiao) : pluggable priority policy
static int32_t GetTaskPriority(std::string& type) {
    // great number comes great priority
    static std::map<std::string, int32_t> priority_dict;

    // unload
    priority_dict["unload-disable-table"] = 5;
    priority_dict["unload-merge"]         = 10;
    priority_dict["unload-balance-move"]  = 20;

    // load
    priority_dict["load"]                 = 10;

    // split
    priority_dict["split"]                = 10;

    /* works with C++ 11 but not here, what a sad story :(
    static std::map<std::string, int32_t> priority_dict = {
        // unload
        {"unload-disable-table",  5},
        {"unload-merge",         10},
        {"unload-balance-move",  20},

        // load
        {"load",                 10},

        // split
        {"split",                10}, 
    }; 
    */
    std::map<std::string, int32_t>::const_iterator it = priority_dict.find(type);
    assert(it != priority_dict.end()); // "unknown task type";
    return it->second;
}

// type of item in concurrency control queue
struct concurrency_task_t {
    std::string type; // "disable-unload" | "merge-unload" | "balance-unload" | ... | "load" | "split"
    boost::function<void ()> async_call;

    //concurrency_task_t() {}
    concurrency_task_t(std::string atype, boost::function<void ()>& aasync_call)
        : type(atype), async_call(aasync_call) {}

    friend bool operator< (concurrency_task_t t1, concurrency_task_t t2) {
        int32_t t1_priority = GetTaskPriority(t1.type);
        int32_t t2_priority = GetTaskPriority(t2.type);
        return t1_priority < t2_priority;
    }
};

class TaskSpatula {
public:
    TaskSpatula(int32_t concurrency_max);
    ~TaskSpatula();

    // the function adds a item(`task') to concurrency control queue
    void EnQueueTask(concurrency_task_t task);

    // the function deletes a item (`task') from concurrency control queue
    // and stores the deleted item at the location given by `task'.
    //
    // return value:
    // if concurrency control queue is empty before deletes, return false;
    // otherwise, returns true.
    bool DeQueueTask(concurrency_task_t *task);

    // the function do its best to executes task in concurrency control queue,
    // until:
    // 1) reachs the threshold (running count >= concurrency_max), 
    //    concurrency_max is the parameter of constructor.
    // 2) concurrency control queue has no item anymore
    void TryDrain();

    // the function minus (count of running task) one
    // users must call this function when a async task done!
    void FinishTask();

private:
    mutable Mutex m_mutex;
    std::priority_queue<concurrency_task_t> m_queue; // concurrency control queue
    int32_t m_pending_count; // count of task in concurrency control queue
    int32_t m_running_count; // count of task is running
    int32_t m_max_concurrency;
};

} // namespace master
} // namespace tera

#endif  // TERA_MASTER_TASK_SPATULA_H_
