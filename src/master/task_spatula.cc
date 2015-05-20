// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Description:  concurrency control with priority for (load/unload/split/merge)

#include "task_spatula.h"

#include <unistd.h>

#include <glog/logging.h>

DECLARE_int32(tera_master_tabletnode_timeout);
DECLARE_string(tera_master_meta_table_name);

namespace tera {
namespace master {

TaskSpatula::TaskSpatula(int32_t max)
    :m_running_count(0), m_max_concurrency(max) {}

TaskSpatula::~TaskSpatula() {
    MutexLock lock(&m_mutex);
    for (std::set<TabletPtr>::iterator it = m_tablets.begin();
         it != m_tablets.end(); ++it) {
        TabletPtr tablet = *it;
        LOG(INFO) << "[spatula] " << tablet->GetPath();
        if (FLAGS_tera_master_tabletnode_timeout > 0
            && tablet->GetTableName() != FLAGS_tera_master_meta_table_name) {
            if (tablet->SetStatusIf(kTabletPending, kTableOnLoad)) { // tablets in onload queue
                LOG(INFO) << "onload to pending";
            }
            else if (tablet->SetStatusIf(kTabletPending, kTableOnSplit)) { // tablets in onsplit queue
                LOG(INFO) << "onsplit to pending";
            }
            else if (tablet->SetStatusIf(kTableOffLine, kTableUnLoading)) { // tablets in unloading queue
                LOG(INFO) << "unloading to offline";
                //ProcessOffLineTablet(tablet);// master will do this
            } else {
                CHECK(0) << tablet;
            }
        } else if (tablet->SetStatusIf(kTableOffLine, kTableOnLoad)
                   || tablet->SetStatusIf(kTableOffLine, kTableOnSplit)
                   || tablet->SetStatusIf(kTableOffLine, kTableUnLoading)) {
            //ProcessOffLineTablet(tablet);
            LOG(INFO) << "switch to offline";
        } else {
            CHECK(0) << tablet;
        }
    }
}

void TaskSpatula::EnQueueTask(const ConcurrencyTask& atask) {
    MutexLock lock(&m_mutex);
    m_queue.push(atask);
    AddTablet(atask.tablet);
}

void TaskSpatula::FinishTask() {
    MutexLock lock(&m_mutex);
    assert(m_running_count > 0);
    m_running_count--;
}

int32_t TaskSpatula::TryDraining() {
    MutexLock lock(&m_mutex);
    TabletPtr dummy_tablet;
    boost::function<void ()> dummy_func = boost::bind(&TaskSpatula::TryDraining, this);
    ConcurrencyTask atask(0, dummy_tablet, dummy_func);
    int done = 0;
    while(m_running_count < m_max_concurrency
          && DeQueueTask(&atask)) {
        atask.async_call();
        done++;
        m_running_count++;
    }
    return done;
}

bool TaskSpatula::DeQueueTask(ConcurrencyTask* atask) {
    assert(atask != NULL);
    if(m_queue.size() <= 0) {
        return false;
    }
    *atask = m_queue.top();
    m_queue.pop();
    DeleteTablet(atask->tablet);
    return true;
}

int32_t TaskSpatula::GetRunningCount() {
    return m_running_count;
}

// no lock
void TaskSpatula::AddTablet(TabletPtr tablet) {
    if (m_tablets.find(tablet) != m_tablets.end()) {
        LOG(ERROR) << "[spatula] already exsit " << tablet->GetPath();
    } else {
        m_tablets.insert(tablet);
    }
}

// no lock
void TaskSpatula::DeleteTablet(TabletPtr tablet) {
    if (m_tablets.find(tablet) == m_tablets.end()) {
        LOG(ERROR) << "[spatula] not exsit " << tablet->GetPath();
    } else {
        m_tablets.erase(tablet);
    }
}

} // namespace master
} // namespace tera
