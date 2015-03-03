// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Li Kang (com@baidu.com)

#include "tera/tabletnode/rpc_schedule_policy.h"

#include "thirdparty/glog/logging.h"

#include "tera/utils/timer.h"

namespace tera {
namespace tabletnode {

FairSchedulePolicy::FairSchedulePolicy()
    : m_min_elapse_time(0) {}

FairSchedulePolicy::~FairSchedulePolicy() {}

ScheduleEntity* FairSchedulePolicy::NewScheEntity(void* user_ptr) {
    return new FairScheduleEntity(user_ptr);
}

SchedulePolicy::ScheduleEntityList::iterator FairSchedulePolicy::Pick(
                                             ScheduleEntityList* entity_list) {
  ScheduleEntityList::iterator pick = entity_list->end();
    int64_t min_elapse_time = INT64_MAX;

    ScheduleEntityList::iterator it = entity_list->begin();
    for (; it != entity_list->end(); ++it) {
        FairScheduleEntity* entity = (FairScheduleEntity*)it->second;
        UpdateEntity(entity);
        if (!entity->pickable) {
            continue;
        }
        if (min_elapse_time > entity->elapse_time) {
            min_elapse_time = entity->elapse_time;
            pick = it;
        }
    }

    if (pick != entity_list->end()) {
        FairScheduleEntity* pick_entity = (FairScheduleEntity*)pick->second;
        pick_entity->running_count++;
        m_min_elapse_time = min_elapse_time;
    }
    return pick;
}

void FairSchedulePolicy::Done(ScheduleEntity* entity) {
    FairScheduleEntity* fair_entity = (FairScheduleEntity*)entity;
    UpdateEntity(fair_entity);
    CHECK_GE(fair_entity->running_count, 1);
    fair_entity->running_count--;
}

void FairSchedulePolicy::Enable(ScheduleEntity* entity) {
    FairScheduleEntity* fair_entity = (FairScheduleEntity*)entity;
    CHECK(!fair_entity->pickable);
    fair_entity->pickable = true;
    fair_entity->elapse_time += m_min_elapse_time;
}

void FairSchedulePolicy::Disable(ScheduleEntity* entity) {
    FairScheduleEntity* fair_entity = (FairScheduleEntity*)entity;
    CHECK(fair_entity->pickable);
    fair_entity->pickable = false;
    CHECK_GE(fair_entity->elapse_time, m_min_elapse_time);
    fair_entity->elapse_time -= m_min_elapse_time;
}

void FairSchedulePolicy::UpdateEntity(FairScheduleEntity* entity) {
    int64_t now = get_micros();
    entity->elapse_time += (now - entity->last_update_time) * entity->running_count;
    entity->last_update_time = now;
}

} // namespace tabletnode
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
